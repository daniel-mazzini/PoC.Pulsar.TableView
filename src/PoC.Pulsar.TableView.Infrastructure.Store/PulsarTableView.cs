using DotPulsar;
using Microsoft.Extensions.Logging;
using PoC.Pulsar.TableView.Domain.Checkpoints;
using PoC.Pulsar.TableView.Domain.Filter;
using PoC.Pulsar.TableView.Domain.Metadatas;
using PoC.Pulsar.TableView.Domain.Projector;
using PoC.Pulsar.TableView.Domain.Serializers;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Domain.TableView;
using PoC.Pulsar.TableView.Infrastructure.Store.Observability;
using PoC.Pulsar.TableView.Infrastructure.Store.Readers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace PoC.Pulsar.TableView.Infrastructure.Store;

public sealed class PulsarTableView<TMessage> : IPulsarTableView<TMessage>
    where TMessage : class
{
    private static readonly TimeSpan HighWatermarkLookupTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan LiveTailShardDiscoveryInterval = TimeSpan.FromSeconds(30);

    private readonly IAvroSerializer _avroSerializer;
    private readonly ILogger<PulsarTableView<TMessage>> _logger;
    private readonly ITableViewMessageApplier<TMessage> _messageApplier;
    private readonly ITopicShardReaderStrategy _readerStrategy;
    private readonly ConcurrentDictionary<string, TMessage> _snapshot = new(StringComparer.Ordinal);
    private readonly StoreMetadata _storeMetadata;
    private readonly Subject<TableEntryChange<TMessage>> _subject = new();
    private readonly string _topic;
    private readonly IUnitOfWorkFactory _unitOfWorkFactory;
    private readonly ConcurrentDictionary<TopicShard, MessageId> _liveTailStartMessageId = [];
    private readonly ConcurrentDictionary<TopicShard, Task> _liveTailTasks = [];

    public PulsarTableView(
        string topic,
        ITopicShardReaderStrategy readerStrategy,
        IUnitOfWorkFactory unitOfWorkFactory,
        IAvroSerializer avroSerializer,
        ITableViewMessageApplier<TMessage> messageApplier,
        StoreMetadata storeMetadata,
        ILogger<PulsarTableView<TMessage>> logger)
    {
        ArgumentNullException.ThrowIfNull(topic);
        ArgumentNullException.ThrowIfNull(readerStrategy);
        ArgumentNullException.ThrowIfNull(unitOfWorkFactory);
        ArgumentNullException.ThrowIfNull(avroSerializer);
        ArgumentNullException.ThrowIfNull(messageApplier);
        ArgumentNullException.ThrowIfNull(storeMetadata);
        ArgumentNullException.ThrowIfNull(logger);

        (_topic, _readerStrategy, _unitOfWorkFactory, _avroSerializer, _messageApplier, _storeMetadata, _logger) =
            (topic, readerStrategy, unitOfWorkFactory, avroSerializer, messageApplier, storeMetadata, logger);
    }

    public IObservable<TableEntryChange<TMessage>> OnChanges => _subject.AsObservable();

    public ValueTask<TMessage?> GetEntry(string key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(_snapshot.TryGetValue(key, out var value) ? value : null);
    }

    public IDictionary<string, TMessage> GetSnapshot(IValuePredicate<TMessage>? filter = null)
    {
        if (filter is null)
        {
            return new Dictionary<string, TMessage>(_snapshot, StringComparer.Ordinal);
        }

        return _snapshot.Where(entry => filter.Match(entry.Value))
                        .ToDictionary(entry => entry.Key, entry => entry.Value, StringComparer.Ordinal);
    }

    public async Task<TopicBootstrapResult<TMessage>> StartBootstrapAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Resolving high-watermarks for topic {Topic}.", _topic);
        TopicHighWatermark topicHighWatermark = await GetHighWaterMarkForTopic(cancellationToken);

        if (!topicHighWatermark.HasMessages)
        {
            _liveTailStartMessageId.Clear();
            _logger.LogInformation("No messages found in topic {Topic}. Bootstrap completed instantly.", _topic);
            return new TopicHighWatermarkNotFound<TMessage>();
        }

        StoreRecoveryMode recoveryMode = await ResolveStoreRecoveryModeAsync(topicHighWatermark, cancellationToken);

        if (recoveryMode is RecoverFromStateStore)
        {
            await LoadStateStoreIntoMemoryAsync(cancellationToken);
        }
        else
        {
            await ClearMemoryAsync(cancellationToken);
            await ClearStateStoreAsync(cancellationToken);
        }

        var readerTasks = topicHighWatermark.ShardWatermarks
                                            .OrderBy(watermark => watermark.Shard.PartitionId)
                                            .Select(watermark => StartBootstrapByShardAsync(watermark.Shard,
                                                                                           watermark.LastMessageId,
                                                                                           recoveryMode,
                                                                                           cancellationToken))
                                            .ToList();

        _logger.LogInformation("Starting bootstrap for topic {Topic} across {ShardCount} shard(s).", _topic, topicHighWatermark.ShardWatermarks.Count);

        await Task.WhenAll(readerTasks);
        return GetResult(recoveryMode, readerTasks);
    }

    public async Task StartLiveTailAsync(CancellationToken cancellationToken)
    {
        await EnsureLiveTailReadersAsync(cancellationToken);

        while (!cancellationToken.IsCancellationRequested)
        {
            var delayTask = Task.Delay(LiveTailShardDiscoveryInterval, cancellationToken);
            var runningTasks = _liveTailTasks.Values.ToArray();
            var completedTask = await Task.WhenAny(runningTasks.Append(delayTask));

            if (completedTask != delayTask)
            {
                await completedTask;
            }

            await EnsureLiveTailReadersAsync(cancellationToken);
        }
    }

    private static TopicBootstrapResult<TMessage> GetResult(StoreRecoveryMode recoveryMode, List<Task<ShardBootstrapResult<TMessage>>> readerTasks)
    {
        var shardResults = readerTasks.Select(task => task.Result).ToArray();

        if (recoveryMode is RebuildFromEarliest rebuilt)
        {
            return new TopicRebuiltFromEarliest<TMessage>(rebuilt.Reason);
        }

        var deltaChanges = shardResults
            .OfType<ShardRecoveredFromStateStore<TMessage>>()
            .SelectMany(result => result.DeltaChanges)
            .ToArray();

        return new TopicRecoveredFromStateStore<TMessage>(deltaChanges);
    }

    private static MessageId ToDotPulsarMessageId(PulsarMessageId messageId, string physicalTopic)
        => new(checked((ulong)messageId.LedgerId), checked((ulong)messageId.EntryId), messageId.PartitionIndex, messageId.BatchIndex, physicalTopic);

    private async Task ClearMemoryAsync(CancellationToken cancellationToken)
    {
        _snapshot.Clear();
        _liveTailStartMessageId.Clear();
        await Task.CompletedTask;
    }

    private async Task ClearStateStoreAsync(CancellationToken cancellationToken)
    {
        using var uow = _unitOfWorkFactory.CreateBootstrap<TMessage>();
        await uow.MessageStorage.ClearAsync(cancellationToken);
    }

    private async Task<TopicHighWatermark> GetHighWaterMarkForTopic(CancellationToken cancellationToken)
    {
        using var highWatermarkCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        highWatermarkCts.CancelAfter(HighWatermarkLookupTimeout);

        try
        {
            TopicHighWatermark topicHighWatermark = await _readerStrategy.CaptureHighWatermarkAsync(_topic, highWatermarkCts.Token);
            _logger.LogInformation("Resolved {ShardCount} high-watermark(s) for topic {Topic}.", topicHighWatermark.ShardWatermarks.Count, _topic);
            return topicHighWatermark;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && highWatermarkCts.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Timed out resolving high-watermarks for topic {_topic} after {HighWatermarkLookupTimeout}. Check Pulsar broker connectivity and advertised listener configuration.");
        }
    }

    private async Task LoadStateStoreIntoMemoryAsync(CancellationToken cancellationToken)
    {
        using var uow = _unitOfWorkFactory.CreateBootstrap<TMessage>();
        var snapshot = uow.MessageStorage.GetAll();

        _snapshot.Clear();

        foreach (var (key, value) in snapshot)
        {
            _snapshot[key] = value;
        }

        await Task.CompletedTask;
    }

    private async Task<TableEntryChange<TMessage>?> ProcessMessageAsync(
        TableViewMessage tableViewMessage,
        ITableViewUnitOfWork<TMessage> unitOfWork,
        bool collectDelta,
        bool emitEvents,
        CancellationToken cancellationToken)
    {
        ProcessPhase phase = emitEvents ? ProcessPhase.Live : ProcessPhase.Bootstrap;
        var tags = new TagList
        {
            ProjectorStoreTelemetry.StoreTag,
            new("topic", tableViewMessage.TopicName),
            new("partition_id", tableViewMessage.PartitionId),
            new("phase", phase)
        };

        var stopwatch = Stopwatch.StartNew();
        using var activity = ProjectorStoreTelemetry.StartActivity("reading and handling a Pulsar input message",
                                                                   tableViewMessage.TopicName,
                                                                   tableViewMessage.PartitionId,
                                                                   phase: phase.Name);

        TableMessageApplyResult<TMessage> applyResult = await _messageApplier.ApplyAsync(tableViewMessage,
                                                                                         phase,
                                                                                         unitOfWork,
                                                                                         dataArray => _avroSerializer.Deserialize<TMessage>(dataArray),
                                                                                         cancellationToken);
        RecordMessageProcessed(tableViewMessage, phase, tags, stopwatch);
        return CreateResponse(tableViewMessage, collectDelta, emitEvents, phase, activity, applyResult);
    }

    private TableEntryChange<TMessage>? CreateResponse(TableViewMessage tableViewMessage, bool collectDelta, bool emitEvents, ProcessPhase phase, Activity? activity, TableMessageApplyResult<TMessage> applyResult)
    {
        if (applyResult is TableMessageRejected<TMessage>)
        {
            activity?.SetTag("result", "rejected");
            return null;
        }

        if (applyResult is TableMessageNoOp<TMessage> noOp)
        {
            activity?.SetTag("result", "noop");
            _logger.LogInformation("message skipped during {Phase} phase for topic {Topic} partition {PartitionId} PulsarMessageId {PulsarMessageId}. Reason {Reason}",
                                   phase.Name,
                                   tableViewMessage.TopicName,
                                   tableViewMessage.PartitionId,
                                   tableViewMessage.BrokerMessageId.ToString(),
                                   noOp.Reason);
            return null;
        }

        activity?.SetTag("result", "success");

        if (applyResult is not TableMessageApplied<TMessage> applied)
        {
            return null;
        }

        switch (applied.Change)
        {
            case TableEntryCreated<TMessage> created:
                _snapshot.AddOrUpdate(created.Key, created.NewValue, (_, _) => created.NewValue);

                if (emitEvents)
                {
                    _subject.OnNext(created);
                }

                return collectDelta ? created : null;

            case TableEntryUpdated<TMessage> updated:
                _snapshot.AddOrUpdate(updated.Key, updated.NewValue, (_, _) => updated.NewValue);

                if (emitEvents)
                {
                    _subject.OnNext(updated);
                }

                return collectDelta ? updated : null;

            case EventDeleted<TMessage> deleted:
                _snapshot.TryRemove(deleted.Key, out _);

                if (emitEvents)
                {
                    _subject.OnNext(deleted);
                }

                return collectDelta ? deleted : null;

            default:
                return null;
        }
    }

    private void RecordMessageProcessed(TableViewMessage tableViewMessage, ProcessPhase phase, TagList tags, Stopwatch stopwatch)
    {
        ProjectorStoreTelemetry.TopicReaderMessagesProcessed.Add(1, tags);

        bool isBootstrap = phase == ProcessPhase.Bootstrap;

        var messageCounter = isBootstrap
            ? ProjectorStoreTelemetry.TopicReaderBootstrapMessagesProcessed
            : ProjectorStoreTelemetry.TopicReaderLiveMessagesProcessed;

        messageCounter.Add(1, tags);
        _logger.LogInformation("message processed during {Phase} phase for topic {Topic} partition {PartitionId} PulsarMessageId {PulsarMessageId}",
                               phase.Name,
                               tableViewMessage.TopicName,
                               tableViewMessage.PartitionId,
                               tableViewMessage.BrokerMessageId.ToString());

        ProjectorStoreTelemetry.TopicMessageProcessingDuration.Record(stopwatch.Elapsed.TotalMilliseconds, tags);
    }

    private async Task<MessageId> ResolveCheckpointStartMessageIdAsync(TopicShard shard, CancellationToken cancellationToken)
    {
        using var uow = _unitOfWorkFactory.CreateBootstrap<TMessage>();
        TopicCheckpoint? lastCheckpoint = await uow.CheckpointStorage.GetLastCheckpoint(shard, cancellationToken);

        return lastCheckpoint is not null
            ? ToDotPulsarMessageId(lastCheckpoint.LastProcessedMessageId, lastCheckpoint.PhysicalTopic)
            : throw new InvalidOperationException($"Checkpoint not found for topic {shard.LogicalTopic} shard {shard.PhysicalTopic} while recovery mode was already resolved as trustworthy.");
    }

    private async Task<MessageId> ResolveLiveTailStartMessageIdAsync(TopicShard shard, CancellationToken cancellationToken)
    {
        if (_liveTailStartMessageId.TryGetValue(shard, out var bootstrapStart))
        {
            return bootstrapStart;
        }

        using var uow = _unitOfWorkFactory.CreateBootstrap<TMessage>();
        TopicCheckpoint? checkpoint = await uow.CheckpointStorage.GetLastCheckpoint(shard, cancellationToken);

        return checkpoint is not null && checkpoint.StoreId == _storeMetadata.StoreGenerationId
            ? ToDotPulsarMessageId(checkpoint.LastProcessedMessageId, checkpoint.PhysicalTopic)
            : MessageId.Earliest;
    }

    private async Task<StoreRecoveryMode> ResolveStoreRecoveryModeAsync(TopicHighWatermark topicHighWatermark, CancellationToken cancellationToken)
    {
        if (!_storeMetadata.IsBoostrapCompleted || _storeMetadata.StoreGenerationId == Guid.Empty)
        {
            return new RebuildFromEarliest("store_metadata_untrusted");
        }

        using var uow = _unitOfWorkFactory.CreateBootstrap<TMessage>();

        foreach (var shard in topicHighWatermark.Shards.OrderBy(shard => shard.PartitionId))
        {
            TopicCheckpoint? checkpoint = await uow.CheckpointStorage.GetLastCheckpoint(shard, cancellationToken);

            if (checkpoint is null)
            {
                return new RebuildFromEarliest($"checkpoint_missing_shard_{shard.PhysicalTopic}");
            }

            if (checkpoint.StoreId != _storeMetadata.StoreGenerationId)
            {
                return new RebuildFromEarliest($"checkpoint_store_generation_mismatch_shard_{shard.PhysicalTopic}");
            }
        }

        return new RecoverFromStateStore();
    }

    private void SetLiveTailStart(TopicShard shard, MessageId highWatermarkMessageId)
    {
        _liveTailStartMessageId.AddOrUpdate(shard, _ => highWatermarkMessageId, (_, _) => highWatermarkMessageId);
    }

    private async Task EnsureLiveTailReadersAsync(CancellationToken cancellationToken)
    {
        var discoveredShards = await _readerStrategy.DiscoverShardsAsync(_topic, cancellationToken);

        foreach (var shard in discoveredShards)
        {
            if (_liveTailTasks.ContainsKey(shard))
            {
                continue;
            }

            MessageId startMessageId = await ResolveLiveTailStartMessageIdAsync(shard, cancellationToken);
            var task = StartLiveTailByShardAsync(shard, startMessageId, cancellationToken);
            _liveTailTasks.TryAdd(shard, task);
        }
    }

    private async Task StartLiveTailByShardAsync(TopicShard shard, MessageId startMessageId, CancellationToken cancellationToken)
    {
        ProjectorStoreTelemetry.IncrementActiveTopicReaders();

        try
        {
            await using var reader = await _readerStrategy.CreateReaderAsync(shard, startMessageId, cancellationToken);

            while (!cancellationToken.IsCancellationRequested)
            {
                using var unitOfWork = _unitOfWorkFactory.CreateBootstrap<TMessage>();
                TableViewMessage message = await reader.ReceiveAsync(cancellationToken);
                await ProcessMessageAsync(message, unitOfWork, collectDelta: false, emitEvents: true, cancellationToken);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogInformation("topic reader cancelled for topic {Topic} shard {Shard}", shard.LogicalTopic, shard.PhysicalTopic);
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "topic reader failed for topic {Topic} shard {Shard}", shard.LogicalTopic, shard.PhysicalTopic);
            throw;
        }
        finally
        {
            ProjectorStoreTelemetry.DecrementActiveTopicReaders();
        }
    }

    private async Task<ShardBootstrapResult<TMessage>> StartBootstrapByShardAsync(
        TopicShard shard,
        MessageId highWatermarkMessageId,
        StoreRecoveryMode recoveryMode,
        CancellationToken cancellationToken)
    {
        ProjectorStoreTelemetry.IncrementActiveTopicReaders();
        using var readerActivity = ProjectorStoreTelemetry.StartActivity("topic reader started",
                                                                         shard.LogicalTopic,
                                                                         shard.PartitionId,
                                                                         phase: "boostrap");

        bool collectDelta = recoveryMode is RecoverFromStateStore;
        MessageId startMessageId = recoveryMode switch
        {
            RecoverFromStateStore => await ResolveCheckpointStartMessageIdAsync(shard, cancellationToken),
            RebuildFromEarliest => MessageId.Earliest,
            _ => throw new UnreachableException()
        };

        try
        {
            bool needsBootstrap = recoveryMode is RebuildFromEarliest || highWatermarkMessageId.CompareTo(startMessageId) > 0;

            if (!needsBootstrap)
            {
                SetLiveTailStart(shard, highWatermarkMessageId);
                return recoveryMode switch
                {
                    RecoverFromStateStore => new ShardRecoveredFromStateStore<TMessage>(shard, []),
                    RebuildFromEarliest => new ShardRebuiltFromEarliest<TMessage>(shard),
                    _ => throw new UnreachableException()
                };
            }

            int limitCheck = 100_000;
            int counter = 0;
            var changes = new List<TableEntryChange<TMessage>>();

            await using var reader = await _readerStrategy.CreateReaderAsync(shard, startMessageId, cancellationToken);
            using var unitOfWork = _unitOfWorkFactory.CreateBootstrap<TMessage>();

            while (!cancellationToken.IsCancellationRequested)
            {
                TableViewMessage message = await reader.ReceiveAsync(cancellationToken);
                TableEntryChange<TMessage>? change = await ProcessMessageAsync(message, unitOfWork, collectDelta, false, cancellationToken);

                if (collectDelta && change is not null)
                {
                    changes.Add(change);
                }

                counter++;

                var currentId = ToDotPulsarMessageId(message.BrokerMessageId, shard.PhysicalTopic);
                if (currentId.CompareTo(highWatermarkMessageId) >= 0)
                {
                    _logger.LogInformation("Bootstrap finished for {Topic} shard {Shard} at message {Id}", shard.LogicalTopic, shard.PhysicalTopic, message.BrokerMessageId);
                    break;
                }

                if (counter % limitCheck == 0)
                {
                    await _unitOfWorkFactory.MoveDurableAsync(cancellationToken);
                }
            }

            SetLiveTailStart(shard, highWatermarkMessageId);

            return recoveryMode switch
            {
                RecoverFromStateStore => new ShardRecoveredFromStateStore<TMessage>(shard, changes),
                RebuildFromEarliest => new ShardRebuiltFromEarliest<TMessage>(shard),
                _ => throw new UnreachableException()
            };
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            using var cancelledActivity = ProjectorStoreTelemetry.StartActivity("topic reader cancelled",
                                                                                 shard.LogicalTopic,
                                                                                 shard.PartitionId,
                                                                                 phase: "boostrap");
            cancelledActivity?.SetTag("result", "cancelled");
            ProjectorStoreTelemetry.TopicReaderCancelled.Add(1,
                                                              ProjectorStoreTelemetry.StoreTag,
                                                              new KeyValuePair<string, object?>("topic", shard.LogicalTopic),
                                                              new KeyValuePair<string, object?>("phase", "boostrap"));
            _logger.LogInformation("topic reader cancelled for topic {Topic} shard {Shard}", shard.LogicalTopic, shard.PhysicalTopic);
            throw;
        }
        catch (Exception exception)
        {
            using var failedActivity = ProjectorStoreTelemetry.StartActivity("topic reader failed",
                                                                             shard.LogicalTopic,
                                                                             shard.PartitionId,
                                                                             phase: "boostrap");
            failedActivity?.SetTag("result", "error");
            failedActivity?.SetStatus(ActivityStatusCode.Error, exception.GetType().Name);
            ProjectorStoreTelemetry.TopicReaderErrors.Add(1,
                                                           ProjectorStoreTelemetry.StoreTag,
                                                           new KeyValuePair<string, object?>("topic", shard.LogicalTopic),
                                                           new KeyValuePair<string, object?>("phase", "boostrap"));
            _logger.LogError(exception, "topic reader failed for topic {Topic} shard {Shard}", shard.LogicalTopic, shard.PhysicalTopic);
            throw;
        }
        finally
        {
            ProjectorStoreTelemetry.DecrementActiveTopicReaders();
        }
    }
}
