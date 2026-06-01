using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Microsoft.Extensions.Logging;
using PoC.Pulsar.TableView.Domain.Entities;
using PoC.Pulsar.TableView.Domain.Filter;
using PoC.Pulsar.TableView.Domain.Storages;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Infrastructure.Store.Observability;
using PoC.Pulsar.TableView.Infrastructure.Store.Publisher;
using PoC.Pulsar.TableView.Infrastructure.Store.Readers;
using PoC.Pulsar.TableView.Infrastructure.Store.Serialization;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace PoC.Pulsar.TableView.Infrastructure.Store;

public sealed class PulsarTableView<TMessage> : IPulsarTableView<TMessage>
    where TMessage : class
{
    private static readonly TimeSpan HighWatermarkLookupTimeout = TimeSpan.FromSeconds(30);

    private readonly IAvroSerializer _avroSerializer;
    private readonly IProjectorMessageApplier<TMessage> _messageApplier;
    private readonly StoreMetadata _storeMetadata;
    private readonly ConcurrentDictionary<string, TMessage> _localStorage = [];
    private readonly ILogger<PulsarTableView<TMessage>> _logger;
    private readonly IUnitOfWorkFactory _unitOfWorkFactory;
    private readonly IProjectorTopicReaderFactory _projectorTopicReaderFactory;
    private readonly IRejectedMessagePublisher _rejectedMessagePublisher;
    private readonly Subject<Event<TMessage>> _subject = new();
    private readonly string _topic;
    private ConcurrentDictionary<int, MessageId?> _liveTailStartMessageId = [];

    public PulsarTableView(string topic,
                           IProjectorTopicReaderFactory projectorTopicReaderFactory,
                           IRejectedMessagePublisher rejectedMessagePublisher,
                           IUnitOfWorkFactory unitOfWorkFactory,
                           IAvroSerializer avroSerializer,
                           IProjectorMessageApplier<TMessage> messageApplier,
                           StoreMetadata storeMetadata,
                           ILogger<PulsarTableView<TMessage>> logger)
        => (_topic, _projectorTopicReaderFactory, _rejectedMessagePublisher, _unitOfWorkFactory, _avroSerializer, _messageApplier, _storeMetadata, _logger) =
            (topic, projectorTopicReaderFactory, rejectedMessagePublisher, unitOfWorkFactory, avroSerializer, messageApplier, storeMetadata, logger);

    public IObservable<Event<TMessage>> OnUpdate => _subject.AsObservable();
    public async ValueTask<TMessage?> GetAsync(string key, CancellationToken cancellationToken)
    {
        using var uow = _unitOfWorkFactory.CreateBootstrap<TMessage>();
        return await uow.MessageStorage.TryLoadAsync(key, cancellationToken);
    }

    public IDictionary<string, TMessage> GetLoadedOnBoostrap()
    {
        return _localStorage;
    }

    public IDictionary<string, TMessage> GetLoadedOnBoostrapFilterBy(IValuePredicate<TMessage> filter)
    {
        Dictionary<string, TMessage> result = new(capacity: _localStorage.Count);
        foreach (var kvp in _localStorage)
        {
            if (filter.Match(kvp.Value))
            {
                result.Add(kvp.Key, kvp.Value);
            }
        }

        return result;
    }

    private async Task StartBoostrapByPartition(string topicName,
                                                int partitionId,
                                                MessageId highMarketPartitionedMessageId,
                                                CancellationToken cancellationToken)
    {
        ProjectorStoreTelemetry.IncrementActiveTopicReaders();
        using var readerActivity = ProjectorStoreTelemetry.StartActivity("topic reader started",
                                                                         topicName,
                                                                         partitionId,
                                                                         phase: "boostrap");

        MessageId startMessageId = await GetStoreCheckPoint(topicName, partitionId, _storeMetadata, cancellationToken);

        try
        {
            bool needsBootstrap = highMarketPartitionedMessageId.CompareTo(startMessageId) > 0;

            if (!needsBootstrap)
            {
                return;
            }

            int limitCheck = 100_000;
            int counter = 0;
            await using var reader = await _projectorTopicReaderFactory.CreateReaderAsync(topicName, partitionId, startMessageId, cancellationToken);
            using (var unitOfWork = _unitOfWorkFactory.CreateBootstrap<TMessage>())
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    TableViewMessage message = await reader.ReceiveAsync(cancellationToken);
                    await ProcessMessageAsync(message, unitOfWork, false, cancellationToken);

                    counter++;

                    var currentId = ToDotPulsarMessageId(message.MessageId, topicName);
                    if (currentId.CompareTo(highMarketPartitionedMessageId) >= 0)
                    {
                        _logger.LogInformation("Bootstrap finalizado para {Topic} en el mensaje {Id}", topicName, message.MessageId);
                        await unitOfWork.CheckpointStorage.SaveCheckpointAsync(topicName, partitionId, message.MessageId, cancellationToken);
                        break;
                    }
                    if (counter % limitCheck == 0)
                    {
                        await _unitOfWorkFactory.MoveDurableAsync(cancellationToken);
                    }
                }
            }
            _liveTailStartMessageId.AddOrUpdate(partitionId, (_) => highMarketPartitionedMessageId, (_, _) => highMarketPartitionedMessageId);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            using var cancelledActivity = ProjectorStoreTelemetry.StartActivity("topic reader cancelled",
                                                                                topicName,
                                                                                partitionId,
                                                                                phase: "boostrap");
            cancelledActivity?.SetTag("result", "cancelled");
            ProjectorStoreTelemetry.TopicReaderCancelled.Add(1,
                                                             ProjectorStoreTelemetry.StoreTag,
                                                             new KeyValuePair<string, object?>("topic", topicName),
                                                             new KeyValuePair<string, object?>("phase", "boostrap"));
            _logger.LogInformation("topic reader cancelled for topic {Topic} partition {PartitionId}", topicName, partitionId);
            throw;
        }
        catch (Exception exception)
        {
            using var failedActivity = ProjectorStoreTelemetry.StartActivity("topic reader failed",
                                                                             topicName,
                                                                             partitionId,
                                                                             phase: "boostrap");
            failedActivity?.SetTag("result", "error");
            failedActivity?.SetStatus(ActivityStatusCode.Error, exception.GetType().Name);
            ProjectorStoreTelemetry.TopicReaderErrors.Add(1,
                                                          ProjectorStoreTelemetry.StoreTag,
                                                          new KeyValuePair<string, object?>("topic", topicName),
                                                          new KeyValuePair<string, object?>("phase", "boostrap"));
            _logger.LogError(exception, "topic reader failed for topic {Topic} partition {PartitionId}", topicName, partitionId);
            throw;
        }
        finally
        {
            ProjectorStoreTelemetry.DecrementActiveTopicReaders();
        }

    }

    private async Task ProcessMessageAsync(TableViewMessage tableViewMessage, ITableViewUnitOfWork<TMessage> unitOfWork, bool emitEvents, CancellationToken cancellationToken)
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
        

        await _messageApplier.ApplyAsync(tableViewMessage,
                                         phase,
                                         unitOfWork,
                                         (dataArray) => _avroSerializer.Deserialize<TMessage>(dataArray),
                                         cancellationToken);

        await unitOfWork.CheckpointStorage.SaveCheckpointAsync(tableViewMessage.TopicName,
                                                               tableViewMessage.PartitionId,
                                                               tableViewMessage.MessageId,
                                                               cancellationToken);

        ProjectorStoreTelemetry.TopicReaderMessagesProcessed.Add(1, tags);

        if (phase == ProcessPhase.Bootstrap)
        {
            ProjectorStoreTelemetry.TopicReaderBootstrapMessagesProcessed.Add(1, tags);
            _logger.LogInformation("message processed during bootstrap phase for topic {Topic} partition {PartitionId} PulsarMessageId {PulsarMessageId}",
                                   tableViewMessage.TopicName,
                                   tableViewMessage.PartitionId,
                                   tableViewMessage.MessageId.ToString());
        }
        else
        {
            ProjectorStoreTelemetry.TopicReaderLiveMessagesProcessed.Add(1, tags);
            _logger.LogInformation("message processed during live phase for topic {Topic} partition {PartitionId} PulsarMessageId {PulsarMessageId}",
                                   tableViewMessage.TopicName,
                                   tableViewMessage.PartitionId,
                                   tableViewMessage.MessageId.ToString());
        }
        ProjectorStoreTelemetry.TopicMessageProcessingDuration.Record(stopwatch.Elapsed.TotalMilliseconds, tags);
        activity?.SetTag("result", "success");
    }

    private async Task<MessageId> GetStoreCheckPoint(string topicName, int partitionId, StoreMetadata storeMetadata, CancellationToken cancellationToken)
    {
        MessageId startMessageId = MessageId.Earliest;
        using (var uow = _unitOfWorkFactory.CreateBootstrap<TMessage>())
        {
            TopicCheckpoint? lastCheckpoint = await uow.CheckpointStorage.GetLastCheckpoint(topicName, partitionId, cancellationToken);
            if (lastCheckpoint == null)
            {
                return startMessageId;
            }

            if (!storeMetadata.IsBoostrapCompleted)
            {
                ProjectorStoreTelemetry.CheckpointsIgnored.Add(1,
                                                               ProjectorStoreTelemetry.StoreTag,
                                                               new KeyValuePair<string, object?>("topic", topicName),
                                                               new KeyValuePair<string, object?>("partition_id", partitionId),
                                                               new KeyValuePair<string, object?>("result", "bootstrap_incomplete"));
                _logger.LogInformation("checkpoint ignored for topic {Topic} partition {PartitionId}", topicName, partitionId);
                return startMessageId;
            }
            else if (lastCheckpoint.StoreId != storeMetadata.StoreGenerationId)
            {
                ProjectorStoreTelemetry.CheckpointsIgnored.Add(1,
                                                               ProjectorStoreTelemetry.StoreTag,
                                                               new KeyValuePair<string, object?>("topic", topicName),
                                                               new KeyValuePair<string, object?>("partition_id", partitionId),
                                                               new KeyValuePair<string, object?>("result", "store_generation_mismatch"));
                _logger.LogWarning(
                    "checkpoint StoreGenerationId mismatch for topic {Topic} partition {PartitionId}",
                    topicName,
                    partitionId);
            }

            startMessageId = ToDotPulsarMessageId(lastCheckpoint.LastProcessedMessageId, lastCheckpoint.TopicName);
        }

        return startMessageId;
    }
    //private async Task<TopicCheckpoint?> TryLoadTrustedCheckpointAsync(
    //    string topicName,
    //    int partitionId,
    //    MessageId? highWatermark,
    //    StoreMetadata metadata,
    //    CancellationToken cancellationToken)
    //{
    //    if (highWatermark is null)
    //    {
    //        return null;
    //    }

    //    var checkpoint = await _store.TryLoadCheckpointAsync(topicName, partitionId, cancellationToken);
    //    var canTrustCheckpoint = metadata.BootstrapCompleted
    //        && checkpoint is not null
    //        && checkpoint.StoreGenerationId == metadata.StoreGenerationId
    //        && await _store.HasDurableProjectionStateAsync(topicName, partitionId, cancellationToken);

    //    if (checkpoint is not null && !metadata.BootstrapCompleted)
    //    {
    //        ProjectorStoreTelemetry.CheckpointsIgnored.Add(
    //            1,
    //            ProjectorStoreTelemetry.StoreTag,
    //            new KeyValuePair<string, object?>("topic", topicName),
    //            new KeyValuePair<string, object?>("partition_id", partitionId),
    //            new KeyValuePair<string, object?>("result", "bootstrap_incomplete"));
    //        _logger.LogInformation("checkpoint ignored for topic {Topic} partition {PartitionId}", topicName, partitionId);
    //    }
    //    else if (checkpoint is not null && checkpoint.StoreGenerationId != metadata.StoreGenerationId)
    //    {
    //        ProjectorStoreTelemetry.CheckpointsIgnored.Add(
    //            1,
    //            ProjectorStoreTelemetry.StoreTag,
    //            new KeyValuePair<string, object?>("topic", topicName),
    //            new KeyValuePair<string, object?>("partition_id", partitionId),
    //            new KeyValuePair<string, object?>("result", "store_generation_mismatch"));
    //        _logger.LogWarning(
    //            "checkpoint StoreGenerationId mismatch for topic {Topic} partition {PartitionId}",
    //            topicName,
    //            partitionId);
    //    }
    //    else if (checkpoint is not null && !canTrustCheckpoint)
    //    {
    //        ProjectorStoreTelemetry.CheckpointsIgnored.Add(
    //            1,
    //            ProjectorStoreTelemetry.StoreTag,
    //            new KeyValuePair<string, object?>("topic", topicName),
    //            new KeyValuePair<string, object?>("partition_id", partitionId),
    //            new KeyValuePair<string, object?>("result", "missing_local_projection"));
    //        _logger.LogInformation("checkpoint ignored for topic {Topic} partition {PartitionId}", topicName, partitionId);
    //    }

    //    return canTrustCheckpoint ? checkpoint : null;
    //}
    public async Task StartBootstrapAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Resolving high-watermarks for topic {Topic}.", _topic);


        TopicHighWatermark topicHighWatermark = await GetHighWaterMarkForTopic(cancellationToken);

        if (!topicHighWatermark.HasMessages)
        {
            _liveTailStartMessageId = [];
            _logger.LogInformation("No messages found in topic {Topic}. Bootstrap completed instantly.", _topic);
            return;
        }

        List<Task> readerTasks = new(topicHighWatermark.PartitionIds.Count);
        foreach (var partitionId in topicHighWatermark.PartitionIds.DefaultIfEmpty(0).OrderBy(partitionId => partitionId))
        {
            var startPatition = StartBoostrapByPartition(_topic,
                                                         partitionId,
                                                         topicHighWatermark.GetPartitionHighWatermarkOrThrow(partitionId),
                                                         cancellationToken);
            readerTasks.Add(startPatition);
        }
        _logger.LogInformation("Starting bootstrap for topic {Topic} across {PartitionCount} partitions.", _topic, topicHighWatermark.PartitionIds);
        await Task.WhenAll(readerTasks);

        
    }

    private async Task<TopicHighWatermark> GetHighWaterMarkForTopic(CancellationToken cancellationToken)
    {
        using var highWatermarkCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        highWatermarkCts.CancelAfter(HighWatermarkLookupTimeout);
        TopicHighWatermark topicHighWatermark;
        try
        {
            topicHighWatermark = await _projectorTopicReaderFactory.CaptureHighWatermarkAsync(_topic, highWatermarkCts.Token);
            _logger.LogInformation("Resolved {PartitionCount} high-watermark(s) for topic {Topic}.", topicHighWatermark.LastMessageIds.Count, _topic);
            return topicHighWatermark;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && highWatermarkCts.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Timed out resolving high-watermarks for topic {_topic} after {HighWatermarkLookupTimeout}. Check Pulsar broker connectivity and advertised listener configuration.");
        }
    }

    public async Task StartLiveTailAsync(CancellationToken cancellationToken)
    {
        await Task.CompletedTask;
        //var startMessageId = _liveTailStartMessageId ?? MessageId.Latest;

        //_logger.LogInformation("Starting live tail for topic {Topic} from message id {MessageId}.", _topic, startMessageId);

        //await foreach (var message in _readLiveMessages(startMessageId, cancellationToken).WithCancellation(cancellationToken))
        //{
        //    await TryApplyMessageAsync(message, emitEvents: true, cancellationToken);
        //}
    }

    [ExcludeFromCodeCoverage]
    private static async ValueTask<IReadOnlyDictionary<int, PulsarMessageId>> GetHighWatermarksAsync(IPulsarClient client,
                                                                                                      string topic,
                                                                                                      CancellationToken cancellationToken)
    {
        await using var reader = client.NewReader(Schema.ByteSequence)
            .Topic(topic)
            .StartMessageId(MessageId.Earliest)
            .ReadCompacted(true)
            .Create();

        var messageIds = await reader.GetLastMessageIds(cancellationToken);
        var watermarks = new Dictionary<int, PulsarMessageId>(messageIds.Count());

        foreach (var messageId in messageIds)
        {
            watermarks[messageId.Partition] = ToPulsarMessageId(messageId);
        }

        return watermarks;
    }


    [ExcludeFromCodeCoverage]
    private static async IAsyncEnumerable<TableViewMessage> ReadBootstrapMessagesAsync(IPulsarClient client,
                                                                                        string topic,
                                                                                        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await using var reader = client.NewReader(Schema.ByteSequence)
            .Topic(topic)
            .StartMessageId(MessageId.Earliest)
            .ReadCompacted(true)
            .Create();

        await foreach (var message in reader.Messages(cancellationToken))
        {
            yield return new TableViewMessage(topic, 0, message.Key, message.Data, ToPulsarMessageId(message.MessageId));
        }
    }

    [ExcludeFromCodeCoverage]
    private static async IAsyncEnumerable<TableViewMessage> ReadLiveMessagesAsync(IPulsarClient client,
                                                                                   string topic,
                                                                                   MessageId startMessageId,
                                                                                   [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await using var reader = client.NewReader(Schema.ByteSequence)
            .Topic(topic)
            .StartMessageId(startMessageId)
            .Create();

        await foreach (var message in reader.Messages(cancellationToken))
        {
            yield return new TableViewMessage(topic, 0, message.Key, message.Data, ToPulsarMessageId(message.MessageId));
        }
    }

    //private static PulsarMessageId SelectLiveTailStartMessageId(IReadOnlyDictionary<int, PulsarMessageId> targetWatermarks)
    //{
    //    return targetWatermarks.Values.Aggregate((current, candidate) =>
    //        HasReachedOrExceeded(candidate, current) ? candidate : current);
    //}

    private static MessageId ToDotPulsarMessageId(PulsarMessageId messageId, string topic)
        => new(checked((ulong)messageId.LedgerId), checked((ulong)messageId.EntryId), messageId.PartitionIndex, messageId.BatchIndex, topic);

    private static PulsarMessageId ToPulsarMessageId(MessageId messageId)
    {
        // You won't want to save 'Latest' as a checkpoint with long.maxValue
        if (messageId == MessageId.Earliest || messageId == MessageId.Latest)
        {
            return new PulsarMessageId(-1, -1, messageId.Partition, -1);
        }

        // If we get here, it's a real physical ID; Ledger and Entry won't exceed long.MaxValue under normal conditions.
        return new PulsarMessageId((long)messageId.LedgerId, (long)messageId.EntryId, messageId.Partition, messageId.BatchIndex);
    }

    //private async Task<bool> TryApplyMessageAsync(TableViewMessage message, bool emitEvents, CancellationToken cancellationToken)
    //{
    //    if (string.IsNullOrWhiteSpace(message.Key))
    //    {
    //        _logger.LogWarning("Skipping message without key at {LedgerId}:{EntryId}:{PartitionIndex}.",
    //                           message.MessageId.LedgerId, message.MessageId.EntryId, message.MessageId.PartitionIndex);
    //        // TODO: save rejected message
    //        return false;
    //    }

    //    if (message.Data.Length == 0)
    //    {
    //        await WhenArriveTombStone(message.Key, message.MessageId, emitEvents, cancellationToken);
    //        return true;
    //    }

    //    TMessage incomingValue = _valueDeserializer(message.Data);
    //    string key = message.Key!;

    //    var capturedOldValue = await _messageStorage.TryLoadAsync(key, cancellationToken);
    //    await _messageStorage.UpsertAsync(incomingValue, cancellationToken);


    //    if (emitEvents)
    //    {
    //        if (capturedOldValue == null)
    //        {
    //            _subject.OnNext(new EventCreated<TMessage>(message.Key, incomingValue));
    //        }
    //        else
    //        {
    //            _subject.OnNext(new EventUpdated<TMessage>(message.Key, incomingValue, capturedOldValue!));
    //        }
    //    }
    //    else
    //    {
    //        _localStorage.AddOrUpdate(message.Key, incomingValue, (_, _) => incomingValue);
    //    }

    //    return true;
    //}

    
}
