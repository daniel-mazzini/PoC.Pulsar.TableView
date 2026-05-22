using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Microsoft.Extensions.Logging;
using PoC.Pulsar.TableView.Infrastructure.Store.Abstractions;

namespace PoC.Pulsar.TableView.Infrastructure.Store;

public abstract record Event<T>(string Key);
public record DeleteEvent<T>(string Key) : Event<T>(Key);
public record UpdateEvent<T>(string Key, T NewValue) : Event<T>(Key);
internal readonly record struct TableViewMessage(string? Key, ReadOnlySequence<byte> Data, PulsarMessageId MessageId);

public sealed class PulsarTableView<TValue> : IPulsarTableView<TValue>
    where TValue : class
{
    private readonly IStateStore<string, TValue> _stateStore;
    private readonly Func<ReadOnlySequence<byte>, TValue> _valueDeserializer;
    private readonly Func<CancellationToken, ValueTask<IReadOnlyDictionary<int, PulsarMessageId>>> _getHighWatermarks;
    private readonly Func<CancellationToken, IAsyncEnumerable<TableViewMessage>> _readBootstrapMessages;
    private readonly Func<MessageId, CancellationToken, IAsyncEnumerable<TableViewMessage>> _readLiveMessages;
    private readonly ILogger<PulsarTableView<TValue>> _logger;
    private readonly string _topic;
    private readonly Subject<Event<TValue>> _subject = new();
    private MessageId? _liveTailStartMessageId;

    public IObservable<Event<TValue>> OnUpdate => _subject.AsObservable();

    [ExcludeFromCodeCoverage]
    public PulsarTableView(IPulsarClient client,
                           string topic,
                           Func<ReadOnlySequence<byte>, TValue> valueDeserializer,
                           IStateStore<string, TValue> stateStore,
                           ILogger<PulsarTableView<TValue>> logger)
        : this(stateStore,
               topic,
               valueDeserializer,
               cancellationToken => GetHighWatermarksAsync(client, topic, cancellationToken),
               cancellationToken => ReadBootstrapMessagesAsync(client, topic, cancellationToken),
               (startMessageId, cancellationToken) => ReadLiveMessagesAsync(client, topic, startMessageId, cancellationToken),
               logger)
    {
    }

    internal PulsarTableView(IStateStore<string, TValue> stateStore,
                             string topic,
                             Func<ReadOnlySequence<byte>, TValue> valueDeserializer,
                             Func<CancellationToken, ValueTask<IReadOnlyDictionary<int, PulsarMessageId>>> getHighWatermarks,
                             Func<CancellationToken, IAsyncEnumerable<TableViewMessage>> readBootstrapMessages,
                             Func<MessageId, CancellationToken, IAsyncEnumerable<TableViewMessage>> readLiveMessages,
                             ILogger<PulsarTableView<TValue>> logger)
    {
        _stateStore = stateStore;
        _topic = topic;
        _valueDeserializer = valueDeserializer;
        _getHighWatermarks = getHighWatermarks;
        _readBootstrapMessages = readBootstrapMessages;
        _readLiveMessages = readLiveMessages;
        _logger = logger;
    }

    public TValue? Get(string key) => _stateStore.Get(key);

    public async Task StartBootstrapAsync(CancellationToken cancellationToken = default)
    {
        var targetWatermarks = await _getHighWatermarks(cancellationToken);

        if (targetWatermarks.Count == 0)
        {
            _liveTailStartMessageId = null;
            _logger.LogInformation("No messages found in topic {Topic}. Bootstrap completed instantly.", _topic);
            return;
        }

        _liveTailStartMessageId = ToDotPulsarMessageId(SelectLiveTailStartMessageId(targetWatermarks), _topic);

        _logger.LogInformation("Starting bootstrap for topic {Topic} across {PartitionCount} partitions.", _topic, targetWatermarks.Count);

        var completedPartitions = new HashSet<int>();

        await foreach (var message in _readBootstrapMessages(cancellationToken).WithCancellation(cancellationToken))
        {
            if (!TryApplyMessage(message, emitEvents: false))
            {
                continue;
            }

            var currentPartition = message.MessageId.PartitionIndex;

            if (targetWatermarks.TryGetValue(currentPartition, out var watermarkForPartition)
                && HasReachedOrExceeded(message.MessageId, watermarkForPartition)
                && completedPartitions.Add(currentPartition))
            {
                _logger.LogInformation("Partition {PartitionIndex} reached its high-watermark ({LedgerId}:{EntryId}).",
                                       currentPartition, watermarkForPartition.LedgerId, watermarkForPartition.EntryId);
            }

            if (completedPartitions.Count == targetWatermarks.Count)
            {
                _logger.LogInformation("Bootstrap successfully completed for all {PartitionCount} partitions of topic {Topic}.",
                                       targetWatermarks.Count, _topic);
                return;
            }
        }
    }

    public async Task StartLiveTailAsync(CancellationToken cancellationToken)
    {
        var startMessageId = _liveTailStartMessageId ?? MessageId.Latest;

        _logger.LogInformation("Starting live tail for topic {Topic} from message id {MessageId}.", _topic, startMessageId);

        await foreach (var message in _readLiveMessages(startMessageId, cancellationToken).WithCancellation(cancellationToken))
        {
            TryApplyMessage(message, emitEvents: true);
        }
    }

    private bool TryApplyMessage(TableViewMessage message, bool emitEvents)
    {
        if (string.IsNullOrWhiteSpace(message.Key))
        {
            _logger.LogWarning("Skipping message without key at {LedgerId}:{EntryId}:{PartitionIndex}.",
                               message.MessageId.LedgerId, message.MessageId.EntryId, message.MessageId.PartitionIndex);
            return false;
        }

        if (message.Data.Length == 0)
        {
            _stateStore.Delete(message.Key);
            _stateStore.SaveCheckpoint(message.MessageId);

            if (emitEvents)
            {
                _subject.OnNext(new DeleteEvent<TValue>(message.Key));
            }

            return true;
        }

        var value = _valueDeserializer(message.Data);
        _stateStore.Upsert(message.Key, value);
        _stateStore.SaveCheckpoint(message.MessageId);

        if (emitEvents)
        {
            _subject.OnNext(new UpdateEvent<TValue>(message.Key, value));
        }

        return true;
    }

    [ExcludeFromCodeCoverage]
    private static async ValueTask<IReadOnlyDictionary<int, PulsarMessageId>> GetHighWatermarksAsync(IPulsarClient client,
                                                                                                      string topic,
                                                                                                      CancellationToken cancellationToken)
    {
        await using var reader = client.NewReader(Schema.ByteSequence)
            .Topic(topic)
            .StartMessageId(MessageId.Earliest)
            //.ReadCompacted(true)
            .Create();

        var messageIds = await reader.GetLastMessageIds(cancellationToken);
        var watermarks = new Dictionary<int, PulsarMessageId>();

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
            yield return new TableViewMessage(message.Key, message.Data, ToPulsarMessageId(message.MessageId));
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
            yield return new TableViewMessage(message.Key, message.Data, ToPulsarMessageId(message.MessageId));
        }
    }

    private static PulsarMessageId SelectLiveTailStartMessageId(IReadOnlyDictionary<int, PulsarMessageId> targetWatermarks)
    {
        return targetWatermarks.Values.Aggregate((current, candidate) =>
            HasReachedOrExceeded(candidate, current) ? candidate : current);
    }

    private static bool HasReachedOrExceeded(PulsarMessageId current, PulsarMessageId target)
    {
        if (current.LedgerId != target.LedgerId)
        {
            return current.LedgerId > target.LedgerId;
        }

        return current.EntryId >= target.EntryId;
    }

    private static PulsarMessageId ToPulsarMessageId(MessageId messageId)
        => new(checked((long)messageId.LedgerId), checked((long)messageId.EntryId), messageId.Partition);

    private static MessageId ToDotPulsarMessageId(PulsarMessageId messageId, string topic)
        => new(checked((ulong)messageId.LedgerId), checked((ulong)messageId.EntryId), messageId.PartitionIndex, -1, topic);

    public IAsyncEnumerable<TValue> GetAllAsync(CancellationToken cancellationToken = default) => _stateStore.GetAllAsync(cancellationToken);

}
