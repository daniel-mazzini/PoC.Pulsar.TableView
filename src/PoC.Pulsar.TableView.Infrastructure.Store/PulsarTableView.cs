using System.Buffers;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Microsoft.Extensions.Logging;
using PoC.Pulsar.TableView.Infrastructure.Store.Abstractions;

namespace PoC.Pulsar.TableView.Infrastructure.Store;

// --- DUs para los eventos ---
public abstract record Event<T>(string Key);
public record DeleteEvent<T>(string Key) : Event<T>(Key);
public record UpdateEvent<T>(string Key, T NewValue) : Event<T>(Key);
internal readonly record struct BootstrapMessage(string? Key, ReadOnlySequence<byte> Data, PulsarMessageId MessageId);

public sealed class PulsarTableView<TValue> : IPulsarTableView<TValue>
    where TValue : class
{
    private readonly IStateStore<string, TValue> _stateStore;
    private readonly Func<ReadOnlySequence<byte>, TValue> _valueDeserializer;
    
    // Ahora devuelve un diccionario de MessageId por cada Partición
    private readonly Func<CancellationToken, ValueTask<IReadOnlyDictionary<int, PulsarMessageId>>> _getHighWatermarks;
    private readonly Func<CancellationToken, IAsyncEnumerable<BootstrapMessage>> _readBootstrapMessages;
    
    private readonly ILogger<PulsarTableView<TValue>> _logger;
    private readonly string _topic;
    private readonly Subject<Event<TValue>> _subject = new();

    // El flujo al que se suscribirá tu Proyector (Live Tail)
    public IObservable<Event<TValue>> OnUpdate => _subject.AsObservable();

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
               logger)
    {
    }

    internal PulsarTableView(IStateStore<string, TValue> stateStore,
                             string topic,
                             Func<ReadOnlySequence<byte>, TValue> valueDeserializer,
                             Func<CancellationToken, ValueTask<IReadOnlyDictionary<int, PulsarMessageId>>> getHighWatermarks,
                             Func<CancellationToken, IAsyncEnumerable<BootstrapMessage>> readBootstrapMessages,
                             ILogger<PulsarTableView<TValue>> logger)
    {
        _stateStore = stateStore;
        _valueDeserializer = valueDeserializer;
        _getHighWatermarks = getHighWatermarks;
        _readBootstrapMessages = readBootstrapMessages;
        _logger = logger;
        _topic = topic;
    }

    public TValue? Get(string key) => _stateStore.Get(key);

    public IEnumerable<TValue> GetAll() => _stateStore.GetAll();

    public async Task StartBootstrapAsync(CancellationToken cancellationToken = default)
    {
        // 1. Get the high-watermark for each partition to know when we can consider the bootstrap complete
        var targetWatermarks = await _getHighWatermarks(cancellationToken);

        if (targetWatermarks.Count == 0)
        {
            _logger.LogInformation("No messages found in topic {Topic}. Bootstrap completed instantly.", _topic);
            return;
        }

        _logger.LogInformation("Starting bootstrap for topic {Topic} across {PartitionCount} partitions.", _topic, targetWatermarks.Count);

        var completedPartitions = new HashSet<int>();

        // 2. Read messages in order (compacted) and apply them to the state store until we reach the high-watermark for each partition
        await foreach (var message in _readBootstrapMessages(cancellationToken).WithCancellation(cancellationToken))
        {
            if (string.IsNullOrWhiteSpace(message.Key))
            {
                _logger.LogWarning("Skipping message without key during bootstrap at {LedgerId}:{EntryId}:{PartitionIndex}.",
                                   message.MessageId.LedgerId, message.MessageId.EntryId, message.MessageId.PartitionIndex);
                continue;
            }

            // 3. Apply the message to the state store. 
            //   - If the message has empty data, we treat it as a delete (tombstone).
            //   - Otherwise, we deserialize and upsert the value.
            //   - Never emit events to the subject during bootstrap.
            if (message.Data.Length == 0)
            {
                _stateStore.Delete(message.Key);
            }
            else
            {
                var value = _valueDeserializer(message.Data);
                _stateStore.Upsert(message.Key, value);
            }

            // 4. Verify if we've reached the high-watermark for the partition of the current message. If so, mark that partition as completed.
            var currentPartition = message.MessageId.PartitionIndex;
            
            if (targetWatermarks.TryGetValue(currentPartition, out var watermarkForPartition))
            {
                if (HasReachedOrExceeded(message.MessageId, watermarkForPartition))
                {
                    if (completedPartitions.Add(currentPartition))
                    {
                        _logger.LogInformation("Partition {PartitionIndex} reached its high-watermark ({LedgerId}:{EntryId}).", 
                                               currentPartition, watermarkForPartition.LedgerId, watermarkForPartition.EntryId);
                    }
                }
            }

            // 5. Exit condition: if we've completed all partitions, we can consider the bootstrap complete and exit the loop.
            if (completedPartitions.Count == targetWatermarks.Count)
            {
                _logger.LogInformation("Bootstrap successfully completed for all {PartitionCount} partitions of topic {Topic}.", 
                                       targetWatermarks.Count, _topic);
                return;
            }
        }
    }

    private static async ValueTask<IReadOnlyDictionary<int, PulsarMessageId>> GetHighWatermarksAsync(IPulsarClient client,
                                                                                                     string topic,
                                                                                                     CancellationToken cancellationToken)
    {
        await using var reader = client.NewReader(Schema.ByteSequence)
            .Topic(topic)
            .StartMessageId(MessageId.Latest)
            .Create();

        var messageIds = await reader.GetLastMessageIds(cancellationToken);
        var watermarks = new Dictionary<int, PulsarMessageId>();

        foreach (var msgId in messageIds)
        {
            watermarks[msgId.Partition] = ToPulsarMessageId(msgId);
        }

        return watermarks;
    }

    private static async IAsyncEnumerable<BootstrapMessage> ReadBootstrapMessagesAsync(IPulsarClient client,
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
            yield return new BootstrapMessage(message.Key, message.Data, ToPulsarMessageId(message.MessageId));
        }
    }

    private static bool HasReachedOrExceeded(PulsarMessageId current, PulsarMessageId target)
    {
        // Pulsar orders messages first by LedgerId, and within the same Ledger, by EntryId.
        if (current.LedgerId != target.LedgerId)
        {
            return current.LedgerId > target.LedgerId;
        }

        return current.EntryId >= target.EntryId;
    }

    private static PulsarMessageId ToPulsarMessageId(MessageId messageId) 
        => new PulsarMessageId(checked((long)messageId.LedgerId),
                               checked((long)messageId.EntryId),
                               messageId.Partition);

    
}