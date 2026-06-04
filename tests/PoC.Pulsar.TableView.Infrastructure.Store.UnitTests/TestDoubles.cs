using DotPulsar;
using Microsoft.Extensions.Logging;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Checkpoints;
using PoC.Pulsar.TableView.Domain.Filter;
using PoC.Pulsar.TableView.Domain.Rejected;
using PoC.Pulsar.TableView.Domain.Serializers;
using PoC.Pulsar.TableView.Domain.Storages.Entities;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Domain.TableView;
using PoC.Pulsar.TableView.Infrastructure.Store.Readers;
using System.Buffers;
using System.Text.Json;

namespace PoC.Pulsar.TableView.Infrastructure.Store.UnitTests;

internal sealed class JsonAvroSerializer : IAvroSerializer
{
    public T Deserialize<T>(ReadOnlySpan<byte> data)
        => JsonSerializer.Deserialize<T>(data) ?? throw new InvalidOperationException($"Could not deserialize {typeof(T).Name}.");

    public T Deserialize<T>(ReadOnlySequence<byte> data)
        => Deserialize<T>(data.ToArray());

    public async Task<T> DeserializeFromStream<T>(Stream stream, CancellationToken cancellationToken)
        => (await JsonSerializer.DeserializeAsync<T>(stream, cancellationToken: cancellationToken))
           ?? throw new InvalidOperationException($"Could not deserialize {typeof(T).Name}.");

    public void Serialize<T>(T message, Stream output)
        => JsonSerializer.Serialize(output, message);
}

internal sealed class FakeRejectedMessagePublisher : IRejectedMessagePublisher
{
    public List<object> PublishedMessages { get; } = [];
    public List<Dictionary<string, string>> PublishedHeaders { get; } = [];

    public Task PublishAsync<TMessage>(Rejected<TMessage> write, Dictionary<string, string> headers, CancellationToken cancellationToken)
    {
        PublishedMessages.Add(write!);
        PublishedHeaders.Add(new Dictionary<string, string>(headers, StringComparer.Ordinal));
        return Task.CompletedTask;
    }
}

internal sealed class FakeSportMessageStorage : IMessageStorage<string, SportMessage>
{
    private readonly Dictionary<string, SportMessage> _messages = new(StringComparer.Ordinal);

    public int ClearCallCount { get; private set; }
    public int UpsertCallCount { get; private set; }
    public int DeleteCallCount { get; private set; }

    public void Seed(SportMessage message)
        => _messages[message.Id] = Clone(message);

    public ValueTask DeleteAsync(string id, CancellationToken cancellationToken)
    {
        DeleteCallCount++;
        _messages.Remove(id);
        return ValueTask.CompletedTask;
    }

    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        ClearCallCount++;
        _messages.Clear();
        return ValueTask.CompletedTask;
    }

    public ValueTask<SportMessage?> TryLoadAsync(string id, CancellationToken cancellationToken)
        => ValueTask.FromResult(_messages.TryGetValue(id, out var message) ? Clone(message) : null);

    public ValueTask UpsertAsync(SportMessage message, CancellationToken cancellationToken)
    {
        UpsertCallCount++;
        _messages[message.Id] = Clone(message);
        return ValueTask.CompletedTask;
    }

    public Dictionary<string, SportMessage> GetAll(IValuePredicate<SportMessage>? valuePredicate = null)
    {
        if (valuePredicate is null)
        {
            return _messages.ToDictionary(pair => pair.Key, pair => Clone(pair.Value), StringComparer.Ordinal);
        }

        return _messages.Where(pair => valuePredicate.Match(pair.Value))
                        .ToDictionary(pair => pair.Key, pair => Clone(pair.Value), StringComparer.Ordinal);
    }

    public SportMessage? GetById(string id)
        => _messages.TryGetValue(id, out var message) ? Clone(message) : null;

    private static SportMessage Clone(SportMessage message)
        => new()
        {
            Id = message.Id,
            Name = message.Name,
            SportType = message.SportType,
            Provider = message.Provider,
            EntityCoverage = message.EntityCoverage,
            Version = message.Version,
            ExternalEntities = message.ExternalEntities.Select(entity => new ExternalEntity
            {
                Id = entity.Id,
                Provider = entity.Provider,
                EntityCoverage = entity.EntityCoverage,
                DefaultName = entity.DefaultName
            }).ToList()
        };
}

internal sealed class FakeRawCategoryMessageStorage : IMessageStorage<string, RawCategoryMessage>
{
    private readonly Dictionary<string, RawCategoryMessage> _messages = new(StringComparer.Ordinal);

    public int ClearCallCount { get; private set; }
    public int UpsertCallCount { get; private set; }
    public int DeleteCallCount { get; private set; }

    public void Seed(RawCategoryMessage message)
        => _messages[message.Id] = Clone(message);

    public ValueTask DeleteAsync(string id, CancellationToken cancellationToken)
    {
        DeleteCallCount++;
        _messages.Remove(id);
        return ValueTask.CompletedTask;
    }

    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        ClearCallCount++;
        _messages.Clear();
        return ValueTask.CompletedTask;
    }

    public ValueTask<RawCategoryMessage?> TryLoadAsync(string id, CancellationToken cancellationToken)
        => ValueTask.FromResult(_messages.TryGetValue(id, out var message) ? Clone(message) : null);

    public ValueTask UpsertAsync(RawCategoryMessage message, CancellationToken cancellationToken)
    {
        UpsertCallCount++;
        _messages[message.Id] = Clone(message);
        return ValueTask.CompletedTask;
    }

    public Dictionary<string, RawCategoryMessage> GetAll(IValuePredicate<RawCategoryMessage>? valuePredicate = null)
    {
        if (valuePredicate is null)
        {
            return _messages.ToDictionary(pair => pair.Key, pair => Clone(pair.Value), StringComparer.Ordinal);
        }

        return _messages.Where(pair => valuePredicate.Match(pair.Value))
                        .ToDictionary(pair => pair.Key, pair => Clone(pair.Value), StringComparer.Ordinal);
    }

    public RawCategoryMessage? GetById(string id)
        => _messages.TryGetValue(id, out var message) ? Clone(message) : null;

    private static RawCategoryMessage Clone(RawCategoryMessage message)
        => new()
        {
            Id = message.Id,
            Name = message.Name,
            SportId = message.SportId,
            ParentId = message.ParentId,
            SportType = message.SportType,
            CountryCode = message.CountryCode,
            Gender = message.Gender,
            Provider = message.Provider,
            EntityCoverage = message.EntityCoverage,
            Version = message.Version,
            ExternalEntities = message.ExternalEntities.Select(entity => new ExternalEntity
            {
                Id = entity.Id,
                Provider = entity.Provider,
                EntityCoverage = entity.EntityCoverage,
                DefaultName = entity.DefaultName
            }).ToList()
        };
}

internal sealed class FakeRejectedStorage : IRejectedStorage
{
    public RejectedProjection? LastSaved { get; private set; }
    public int SaveCallCount { get; private set; }

    public ValueTask SaveRejectedRecordAsync(RejectedProjection rejectedProjection, CancellationToken cancellationToken)
    {
        SaveCallCount++;
        LastSaved = rejectedProjection;
        return ValueTask.CompletedTask;
    }
}

internal sealed class FakeCheckpointStorage : ICheckpointStorage
{
    public TopicCheckpoint? LastSaved { get; private set; }
    public List<TopicCheckpoint> SavedCheckpoints { get; } = [];
    public Dictionary<(string TopicName, int PartitionId), TopicCheckpoint> Checkpoints { get; } = new();
    public ViewCheckpoint? LastSavedViewCheckpoint { get; private set; }
    public Dictionary<string, ViewCheckpoint> ViewCheckpoints { get; } = new(StringComparer.Ordinal);
    public string CurrentStoreId { get; set; } = Guid.NewGuid().ToString("D");

    public void Seed(TopicCheckpoint checkpoint)
        => Checkpoints[(checkpoint.TopicName, checkpoint.PartitionId)] = checkpoint;

    public void Seed(ViewCheckpoint checkpoint)
        => ViewCheckpoints[checkpoint.ViewName] = checkpoint;

    public Task SaveCheckpointAsync(string topicName, int partitionId, PulsarMessageId lastProcessedMessageId, CancellationToken cancellationToken)
    {
        var checkpoint = new TopicCheckpoint(topicName, partitionId, lastProcessedMessageId, Guid.NewGuid(), DateTimeOffset.UtcNow);
        LastSaved = checkpoint;
        SavedCheckpoints.Add(checkpoint);
        Checkpoints[(topicName, partitionId)] = checkpoint;
        return Task.CompletedTask;
    }

    public ValueTask<TopicCheckpoint?> GetLastCheckpoint(string topicName, int partitionId, CancellationToken cancellationToken)
        => ValueTask.FromResult(Checkpoints.TryGetValue((topicName, partitionId), out var checkpoint) ? checkpoint : null);

    public Task SaveViewCheckpointAsync(string viewName, CancellationToken cancellationToken)
    {
        var checkpoint = new ViewCheckpoint(viewName, CurrentStoreId, BuildCompleted: true, DateTimeOffset.UtcNow);
        LastSavedViewCheckpoint = checkpoint;
        ViewCheckpoints[viewName] = checkpoint;
        return Task.CompletedTask;
    }

    public ValueTask<ViewCheckpoint?> GetViewCheckpointAsync(string viewName, CancellationToken cancellationToken)
        => ValueTask.FromResult(ViewCheckpoints.TryGetValue(viewName, out var checkpoint) ? checkpoint : null);
}

internal sealed class FakeSportTableViewUnitOfWork : ITableViewUnitOfWork<SportMessage>
{
    public FakeSportTableViewUnitOfWork(FakeSportMessageStorage messageStorage, FakeCheckpointStorage checkpointStorage, FakeRejectedStorage rejectedStorage)
        => (MessageStorage, CheckpointStorage, RejectedStorage) = (messageStorage, checkpointStorage, rejectedStorage);

    public IMessageStorage<string, SportMessage> MessageStorage { get; }
    public ICheckpointStorage CheckpointStorage { get; }
    public IRejectedStorage RejectedStorage { get; }

    public Task CommitAsync(CancellationToken ct) => Task.CompletedTask;

    public void Dispose()
    {
    }
}

internal sealed class FakeCategoryTableViewUnitOfWork : ITableViewUnitOfWork<RawCategoryMessage>
{
    public FakeCategoryTableViewUnitOfWork(FakeRawCategoryMessageStorage messageStorage, FakeCheckpointStorage checkpointStorage, FakeRejectedStorage rejectedStorage)
        => (MessageStorage, CheckpointStorage, RejectedStorage) = (messageStorage, checkpointStorage, rejectedStorage);

    public IMessageStorage<string, RawCategoryMessage> MessageStorage { get; }
    public ICheckpointStorage CheckpointStorage { get; }
    public IRejectedStorage RejectedStorage { get; }

    public Task CommitAsync(CancellationToken ct) => Task.CompletedTask;

    public void Dispose()
    {
    }
}

internal sealed class FakeUnitOfWorkFactory : IUnitOfWorkFactory
{
    private readonly FakeSportTableViewUnitOfWork _unitOfWork;

    public FakeUnitOfWorkFactory(FakeSportTableViewUnitOfWork unitOfWork)
        => _unitOfWork = unitOfWork;

    public ITableViewUnitOfWork<TMessage> CreateBootstrap<TMessage>()
    {
        if (typeof(TMessage) != typeof(SportMessage))
        {
            throw new NotSupportedException($"Unsupported message type {typeof(TMessage).FullName}.");
        }

        return (ITableViewUnitOfWork<TMessage>)(object)_unitOfWork;
    }

    public Task MoveDurableAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

internal sealed class FakeProjectorTopicReaderFactory : IProjectorTopicReaderFactory
{
    private readonly Dictionary<(string TopicName, int PartitionId), Queue<TableViewMessage>> _messages = new();
    private readonly Dictionary<string, TopicHighWatermark> _highWatermarks = new(StringComparer.Ordinal);

    public MessageId? LastStartMessageId { get; private set; }

    public void SeedHighWatermark(string topicName, int partitionId, MessageId messageId)
        => _highWatermarks[topicName] = new TopicHighWatermark(topicName, new Dictionary<int, MessageId> { [partitionId] = messageId });

    public void SeedMessages(string topicName, int partitionId, params TableViewMessage[] messages)
        => _messages[(topicName, partitionId)] = new Queue<TableViewMessage>(messages);

    public Task<TopicHighWatermark> CaptureHighWatermarkAsync(string topicName, CancellationToken cancellationToken)
        => Task.FromResult(_highWatermarks[topicName]);

    public Task<IProjectorTopicReader> CreateReaderAsync(string topicName, int partitionId, MessageId startMessageId, CancellationToken cancellationToken)
    {
        LastStartMessageId = startMessageId;
        _messages.TryGetValue((topicName, partitionId), out var messages);
        return Task.FromResult<IProjectorTopicReader>(new FakeProjectorTopicReader(messages ?? new Queue<TableViewMessage>()));
    }

    private sealed class FakeProjectorTopicReader : IProjectorTopicReader
    {
        private readonly Queue<TableViewMessage> _messages;

        public FakeProjectorTopicReader(Queue<TableViewMessage> messages)
            => _messages = messages;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public Task<TableViewMessage> ReceiveAsync(CancellationToken cancellationToken)
        {
            if (_messages.Count == 0)
            {
                throw new InvalidOperationException("No more bootstrap messages were configured.");
            }

            return Task.FromResult(_messages.Dequeue());
        }

        public async IAsyncEnumerable<TableViewMessage> ReadAllAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            while (_messages.Count > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return _messages.Dequeue();
                await Task.Yield();
            }
        }
    }
}

internal sealed class TestLogger<T> : ILogger<T>
{
    public List<string> Messages { get; } = [];

    public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        => Messages.Add(formatter(state, exception));

    private sealed class NullScope : IDisposable
    {
        public static NullScope Instance { get; } = new();
        public void Dispose() { }
    }
}
