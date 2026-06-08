using System.Buffers;
using DotPulsar;
using Microsoft.Extensions.Logging.Abstractions;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Checkpoints;
using PoC.Pulsar.TableView.Domain.Filter;
using PoC.Pulsar.TableView.Domain.Metadatas;
using PoC.Pulsar.TableView.Domain.Projector;
using PoC.Pulsar.TableView.Domain.Rejected;
using PoC.Pulsar.TableView.Domain.Serializers;
using PoC.Pulsar.TableView.Domain.Storages.Entities;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Domain.TableView;
using PoC.Pulsar.TableView.Infrastructure.Store;
using PoC.Pulsar.TableView.Infrastructure.Store.Readers;
using Xunit;

namespace PoC.Pulsar.TableView.Observability.UnitTests;

public sealed class PulsarTableViewObservabilityTests
{
    [Fact]
    [Trait("Category", "UnitTest")]
    public async Task start_live_tail_async_should_record_live_tail_error_metric()
    {
        using var collector = new ObservabilityCollector();
        const string topic = "persistent://public/default/sports";
        var readerStrategy = new FailingReaderStrategy(topic);
        var view = new PulsarTableView<SportMessage>(topic,
                                                      readerStrategy,
                                                      new FakeUnitOfWorkFactory(),
                                                      new StubAvroSerializer(),
                                                      new StubMessageApplier(),
                                                      new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: true, CreatedAt: DateTimeOffset.UtcNow),
                                                      NullLogger<PulsarTableView<SportMessage>>.Instance);

        await Assert.ThrowsAsync<InvalidOperationException>(() => view.StartLiveTailAsync(CancellationToken.None));

        Assert.True(collector.HasLongSum("projector.topic.reader.errors.total",
                                         1,
                                         new("topic", topic),
                                         new("phase", "live")));
        Assert.True(collector.HasActivity("topic reader failed",
                                          new("topic", topic),
                                          new("phase", "live"),
                                          new("result", "error")));
    }

    private sealed class FailingReaderStrategy(string topic) : ITopicShardReaderStrategy
    {
        private readonly TopicShard _shard = TopicShard.Partition(topic, 0);

        public Task<TopicHighWatermark> CaptureHighWatermarkAsync(string logicalTopic, CancellationToken cancellationToken)
            => Task.FromResult(new TopicHighWatermark(logicalTopic, []));

        public Task<IReadOnlyCollection<TopicShard>> DiscoverShardsAsync(string logicalTopic, CancellationToken cancellationToken)
            => Task.FromResult<IReadOnlyCollection<TopicShard>>([_shard]);

        public Task<IProjectorTopicReader> CreateReaderAsync(TopicShard shard, MessageId startMessageId, CancellationToken cancellationToken)
            => throw new InvalidOperationException("reader failed");
    }

    private sealed class FakeUnitOfWorkFactory : IUnitOfWorkFactory
    {
        public ITableViewUnitOfWork<TMessage> CreateBootstrap<TMessage>() => new FakeTableViewUnitOfWork<TMessage>();
        public IGeoTaxonomyBuildUnitOfWork CreateGeoTaxonomyBuild() => throw new NotSupportedException();
        public Task MoveDurableAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class FakeTableViewUnitOfWork<TMessage> : ITableViewUnitOfWork<TMessage>
    {
        public IMessageStorage<string, TMessage> MessageStorage { get; } = new FakeMessageStorage<TMessage>();
        public ICheckpointStorage CheckpointStorage { get; } = new FakeCheckpointStorage();
        public IRejectedStorage RejectedStorage { get; } = new FakeRejectedStorage();
        public Task CommitAsync(CancellationToken ct) => Task.CompletedTask;
        public void Dispose() { }
    }

    private sealed class FakeCheckpointStorage : ICheckpointStorage
    {
        public Task SaveCheckpointAsync(TopicShard shard, PulsarMessageId lastProcessedMessageId, CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask<TopicCheckpoint?> GetLastCheckpoint(TopicShard shard, CancellationToken cancellationToken) => ValueTask.FromResult<TopicCheckpoint?>(null);
        public Task SaveViewCheckpointAsync(string viewName, CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask<ViewCheckpoint?> GetViewCheckpointAsync(string viewName, CancellationToken cancellationToken) => ValueTask.FromResult<ViewCheckpoint?>(null);
    }

    private sealed class FakeMessageStorage<TMessage> : IMessageStorage<string, TMessage>
    {
        public ValueTask DeleteAsync(string id, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask ClearAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask<TMessage?> TryLoadAsync(string id, CancellationToken cancellationToken) => ValueTask.FromResult<TMessage?>(default);
        public ValueTask UpsertAsync(TMessage message, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask<TableMessageApplyDecision> TryApplyAsync(TMessage message, CancellationToken cancellationToken) => ValueTask.FromResult(TableMessageApplyDecision.Created());
        public Dictionary<string, TMessage> GetAll(IValuePredicate<TMessage>? valuePredicate = null) => [];
    }

    private sealed class FakeRejectedStorage : IRejectedStorage
    {
        public ValueTask SaveRejectedRecordAsync(RejectedProjection rejectedProjection, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    private sealed class StubAvroSerializer : IAvroSerializer
    {
        public T Deserialize<T>(ReadOnlySpan<byte> data) => throw new NotSupportedException();
        public T Deserialize<T>(ReadOnlySequence<byte> data) => throw new NotSupportedException();
        public Task<T> DeserializeFromStream<T>(Stream stream, CancellationToken cancellationToken) => throw new NotSupportedException();
        public void Serialize<T>(T message, Stream output) => throw new NotSupportedException();
    }

    private sealed class StubMessageApplier : ITableViewMessageApplier<SportMessage>
    {
        public ValueTask<TableMessageApplyResult<SportMessage>> ApplyAsync(TableViewMessage input,
                                                                           ProcessPhase processPhase,
                                                                           ITableViewUnitOfWork<SportMessage> tableViewUnitOfWork,
                                                                           Func<ReadOnlySequence<byte>, SportMessage> serialize,
                                                                           CancellationToken cancellationToken)
            => throw new NotSupportedException();
    }
}
