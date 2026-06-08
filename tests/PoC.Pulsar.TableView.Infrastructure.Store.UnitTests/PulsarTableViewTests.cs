using System.Buffers;
using System.Text.Json;
using DotPulsar;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Checkpoints;
using PoC.Pulsar.TableView.Domain.Metadatas;
using PoC.Pulsar.TableView.Domain.Projector;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Domain.TableView;
using Xunit;

namespace PoC.Pulsar.TableView.Infrastructure.Store.UnitTests;

public sealed class PulsarTableViewTests
{
    [Fact]
    public async Task start_bootstrap_async_should_recover_from_state_store_and_collect_delta_changes()
    {
        var topic = "persistent://public/default/sports";
        var storeMetadata = new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: true, CreatedAt: DateTimeOffset.UtcNow);
        var messageStorage = new FakeSportMessageStorage();
        messageStorage.Seed(Sport("sport-1", version: 1));
        var checkpointStorage = new FakeCheckpointStorage();
        var shard = TopicShard.Partition(topic, 0);
        checkpointStorage.Seed(new TopicCheckpoint(shard.LogicalTopic, shard.PhysicalTopic, shard.PartitionId, shard.IsPartitioned, new PulsarMessageId(1, 1, 0, 0), storeMetadata.StoreGenerationId, DateTimeOffset.UtcNow));
        var rejectedStorage = new FakeRejectedStorage();
        var unitOfWork = new FakeSportTableViewUnitOfWork(messageStorage, checkpointStorage, rejectedStorage);
        var unitOfWorkFactory = new FakeUnitOfWorkFactory(unitOfWork);
        var readerFactory = new FakeProjectorTopicReaderFactory();
        readerFactory.SeedHighWatermark(topic, 0, new MessageId(1, 2, 0, 0, shard.PhysicalTopic));
        readerFactory.SeedMessages(topic,
                                   0,
                                   CreateMessage(topic, 0, Sport("sport-1", version: 2), new PulsarMessageId(1, 2, 0, 0)));
        var publisher = new FakeRejectedMessagePublisher();
        var serializer = new JsonAvroSerializer();
        var applier = new SportMessageApplier(publisher);
        var logger = new TestLogger<PulsarTableView<SportMessage>>();

        var view = new PulsarTableView<SportMessage>(topic,
                                                      readerFactory,
                                                      unitOfWorkFactory,
                                                      serializer,
                                                      applier,
                                                      storeMetadata,
                                                     logger);

        var result = await view.StartBootstrapAsync(CancellationToken.None);

        var recovered = Assert.IsType<TopicRecoveredFromStateStore<SportMessage>>(result);
        var update = Assert.Single(recovered.DeltaChanges);
        var updated = Assert.IsType<TableEntryUpdated<SportMessage>>(update);
        Assert.Equal("sport-1", updated.Key);
        Assert.Equal(2, updated.NewValue.Version);
        Assert.Equal(2, (await view.GetEntry("sport-1", CancellationToken.None))!.Version);
        Assert.Equal(2, messageStorage.GetById("sport-1")!.Version);
        Assert.Equal(0, messageStorage.ClearCallCount);
        Assert.Single(checkpointStorage.SavedCheckpoints);
        Assert.Empty(publisher.PublishedMessages);
    }

    [Fact]
    public async Task start_bootstrap_async_should_rebuild_from_earliest_when_metadata_is_untrusted_and_clear_state_store()
    {
        var topic = "persistent://public/default/sports";
        var storeMetadata = new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: false, CreatedAt: DateTimeOffset.UtcNow);
        var messageStorage = new FakeSportMessageStorage();
        messageStorage.Seed(Sport("sport-1", version: 1));
        var checkpointStorage = new FakeCheckpointStorage();
        var rejectedStorage = new FakeRejectedStorage();
        var unitOfWork = new FakeSportTableViewUnitOfWork(messageStorage, checkpointStorage, rejectedStorage);
        var unitOfWorkFactory = new FakeUnitOfWorkFactory(unitOfWork);
        var readerFactory = new FakeProjectorTopicReaderFactory();
        readerFactory.SeedHighWatermark(topic, 0, new MessageId(1, 2, 0, 0, TopicShard.Partition(topic, 0).PhysicalTopic));
        readerFactory.SeedMessages(topic,
                                   0,
                                   CreateMessage(topic, 0, Sport("sport-2", version: 1), new PulsarMessageId(1, 2, 0, 0)));
        var publisher = new FakeRejectedMessagePublisher();
        var serializer = new JsonAvroSerializer();
        var applier = new SportMessageApplier(publisher);
        var logger = new TestLogger<PulsarTableView<SportMessage>>();

        var view = new PulsarTableView<SportMessage>(topic,
                                                      readerFactory,
                                                      unitOfWorkFactory,
                                                      serializer,
                                                      applier,
                                                      storeMetadata,
                                                     logger);

        var result = await view.StartBootstrapAsync(CancellationToken.None);

        var rebuilt = Assert.IsType<TopicRebuiltFromEarliest<SportMessage>>(result);
        Assert.Equal("store_metadata_untrusted", rebuilt.Reason);
        Assert.Equal(1, messageStorage.ClearCallCount);
        Assert.Null(messageStorage.GetById("sport-1"));
        Assert.Equal(1, messageStorage.GetById("sport-2")!.Version);
        Assert.Equal(0, recoveredDeltaCount(result));
        Assert.Empty(publisher.PublishedMessages);
        Assert.Single(checkpointStorage.SavedCheckpoints);
        Assert.Equal(1, (await view.GetEntry("sport-2", CancellationToken.None))!.Version);
    }

    [Fact]
    public async Task start_bootstrap_async_should_model_non_partitioned_topic_as_single_physical_shard()
    {
        var topic = "persistent://public/default/sports";
        var storeMetadata = new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: false, CreatedAt: DateTimeOffset.UtcNow);
        var messageStorage = new FakeSportMessageStorage();
        var checkpointStorage = new FakeCheckpointStorage();
        var rejectedStorage = new FakeRejectedStorage();
        var unitOfWork = new FakeSportTableViewUnitOfWork(messageStorage, checkpointStorage, rejectedStorage);
        var unitOfWorkFactory = new FakeUnitOfWorkFactory(unitOfWork);
        var readerFactory = new FakeProjectorTopicReaderFactory();
        readerFactory.SeedHighWatermark(topic, -1, new MessageId(1, 2, -1, 0, topic));
        readerFactory.SeedMessages(topic,
                                   -1,
                                   new TableViewMessage(topic,
                                                        0,
                                                        "sport-1",
                                                        new ReadOnlySequence<byte>(JsonSerializer.SerializeToUtf8Bytes(Sport("sport-1", version: 1))),
                                                        new PulsarMessageId(1, 2, -1, 0),
                                                        PhysicalTopicName: topic,
                                                        IsPartitioned: false));
        var view = new PulsarTableView<SportMessage>(topic,
                                                      readerFactory,
                                                      unitOfWorkFactory,
                                                      new JsonAvroSerializer(),
                                                      new SportMessageApplier(new FakeRejectedMessagePublisher()),
                                                      storeMetadata,
                                                      new TestLogger<PulsarTableView<SportMessage>>());

        await view.StartBootstrapAsync(CancellationToken.None);

        Assert.NotNull(checkpointStorage.LastSaved);
        Assert.Equal(topic, checkpointStorage.LastSaved!.LogicalTopic);
        Assert.Equal(topic, checkpointStorage.LastSaved.PhysicalTopic);
        Assert.Equal(0, checkpointStorage.LastSaved.PartitionId);
        Assert.False(checkpointStorage.LastSaved.IsPartitioned);
    }

    [Fact]
    public async Task start_bootstrap_async_should_collect_created_delta_and_update_snapshot_when_recovering_from_state_store()
    {
        var topic = "persistent://public/default/sports";
        var storeMetadata = new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: true, CreatedAt: DateTimeOffset.UtcNow);
        var messageStorage = new FakeSportMessageStorage();
        var checkpointStorage = new FakeCheckpointStorage();
        var shard = TopicShard.Partition(topic, 0);
        checkpointStorage.Seed(new TopicCheckpoint(shard.LogicalTopic, shard.PhysicalTopic, shard.PartitionId, shard.IsPartitioned, new PulsarMessageId(1, 1, 0, 0), storeMetadata.StoreGenerationId, DateTimeOffset.UtcNow));
        var unitOfWorkFactory = new FakeUnitOfWorkFactory(new FakeSportTableViewUnitOfWork(messageStorage, checkpointStorage, new FakeRejectedStorage()));
        var readerFactory = new FakeProjectorTopicReaderFactory();
        readerFactory.SeedHighWatermark(topic, 0, new MessageId(1, 2, 0, 0, shard.PhysicalTopic));
        readerFactory.SeedMessages(topic, 0, CreateMessage(topic, 0, Sport("sport-1", version: 1), new PulsarMessageId(1, 2, 0, 0)));

        var view = new PulsarTableView<SportMessage>(topic,
                                                     readerFactory,
                                                     unitOfWorkFactory,
                                                     new JsonAvroSerializer(),
                                                     new SportMessageApplier(new FakeRejectedMessagePublisher()),
                                                     storeMetadata,
                                                     new TestLogger<PulsarTableView<SportMessage>>());

        var result = await view.StartBootstrapAsync(CancellationToken.None);

        var recovered = Assert.IsType<TopicRecoveredFromStateStore<SportMessage>>(result);
        var created = Assert.IsType<TableEntryCreated<SportMessage>>(Assert.Single(recovered.DeltaChanges));
        Assert.Equal("sport-1", created.Key);
        Assert.Equal(1, (await view.GetEntry("sport-1", CancellationToken.None))!.Version);
        Assert.Equal(1, messageStorage.GetById("sport-1")!.Version);
    }

    [Fact]
    public async Task start_bootstrap_async_should_not_collect_delta_or_change_snapshot_when_apply_returns_noop()
    {
        var topic = "persistent://public/default/sports";
        var storeMetadata = new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: true, CreatedAt: DateTimeOffset.UtcNow);
        var messageStorage = new FakeSportMessageStorage();
        messageStorage.Seed(Sport("sport-1", version: 2));
        var checkpointStorage = new FakeCheckpointStorage();
        var shard = TopicShard.Partition(topic, 0);
        checkpointStorage.Seed(new TopicCheckpoint(shard.LogicalTopic, shard.PhysicalTopic, shard.PartitionId, shard.IsPartitioned, new PulsarMessageId(1, 1, 0, 0), storeMetadata.StoreGenerationId, DateTimeOffset.UtcNow));
        var unitOfWorkFactory = new FakeUnitOfWorkFactory(new FakeSportTableViewUnitOfWork(messageStorage, checkpointStorage, new FakeRejectedStorage()));
        var readerFactory = new FakeProjectorTopicReaderFactory();
        readerFactory.SeedHighWatermark(topic, 0, new MessageId(1, 2, 0, 0, shard.PhysicalTopic));
        readerFactory.SeedMessages(topic, 0, CreateMessage(topic, 0, Sport("sport-1", version: 2), new PulsarMessageId(1, 2, 0, 0)));

        var view = new PulsarTableView<SportMessage>(topic,
                                                     readerFactory,
                                                     unitOfWorkFactory,
                                                     new JsonAvroSerializer(),
                                                     new SportMessageApplier(new FakeRejectedMessagePublisher()),
                                                     storeMetadata,
                                                     new TestLogger<PulsarTableView<SportMessage>>());

        var result = await view.StartBootstrapAsync(CancellationToken.None);

        var recovered = Assert.IsType<TopicRecoveredFromStateStore<SportMessage>>(result);
        Assert.Empty(recovered.DeltaChanges);
        Assert.Equal(2, (await view.GetEntry("sport-1", CancellationToken.None))!.Version);
        Assert.Equal(2, messageStorage.GetById("sport-1")!.Version);
    }

    [Fact]
    public async Task start_bootstrap_async_should_fail_when_apply_returns_updated_without_snapshot_value()
    {
        var topic = "persistent://public/default/sports";
        var storeMetadata = new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: false, CreatedAt: DateTimeOffset.UtcNow);
        var checkpointStorage = new FakeCheckpointStorage();
        var unitOfWorkFactory = new FakeUnitOfWorkFactory(new FakeSportTableViewUnitOfWork(new FakeSportMessageStorage(), checkpointStorage, new FakeRejectedStorage()));
        var readerFactory = new FakeProjectorTopicReaderFactory();
        readerFactory.SeedHighWatermark(topic, 0, new MessageId(1, 2, 0, 0, TopicShard.Partition(topic, 0).PhysicalTopic));
        readerFactory.SeedMessages(topic, 0, CreateMessage(topic, 0, Sport("sport-1", version: 2), new PulsarMessageId(1, 2, 0, 0)));

        var view = new PulsarTableView<SportMessage>(topic,
                                                     readerFactory,
                                                     unitOfWorkFactory,
                                                     new JsonAvroSerializer(),
                                                     new StubSportApplier(new TableMessageApplied<SportMessage>("sport-1", Sport("sport-1", version: 2), TableMessageApplyDecision.Updated())),
                                                     storeMetadata,
                                                     new TestLogger<PulsarTableView<SportMessage>>());

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => view.StartBootstrapAsync(CancellationToken.None));
        Assert.Equal("Snapshot entry 'sport-1' was missing while applying an update.", exception.Message);
    }

    [Fact]
    public async Task start_bootstrap_async_should_fail_when_apply_returns_created_for_existing_snapshot_value()
    {
        var topic = "persistent://public/default/sports";
        var storeMetadata = new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: true, CreatedAt: DateTimeOffset.UtcNow);
        var messageStorage = new FakeSportMessageStorage();
        messageStorage.Seed(Sport("sport-1", version: 1));
        var checkpointStorage = new FakeCheckpointStorage();
        var shard = TopicShard.Partition(topic, 0);
        checkpointStorage.Seed(new TopicCheckpoint(shard.LogicalTopic, shard.PhysicalTopic, shard.PartitionId, shard.IsPartitioned, new PulsarMessageId(1, 1, 0, 0), storeMetadata.StoreGenerationId, DateTimeOffset.UtcNow));
        var unitOfWorkFactory = new FakeUnitOfWorkFactory(new FakeSportTableViewUnitOfWork(messageStorage, checkpointStorage, new FakeRejectedStorage()));
        var readerFactory = new FakeProjectorTopicReaderFactory();
        readerFactory.SeedHighWatermark(topic, 0, new MessageId(1, 2, 0, 0, shard.PhysicalTopic));
        readerFactory.SeedMessages(topic, 0, CreateMessage(topic, 0, Sport("sport-1", version: 2), new PulsarMessageId(1, 2, 0, 0)));

        var view = new PulsarTableView<SportMessage>(topic,
                                                     readerFactory,
                                                     unitOfWorkFactory,
                                                     new JsonAvroSerializer(),
                                                     new StubSportApplier(new TableMessageApplied<SportMessage>("sport-1", Sport("sport-1", version: 2), TableMessageApplyDecision.Created())),
                                                     storeMetadata,
                                                     new TestLogger<PulsarTableView<SportMessage>>());

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => view.StartBootstrapAsync(CancellationToken.None));
        Assert.Equal("Snapshot entry 'sport-1' already existed while applying a create.", exception.Message);
    }

    private static int recoveredDeltaCount(TopicBootstrapResult<SportMessage> result)
        => result is TopicRecoveredFromStateStore<SportMessage> recovered ? recovered.DeltaChanges.Count : 0;

    private static SportMessage Sport(string id, int version)
        => new()
        {
            Id = id,
            Name = $"{id}-name",
            SportType = "sport",
            Provider = "provider",
            EntityCoverage = "covered",
            Version = version
        };

    private static TableViewMessage CreateMessage(string topic, int partitionId, SportMessage message, PulsarMessageId messageId)
        => new(topic,
               partitionId,
               message.Id,
               new ReadOnlySequence<byte>(JsonSerializer.SerializeToUtf8Bytes(message)),
               messageId,
               PhysicalTopicName: partitionId < 0 ? topic : TopicShard.Partition(topic, partitionId).PhysicalTopic,
               IsPartitioned: partitionId >= 0);

    private sealed class StubSportApplier : ITableViewMessageApplier<SportMessage>
    {
        private readonly TableMessageApplyResult<SportMessage> _result;

        public StubSportApplier(TableMessageApplyResult<SportMessage> result)
        {
            _result = result;
        }

        public ValueTask<TableMessageApplyResult<SportMessage>> ApplyAsync(
            TableViewMessage input,
            ProcessPhase processPhase,
            ITableViewUnitOfWork<SportMessage> tableViewUnitOfWork,
            Func<ReadOnlySequence<byte>, SportMessage> deserialize,
            CancellationToken cancellationToken)
            => ValueTask.FromResult(_result);
    }
}
