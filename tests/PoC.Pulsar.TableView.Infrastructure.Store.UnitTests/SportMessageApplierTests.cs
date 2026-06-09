using System.Buffers;
using System.Text.Json;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Projector;
using PoC.Pulsar.TableView.Domain.TableView;
using PoC.Pulsar.TableView.Infrastructure.Store.TableViewAppliers;
using Xunit;

namespace PoC.Pulsar.TableView.Infrastructure.Store.UnitTests;

public sealed class SportMessageApplierTests
{
    [Fact]
    public async Task apply_async_should_apply_when_entity_does_not_exist()
    {
        var messageStorage = expect_message_storage_sport_by_id_return_null();
        var checkpointStorage = new FakeCheckpointStorage();
        var rejectedStorage = new FakeRejectedStorage();
        var unitOfWork = new FakeSportTableViewUnitOfWork(messageStorage, checkpointStorage, rejectedStorage);
        var publisher = new FakeRejectedMessagePublisher();
        var applier = new SportMessageApplier(publisher);
        var input = CreateInput(Sport("sport-1", version: 2), new PulsarMessageId(1, 1, 0, 0));

        var result = await applier.ApplyAsync(input, ProcessPhase.Bootstrap, unitOfWork, DeserializeSportMessage, CancellationToken.None);

        var applied = Assert.IsType<TableMessageApplied<SportMessage>>(result);
        Assert.Equal("sport-1", applied.EntityId);
        Assert.Equal(2, applied.NewValue.Version);
        Assert.Equal(TableMessageApplyKind.Created, applied.Decision.Kind);
        Assert.Equal(1, messageStorage.TryApplyCallCount);
        Assert.Single(checkpointStorage.SavedCheckpoints);
        Assert.Empty(publisher.PublishedMessages);
        Assert.Null(rejectedStorage.LastSaved);
    }

    [Fact]
    public async Task apply_async_should_apply_when_incoming_version_is_greater_than_current_version()
    {
        var messageStorage = expect_message_storage_sport_by_id_return_existing_sport(Sport("sport-1", version: 1));
        var checkpointStorage = new FakeCheckpointStorage();
        var rejectedStorage = new FakeRejectedStorage();
        var unitOfWork = new FakeSportTableViewUnitOfWork(messageStorage, checkpointStorage, rejectedStorage);
        var publisher = new FakeRejectedMessagePublisher();
        var applier = new SportMessageApplier(publisher);
        var input = CreateInput(Sport("sport-1", version: 2), new PulsarMessageId(1, 2, 0, 0));

        var result = await applier.ApplyAsync(input, ProcessPhase.Bootstrap, unitOfWork, DeserializeSportMessage, CancellationToken.None);

        var applied = Assert.IsType<TableMessageApplied<SportMessage>>(result);
        Assert.Equal("sport-1", applied.EntityId);
        Assert.Equal(2, applied.NewValue.Version);
        Assert.Equal(TableMessageApplyKind.Updated, applied.Decision.Kind);
        Assert.Equal(2, messageStorage.GetById("sport-1")!.Version);
        Assert.Equal(1, messageStorage.TryApplyCallCount);
        Assert.Single(checkpointStorage.SavedCheckpoints);
    }

    [Fact]
    public async Task apply_async_should_return_noop_when_incoming_version_is_lower_than_current_version()
    {
        var messageStorage = expect_message_storage_sport_by_id_return_existing_sport(Sport("sport-1", version: 5));
        var checkpointStorage = new FakeCheckpointStorage();
        var rejectedStorage = new FakeRejectedStorage();
        var unitOfWork = new FakeSportTableViewUnitOfWork(messageStorage, checkpointStorage, rejectedStorage);
        var publisher = new FakeRejectedMessagePublisher();
        var applier = new SportMessageApplier(publisher);
        var input = CreateInput(Sport("sport-1", version: 3), new PulsarMessageId(1, 3, 0, 0));

        var result = await applier.ApplyAsync(input, ProcessPhase.Bootstrap, unitOfWork, DeserializeSportMessage, CancellationToken.None);

        var noOp = Assert.IsType<TableMessageNoOp<SportMessage>>(result);
        Assert.Equal("incoming_version_not_greater_than_current", noOp.Reason);
        Assert.Equal(5, messageStorage.GetById("sport-1")!.Version);
        Assert.Equal(1, messageStorage.TryApplyCallCount);
        Assert.Single(checkpointStorage.SavedCheckpoints);
        Assert.Empty(publisher.PublishedMessages);
        Assert.Null(rejectedStorage.LastSaved);
    }

    [Fact]
    public async Task apply_async_should_return_noop_when_incoming_version_is_equal_to_current_version()
    {
        var messageStorage = expect_message_storage_sport_by_id_return_existing_sport(Sport("sport-1", version: 5));
        var checkpointStorage = new FakeCheckpointStorage();
        var rejectedStorage = new FakeRejectedStorage();
        var unitOfWork = new FakeSportTableViewUnitOfWork(messageStorage, checkpointStorage, rejectedStorage);
        var publisher = new FakeRejectedMessagePublisher();
        var applier = new SportMessageApplier(publisher);
        var input = CreateInput(Sport("sport-1", version: 5), new PulsarMessageId(1, 4, 0, 0));

        var result = await applier.ApplyAsync(input, ProcessPhase.Bootstrap, unitOfWork, DeserializeSportMessage, CancellationToken.None);

        var noOp = Assert.IsType<TableMessageNoOp<SportMessage>>(result);
        Assert.Equal("incoming_version_not_greater_than_current", noOp.Reason);
        Assert.Equal(5, messageStorage.GetById("sport-1")!.Version);
        Assert.Equal(1, messageStorage.TryApplyCallCount);
        Assert.Single(checkpointStorage.SavedCheckpoints);
    }

    [Fact]
    public async Task apply_async_should_return_rejected_when_version_is_negative()
    {
        var messageStorage = expect_message_storage_sport_by_id_return_null();
        var checkpointStorage = new FakeCheckpointStorage();
        var rejectedStorage = new FakeRejectedStorage();
        var unitOfWork = new FakeSportTableViewUnitOfWork(messageStorage, checkpointStorage, rejectedStorage);
        var publisher = new FakeRejectedMessagePublisher();
        var applier = new SportMessageApplier(publisher);
        var input = CreateInput(Sport("sport-1", version: -1), new PulsarMessageId(1, 5, 0, 0));

        var result = await applier.ApplyAsync(input, ProcessPhase.Bootstrap, unitOfWork, DeserializeSportMessage, CancellationToken.None);

        var rejected = Assert.IsType<TableMessageRejected<SportMessage>>(result);
        Assert.Equal("sport-1", rejected.EntityId);
        Assert.Equal("version_negative", rejected.Reason.Code);
        Assert.Single(publisher.PublishedMessages);
        Assert.NotNull(rejectedStorage.LastSaved);
        Assert.Equal("version_negative", rejectedStorage.LastSaved!.Reason.Code);
        Assert.Single(checkpointStorage.SavedCheckpoints);
        Assert.Null(messageStorage.GetById("sport-1"));
    }

    [Fact]
    public async Task apply_async_should_delete_when_tombstone_has_existing_entity()
    {
        var messageStorage = expect_message_storage_sport_by_id_return_existing_sport(Sport("sport-1", version: 7));
        var checkpointStorage = new FakeCheckpointStorage();
        var rejectedStorage = new FakeRejectedStorage();
        var unitOfWork = new FakeSportTableViewUnitOfWork(messageStorage, checkpointStorage, rejectedStorage);
        var publisher = new FakeRejectedMessagePublisher();
        var applier = new SportMessageApplier(publisher);
        var input = new TableViewMessage("persistent://public/default/sports", 0, "sport-1", ReadOnlySequence<byte>.Empty, new PulsarMessageId(1, 6, 0, 0));

        var result = await applier.ApplyAsync(input, ProcessPhase.Bootstrap, unitOfWork, DeserializeSportMessage, CancellationToken.None);

        var deleted = Assert.IsType<TableMessageDeleted<SportMessage>>(result);
        Assert.Equal("sport-1", deleted.EntityId);
        Assert.Equal(7, deleted.CurrentValue.Version);
        Assert.Equal(1, messageStorage.DeleteCallCount);
        Assert.Null(messageStorage.GetById("sport-1"));
        Assert.Single(checkpointStorage.SavedCheckpoints);
        Assert.Empty(publisher.PublishedMessages);
    }

    [Fact]
    public async Task apply_async_should_not_save_checkpoint_when_try_apply_fails()
    {
        var messageStorage = expect_message_storage_sport_by_id_return_null();
        messageStorage.ThrowOnTryApply = true;
        var checkpointStorage = new FakeCheckpointStorage();
        var rejectedStorage = new FakeRejectedStorage();
        var unitOfWork = new FakeSportTableViewUnitOfWork(messageStorage, checkpointStorage, rejectedStorage);
        var publisher = new FakeRejectedMessagePublisher();
        var applier = new SportMessageApplier(publisher);
        var input = CreateInput(Sport("sport-1", version: 2), new PulsarMessageId(1, 10, 0, 0));

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await applier.ApplyAsync(input, ProcessPhase.Bootstrap, unitOfWork, DeserializeSportMessage, CancellationToken.None));

        Assert.Empty(checkpointStorage.SavedCheckpoints);
    }

    [Fact]
    public async Task apply_async_should_return_noop_when_tombstone_key_is_missing()
    {
        var messageStorage = expect_message_storage_sport_by_id_return_null();
        var checkpointStorage = new FakeCheckpointStorage();
        var rejectedStorage = new FakeRejectedStorage();
        var unitOfWork = new FakeSportTableViewUnitOfWork(messageStorage, checkpointStorage, rejectedStorage);
        var publisher = new FakeRejectedMessagePublisher();
        var applier = new SportMessageApplier(publisher);
        var input = new TableViewMessage("persistent://public/default/sports", 0, null, ReadOnlySequence<byte>.Empty, new PulsarMessageId(1, 7, 0, 0));

        var result = await applier.ApplyAsync(input, ProcessPhase.Bootstrap, unitOfWork, DeserializeSportMessage, CancellationToken.None);

        var noOp = Assert.IsType<TableMessageNoOp<SportMessage>>(result);
        Assert.Equal("tombstone_missing_key", noOp.Reason);
        Assert.Single(checkpointStorage.SavedCheckpoints);
        Assert.Empty(publisher.PublishedMessages);
        Assert.Null(rejectedStorage.LastSaved);
    }

    private static FakeSportMessageStorage expect_message_storage_sport_by_id_return_null()
        => new();

    private static FakeSportMessageStorage expect_message_storage_sport_by_id_return_existing_sport(SportMessage existingSport)
    {
        var storage = new FakeSportMessageStorage();
        storage.Seed(existingSport);
        return storage;
    }

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

    private static TableViewMessage CreateInput(SportMessage message, PulsarMessageId messageId)
        => new("persistent://public/default/sports",
               0,
               message.Id,
               new ReadOnlySequence<byte>(JsonSerializer.SerializeToUtf8Bytes(message)),
               messageId);

    private static SportMessage DeserializeSportMessage(ReadOnlySequence<byte> data)
        => JsonSerializer.Deserialize<SportMessage>(data.ToArray())!;
}
