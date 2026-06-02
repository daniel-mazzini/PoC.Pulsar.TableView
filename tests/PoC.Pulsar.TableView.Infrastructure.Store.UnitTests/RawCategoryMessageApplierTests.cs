using System.Buffers;
using System.Text.Json;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Projector;
using PoC.Pulsar.TableView.Domain.TableView;
using Xunit;

namespace PoC.Pulsar.TableView.Infrastructure.Store.UnitTests;

public sealed class RawCategoryMessageApplierTests
{
    [Fact]
    public async Task apply_async_should_apply_when_parent_is_missing()
    {
        var messageStorage = new FakeRawCategoryMessageStorage();
        var checkpointStorage = new FakeCheckpointStorage();
        var unitOfWork = new FakeCategoryTableViewUnitOfWork(messageStorage, checkpointStorage, new FakeRejectedStorage());
        var publisher = new FakeRejectedMessagePublisher();
        var applier = new RawCategoryMessageApplier(publisher);
        var input = CreateInput(Category("category-1", "sport-1", parentId: "parent-missing"), new PulsarMessageId(1, 1, 0, 0));

        var result = await applier.ApplyAsync(input, ProcessPhase.Bootstrap, unitOfWork, DeserializeCategoryMessage, CancellationToken.None);

        var applied = Assert.IsType<TableMessageApplied<RawCategoryMessage>>(result);
        var created = Assert.IsType<TableEntryCreated<RawCategoryMessage>>(applied.Change);
        Assert.Equal("category-1", created.Key);
        Assert.Equal("parent-missing", created.NewValue.ParentId);
        Assert.Equal(1, messageStorage.UpsertCallCount);
        Assert.Single(checkpointStorage.SavedCheckpoints);
        Assert.Empty(publisher.PublishedMessages);
    }

    [Fact]
    public async Task apply_async_should_apply_when_parent_exists_with_same_sport_id()
    {
        var parent = Category("parent-1", "sport-1", parentId: null);
        var messageStorage = new FakeRawCategoryMessageStorage();
        messageStorage.Seed(parent);
        var unitOfWork = new FakeCategoryTableViewUnitOfWork(messageStorage, new FakeCheckpointStorage(), new FakeRejectedStorage());
        var publisher = new FakeRejectedMessagePublisher();
        var applier = new RawCategoryMessageApplier(publisher);
        var input = CreateInput(Category("category-1", "sport-1", parentId: "parent-1"), new PulsarMessageId(1, 2, 0, 0));

        var result = await applier.ApplyAsync(input, ProcessPhase.Bootstrap, unitOfWork, DeserializeCategoryMessage, CancellationToken.None);

        var applied = Assert.IsType<TableMessageApplied<RawCategoryMessage>>(result);
        Assert.IsType<TableEntryCreated<RawCategoryMessage>>(applied.Change);
        Assert.Equal(1, messageStorage.UpsertCallCount);
        Assert.Empty(publisher.PublishedMessages);
    }

    [Fact]
    public async Task apply_async_should_return_rejected_when_parent_exists_with_different_sport_id()
    {
        var parent = Category("parent-1", "sport-2", parentId: null);
        var messageStorage = new FakeRawCategoryMessageStorage();
        messageStorage.Seed(parent);
        var checkpointStorage = new FakeCheckpointStorage();
        var rejectedStorage = new FakeRejectedStorage();
        var unitOfWork = new FakeCategoryTableViewUnitOfWork(messageStorage, checkpointStorage, rejectedStorage);
        var publisher = new FakeRejectedMessagePublisher();
        var applier = new RawCategoryMessageApplier(publisher);
        var input = CreateInput(Category("category-1", "sport-1", parentId: "parent-1"), new PulsarMessageId(1, 3, 0, 0));

        var result = await applier.ApplyAsync(input, ProcessPhase.Bootstrap, unitOfWork, DeserializeCategoryMessage, CancellationToken.None);

        var rejected = Assert.IsType<TableMessageRejected<RawCategoryMessage>>(result);
        Assert.Equal("category-1", rejected.EntityId);
        Assert.Equal("parent_sport_mismatch", rejected.Reason.Code);
        Assert.Single(publisher.PublishedMessages);
        Assert.NotNull(rejectedStorage.LastSaved);
        Assert.Single(checkpointStorage.SavedCheckpoints);
        Assert.Equal(0, messageStorage.UpsertCallCount);
    }

    [Fact]
    public async Task apply_async_should_return_rejected_when_id_is_missing()
    {
        var unitOfWork = new FakeCategoryTableViewUnitOfWork(new FakeRawCategoryMessageStorage(), new FakeCheckpointStorage(), new FakeRejectedStorage());
        var publisher = new FakeRejectedMessagePublisher();
        var applier = new RawCategoryMessageApplier(publisher);
        var input = CreateInput(Category(null, "sport-1", parentId: null), new PulsarMessageId(1, 4, 0, 0));

        var result = await applier.ApplyAsync(input, ProcessPhase.Bootstrap, unitOfWork, DeserializeCategoryMessage, CancellationToken.None);

        var rejected = Assert.IsType<TableMessageRejected<RawCategoryMessage>>(result);
        Assert.Equal("id_empty", rejected.Reason.Code);
    }

    [Fact]
    public async Task apply_async_should_return_rejected_when_name_is_missing()
    {
        var unitOfWork = new FakeCategoryTableViewUnitOfWork(new FakeRawCategoryMessageStorage(), new FakeCheckpointStorage(), new FakeRejectedStorage());
        var publisher = new FakeRejectedMessagePublisher();
        var applier = new RawCategoryMessageApplier(publisher);
        var input = CreateInput(Category("category-1", "sport-1", parentId: null, name: string.Empty), new PulsarMessageId(1, 5, 0, 0));

        var result = await applier.ApplyAsync(input, ProcessPhase.Bootstrap, unitOfWork, DeserializeCategoryMessage, CancellationToken.None);

        var rejected = Assert.IsType<TableMessageRejected<RawCategoryMessage>>(result);
        Assert.Equal("name_empty", rejected.Reason.Code);
    }

    [Fact]
    public async Task apply_async_should_return_rejected_when_sport_id_is_missing()
    {
        var unitOfWork = new FakeCategoryTableViewUnitOfWork(new FakeRawCategoryMessageStorage(), new FakeCheckpointStorage(), new FakeRejectedStorage());
        var publisher = new FakeRejectedMessagePublisher();
        var applier = new RawCategoryMessageApplier(publisher);
        var input = CreateInput(Category("category-1", null, parentId: null), new PulsarMessageId(1, 6, 0, 0));

        var result = await applier.ApplyAsync(input, ProcessPhase.Bootstrap, unitOfWork, DeserializeCategoryMessage, CancellationToken.None);

        var rejected = Assert.IsType<TableMessageRejected<RawCategoryMessage>>(result);
        Assert.Equal("sport_id_empty", rejected.Reason.Code);
    }

    [Fact]
    public async Task apply_async_should_return_rejected_when_version_is_negative()
    {
        var unitOfWork = new FakeCategoryTableViewUnitOfWork(new FakeRawCategoryMessageStorage(), new FakeCheckpointStorage(), new FakeRejectedStorage());
        var publisher = new FakeRejectedMessagePublisher();
        var applier = new RawCategoryMessageApplier(publisher);
        var input = CreateInput(Category("category-1", "sport-1", parentId: null, version: -1), new PulsarMessageId(1, 7, 0, 0));

        var result = await applier.ApplyAsync(input, ProcessPhase.Bootstrap, unitOfWork, DeserializeCategoryMessage, CancellationToken.None);

        var rejected = Assert.IsType<TableMessageRejected<RawCategoryMessage>>(result);
        Assert.Equal("version_negative", rejected.Reason.Code);
    }

    [Fact]
    public async Task apply_async_should_delete_when_tombstone_has_existing_entity()
    {
        var existing = Category("category-1", "sport-1", parentId: null);
        var messageStorage = new FakeRawCategoryMessageStorage();
        messageStorage.Seed(existing);
        var unitOfWork = new FakeCategoryTableViewUnitOfWork(messageStorage, new FakeCheckpointStorage(), new FakeRejectedStorage());
        var publisher = new FakeRejectedMessagePublisher();
        var applier = new RawCategoryMessageApplier(publisher);
        var input = new TableViewMessage("persistent://public/default/categories", 0, "category-1", ReadOnlySequence<byte>.Empty, new PulsarMessageId(1, 8, 0, 0));

        var result = await applier.ApplyAsync(input, ProcessPhase.Bootstrap, unitOfWork, DeserializeCategoryMessage, CancellationToken.None);

        var applied = Assert.IsType<TableMessageApplied<RawCategoryMessage>>(result);
        Assert.IsType<EventDeleted<RawCategoryMessage>>(applied.Change);
        Assert.Equal(1, messageStorage.DeleteCallCount);
    }

    [Fact]
    public async Task apply_async_should_return_noop_when_tombstone_key_is_missing()
    {
        var unitOfWork = new FakeCategoryTableViewUnitOfWork(new FakeRawCategoryMessageStorage(), new FakeCheckpointStorage(), new FakeRejectedStorage());
        var publisher = new FakeRejectedMessagePublisher();
        var applier = new RawCategoryMessageApplier(publisher);
        var input = new TableViewMessage("persistent://public/default/categories", 0, null, ReadOnlySequence<byte>.Empty, new PulsarMessageId(1, 9, 0, 0));

        var result = await applier.ApplyAsync(input, ProcessPhase.Bootstrap, unitOfWork, DeserializeCategoryMessage, CancellationToken.None);

        var noOp = Assert.IsType<TableMessageNoOp<RawCategoryMessage>>(result);
        Assert.Equal("tombstone_missing_key", noOp.Reason);
    }

    private static RawCategoryMessage Category(string? id, string? sportId, string? parentId, string? name = null, int version = 1)
        => new()
        {
            Id = id ?? string.Empty,
            Name = name ?? (id ?? string.Empty),
            SportId = sportId ?? string.Empty,
            ParentId = parentId,
            Provider = "provider",
            EntityCoverage = "covered",
            Version = version
        };

    private static TableViewMessage CreateInput(RawCategoryMessage message, PulsarMessageId messageId)
        => new("persistent://public/default/categories",
               0,
               string.IsNullOrWhiteSpace(message.Id) ? null : message.Id,
               new ReadOnlySequence<byte>(JsonSerializer.SerializeToUtf8Bytes(message)),
               messageId);

    private static RawCategoryMessage DeserializeCategoryMessage(ReadOnlySequence<byte> data)
        => JsonSerializer.Deserialize<RawCategoryMessage>(data.ToArray())!;
}
