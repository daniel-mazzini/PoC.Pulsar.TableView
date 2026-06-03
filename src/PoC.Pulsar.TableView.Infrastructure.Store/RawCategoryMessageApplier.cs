using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Projector;
using PoC.Pulsar.TableView.Domain.Rejected;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Domain.TableView;
using PoC.Pulsar.TableView.Infrastructure.Store.Observability;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;

namespace PoC.Pulsar.TableView.Infrastructure.Store;

public sealed class RawCategoryMessageApplier : ITableViewMessageApplier<RawCategoryMessage>
{
    private readonly IRejectedMessagePublisher _rejectedMessagePublisher;

    public RawCategoryMessageApplier(IRejectedMessagePublisher rejectedMessagePublisher)
    {
        _rejectedMessagePublisher = rejectedMessagePublisher;
    }

    public async ValueTask<TableMessageApplyResult<RawCategoryMessage>> ApplyAsync(TableViewMessage input,
                                                                                   ProcessPhase processPhase,
                                                                                   ITableViewUnitOfWork<RawCategoryMessage> tableViewUnitOfWork,
                                                                                   Func<ReadOnlySequence<byte>, RawCategoryMessage> serialize,
                                                                                   CancellationToken cancellationToken)
    {
        using var activity = ProjectorStoreTelemetry.StartActivity("projection.category.apply",
                                                                   input.TopicName,
                                                                   input.PartitionId,
                                                                   phase: processPhase.Name);

        if (input.Data.Length == 0)
        {
            return await WhenArriveTombStone(tableViewUnitOfWork, input, cancellationToken);
        }

        var message = serialize(input.Data);
        activity?.SetTag("entity_type", message.GetType().Name);

        var applyResult = await ApplyWithVersionValidationAsync(tableViewUnitOfWork, input, message, cancellationToken);
        activity?.SetTag("result", applyResult is TableMessageRejected<RawCategoryMessage> ? "rejected" : applyResult is TableMessageNoOp<RawCategoryMessage> ? "noop" : "applied");
        return applyResult;
    }

    private async Task<TableMessageApplyResult<RawCategoryMessage>> WhenArriveTombStone(ITableViewUnitOfWork<RawCategoryMessage> unitOfWork,
                                                                                         TableViewMessage input,
                                                                                         CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(input.Key))
        {
            await unitOfWork.CheckpointStorage.SaveCheckpointAsync(input.TopicName, input.PartitionId, input.BrokerMessageId, cancellationToken);
            return new TableMessageNoOp<RawCategoryMessage>(string.Empty, "tombstone_missing_key");
        }

        var deletedValue = await unitOfWork.MessageStorage.TryLoadAsync(input.Key, cancellationToken);
        if (deletedValue is null)
        {
            await unitOfWork.CheckpointStorage.SaveCheckpointAsync(input.TopicName, input.PartitionId, input.BrokerMessageId, cancellationToken);
            return new TableMessageNoOp<RawCategoryMessage>(input.Key, "tombstone_missing_entity");
        }

        await unitOfWork.MessageStorage.DeleteAsync(input.Key, cancellationToken);
        await unitOfWork.CheckpointStorage.SaveCheckpointAsync(input.TopicName, input.PartitionId, input.BrokerMessageId, cancellationToken);
        return new TableMessageApplied<RawCategoryMessage>(new EventDeleted<RawCategoryMessage>(input.Key, deletedValue));
    }

    private async Task<TableMessageApplyResult<RawCategoryMessage>> ApplyWithVersionValidationAsync(ITableViewUnitOfWork<RawCategoryMessage> tableViewUnitOfWork,
                                                                                                   TableViewMessage input,
                                                                                                   RawCategoryMessage message,
                                                                                                   CancellationToken cancellationToken)
    {
        var validationError = await ValidateMessageAsync(tableViewUnitOfWork, message, cancellationToken);
        if (validationError is not null)
        {
            await SaveCategoryAsRejectedAsync(tableViewUnitOfWork, input, message, validationError, cancellationToken);
            await tableViewUnitOfWork.CheckpointStorage.SaveCheckpointAsync(input.TopicName, input.PartitionId, input.BrokerMessageId, cancellationToken);
            return new TableMessageRejected<RawCategoryMessage>(message.Id, validationError);
        }

        var current = await tableViewUnitOfWork.MessageStorage.TryLoadAsync(message.Id, cancellationToken);
        if (current is not null && message.Version <= current.Version)
        {
            await tableViewUnitOfWork.CheckpointStorage.SaveCheckpointAsync(input.TopicName, input.PartitionId, input.BrokerMessageId, cancellationToken);
            return new TableMessageNoOp<RawCategoryMessage>(message.Id, "incoming_version_not_greater_than_current");
        }

        if (current is null)
        {
            await tableViewUnitOfWork.MessageStorage.UpsertAsync(message, cancellationToken);
            await tableViewUnitOfWork.CheckpointStorage.SaveCheckpointAsync(input.TopicName, input.PartitionId, input.BrokerMessageId, cancellationToken);
            return new TableMessageApplied<RawCategoryMessage>(new TableEntryCreated<RawCategoryMessage>(message.Id, message));
        }

        await tableViewUnitOfWork.MessageStorage.UpsertAsync(message, cancellationToken);
        await tableViewUnitOfWork.CheckpointStorage.SaveCheckpointAsync(input.TopicName, input.PartitionId, input.BrokerMessageId, cancellationToken);
        return new TableMessageApplied<RawCategoryMessage>(new TableEntryUpdated<RawCategoryMessage>(message.Id, message, current));
    }

    private async ValueTask<RejectedReason?> ValidateMessageAsync(ITableViewUnitOfWork<RawCategoryMessage> unitOfWork,
                                                                  RawCategoryMessage message,
                                                                  CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(message.Id))
        {
            return new RejectedReason("id_empty", "Category id is required");
        }

        if (string.IsNullOrWhiteSpace(message.Name))
        {
            return new RejectedReason("name_empty", "Category name is required");
        }

        if (string.IsNullOrWhiteSpace(message.SportId))
        {
            return new RejectedReason("sport_id_empty", "Category sport id is required");
        }

        if (message.Version < 0)
        {
            return new RejectedReason("version_negative", "Category version cannot be negative");
        }

        if (string.IsNullOrWhiteSpace(message.ParentId))
        {
            return null;
        }

        var parent = await unitOfWork.MessageStorage.TryLoadAsync(message.ParentId, cancellationToken);
        if (parent is null)
        {
            return null;
        }

        return parent.SportId == message.SportId
            ? null
            : new RejectedReason("parent_sport_mismatch", "Category parent must belong to the same sport");
    }

    private async ValueTask SaveCategoryAsRejectedAsync(ITableViewUnitOfWork<RawCategoryMessage> uow,
                                                         TableViewMessage originalMessage,
                                                         RawCategoryMessage message,
                                                         RejectedReason reason,
                                                         CancellationToken cancellationToken)
    {
        var (rejectedMessage, headers) = CreateCategoryRejectedWrite(originalMessage, message, reason);
        await SaveRejectedAsync(uow, rejectedMessage, cancellationToken);
        await PublishRejectedAsync(rejectedMessage, headers, cancellationToken);
    }

    private static (RejectedMessage<RawCategoryMessage> message, Dictionary<string, string> headers) CreateCategoryRejectedWrite(TableViewMessage originalMessage,
                                                                                                                                 RawCategoryMessage? categoryMessage,
                                                                                                                                 RejectedReason reason)
    {
        RejectedMessage<RawCategoryMessage> rejectedMessage = categoryMessage is null
            ? RejectedFactory.CreateFromTombStone<RawCategoryMessage>(originalMessage, reason)
            : RejectedFactory.CreateFromPayload(categoryMessage, originalMessage, reason);

        var correlationId = HeaderOrDefault(originalMessage, "correlation-id", Guid.NewGuid().ToString("D"));
        var causationId = HeaderOrDefault(originalMessage, "message-id", originalMessage.BrokerMessageId.ToString());
        var rejectedMessageId = rejectedMessage.RejectedId.ToString("D");

        var headers = CreateRejectedHeaders(nameof(RejectedMessage<RawCategoryMessage>),
                                            "category-rejected",
                                            rejectedMessage.RejectedAt,
                                            correlationId,
                                            causationId,
                                            rejectedMessageId,
                                            originalMessage);

        return (rejectedMessage, headers);
    }

    private static Dictionary<string, string> CreateRejectedHeaders(string type,
                                                                     string eventType,
                                                                     DateTimeOffset timestamp,
                                                                     string correlationId,
                                                                     string causationId,
                                                                     string messageId,
                                                                     TableViewMessage input)
    {
        var headers = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["type"] = type,
            ["event-type"] = eventType,
            ["timestamp"] = timestamp.ToString("O"),
            ["correlation-id"] = correlationId,
            ["causation-id"] = causationId,
            ["message-id"] = messageId
        };

        CopyHeaderIfPresent(input, headers, "traceparent");
        CopyHeaderIfPresent(input, headers, "tracestate");
        return headers;
    }

    private static string HeaderOrDefault(TableViewMessage message, string name, string fallback)
        => message.Headers.TryGetValue(name, out var value) && !string.IsNullOrWhiteSpace(value)
            ? value
            : fallback;

    private static void CopyHeaderIfPresent(TableViewMessage input, IDictionary<string, string> output, string name)
    {
        if (input.Headers.TryGetValue(name, out var value) && !string.IsNullOrWhiteSpace(value))
        {
            output[name] = value;
        }
    }

    private async Task PublishRejectedAsync(RejectedMessage<RawCategoryMessage> rejectedMessage, Dictionary<string, string> headers, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        var tags = new[]
        {
            ProjectorStoreTelemetry.StoreTag,
            new KeyValuePair<string, object?>("topic", rejectedMessage.OriginalTopic),
            new KeyValuePair<string, object?>("partition", rejectedMessage.OriginalPartitionId),
            new KeyValuePair<string, object?>("entity_type", "categories"),
            new KeyValuePair<string, object?>("message_type", "categories"),
            new KeyValuePair<string, object?>("result", "success")
        };

        using var activity = ProjectorStoreTelemetry.StartActivity("projection.rejected.publish",
                                                                   rejectedMessage.OriginalTopic,
                                                                   rejectedMessage.OriginalPartitionId,
                                                                   operation: "Publish");

        activity?.SetTag("entity_type", "categories-rejected");
        activity?.SetTag("message_type", nameof(RejectedMessage<RawCategoryMessage>));
        activity?.SetTag("event_type", "category-rejected");

        try
        {
            await _rejectedMessagePublisher.PublishAsync(rejectedMessage, headers, cancellationToken);
            ProjectorStoreTelemetry.RejectedPublished.Add(1, tags);
            ProjectorStoreTelemetry.RejectedPublishDuration.Record(stopwatch.Elapsed.TotalMilliseconds, tags);
            activity?.SetTag("result", "success");
        }
        catch (Exception exception)
        {
            ProjectorStoreTelemetry.RejectedPublishErrors.Add(1,
                                                              ProjectorStoreTelemetry.StoreTag,
                                                              new KeyValuePair<string, object?>("topic", rejectedMessage.OriginalTopic),
                                                              new KeyValuePair<string, object?>("entity_type", "categories"),
                                                              new KeyValuePair<string, object?>("message_type", headers["type"]),
                                                              new KeyValuePair<string, object?>("event_type", headers["event-type"]),
                                                              new KeyValuePair<string, object?>("result", "error"));
            activity?.SetTag("result", "error");
            activity?.SetStatus(ActivityStatusCode.Error, exception.GetType().Name);
            throw;
        }
    }

    private static async Task SaveRejectedAsync(ITableViewUnitOfWork<RawCategoryMessage> uow,
                                                RejectedMessage<RawCategoryMessage> input,
                                                CancellationToken cancellationToken)
        => await uow.RejectedStorage.SaveRejectedRecordAsync(rejectedProjection: new RejectedProjection(input.OriginalMessageKey,
                                                                                                        input.OriginalTopic,
                                                                                                        input.OriginalPartitionId,
                                                                                                        new RejectedReason(input.Reason.ReasonCode, input.Reason.Reason),
                                                                                                        DateTimeOffset.UtcNow),
                                                              cancellationToken);
}
