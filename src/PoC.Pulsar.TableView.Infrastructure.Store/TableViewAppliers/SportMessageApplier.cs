using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Projector;
using PoC.Pulsar.TableView.Domain.Rejected;
using PoC.Pulsar.TableView.Domain.TableView;
using PoC.Pulsar.TableView.Infrastructure.Store.Observability;
using PoC.Pulsar.TableView.Infrastructure.Store.Readers;
using System.Buffers;
using System.Diagnostics;

namespace PoC.Pulsar.TableView.Infrastructure.Store.TableViewAppliers;
public class SportMessageApplier : ITableViewMessageApplier<SportMessage>
{
    private readonly IRejectedMessagePublisher _rejectedMessagePublisher;

    public SportMessageApplier(IRejectedMessagePublisher rejectedMessagePublisher)
    {
        _rejectedMessagePublisher = rejectedMessagePublisher;
    }

    public async ValueTask<TableMessageApplyResult<SportMessage>> ApplyAsync(TableViewMessage input, ProcessPhase processPhase, ITableViewUnitOfWork<SportMessage> tableViewUnitOfWork, Func<ReadOnlySequence<byte>, SportMessage> serialize, CancellationToken cancellationToken)
    {

        using var activity = ProjectorStoreTelemetry.StartActivity("projection.sport.apply",
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
        activity?.SetTag("result", applyResult is TableMessageRejected<SportMessage> ? "rejected" : applyResult is TableMessageNoOp<SportMessage> ? "noop" : "applied");
        return applyResult;
    }

    private async Task<TableMessageApplyResult<SportMessage>> WhenArriveTombStone(ITableViewUnitOfWork<SportMessage> unitOfWork, TableViewMessage input, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(input.Key))
        {
            await unitOfWork.CheckpointStorage.SaveCheckpointAsync(input.Shard, input.BrokerMessageId, cancellationToken);
            return new TableMessageNoOp<SportMessage>(string.Empty, "tombstone_missing_key");
        }

        var deletedValue = await unitOfWork.MessageStorage.TryLoadAsync(input.Key, cancellationToken);
        if (deletedValue is null)
        {
            await unitOfWork.CheckpointStorage.SaveCheckpointAsync(input.Shard, input.BrokerMessageId, cancellationToken);
            return new TableMessageNoOp<SportMessage>(input.Key, "tombstone_missing_entity");
        }

        await unitOfWork.MessageStorage.DeleteAsync(input.Key, cancellationToken);
        await unitOfWork.CheckpointStorage.SaveCheckpointAsync(input.Shard, input.BrokerMessageId, cancellationToken);
        return new TableMessageDeleted<SportMessage>(input.Key, deletedValue);
    }

    private async Task<TableMessageApplyResult<SportMessage>> ApplyWithVersionValidationAsync(ITableViewUnitOfWork<SportMessage> tableViewUnitOfWork, TableViewMessage input, SportMessage message, CancellationToken cancellationToken)
    {
        var validationError = ValidateMessage(message);
        if (validationError is not null)
        {
            await SaveSportAsRejectedAsync(tableViewUnitOfWork, input, message, validationError, cancellationToken);
            await tableViewUnitOfWork.CheckpointStorage.SaveCheckpointAsync(input.Shard, input.BrokerMessageId, cancellationToken);
            return new TableMessageRejected<SportMessage>(message.Id, validationError);
        }

        var decision = await tableViewUnitOfWork.MessageStorage.TryApplyAsync(message, cancellationToken);
        await tableViewUnitOfWork.CheckpointStorage.SaveCheckpointAsync(input.Shard, input.BrokerMessageId, cancellationToken);
        return decision.Kind switch
        {
            TableMessageApplyKind.NoOp => new TableMessageNoOp<SportMessage>(message.Id, decision.Reason ?? "unknown"),
            TableMessageApplyKind.Created => new TableMessageApplied<SportMessage>(message.Id, message, decision),
            TableMessageApplyKind.Updated => new TableMessageApplied<SportMessage>(message.Id, message, decision),
            _ => throw new NotSupportedException($"Unsupported apply decision kind '{decision.Kind}'.")
        };
    }

    private async ValueTask SaveSportAsRejectedAsync(ITableViewUnitOfWork<SportMessage> uow, TableViewMessage originalMesage, SportMessage message, RejectedReason reason, CancellationToken cancellationToken)
    {
        var (rejectedMessage, headers) = CreateSportRejectedWrite(originalMesage, message, reason);
        await SaveRejectedAsync(uow, rejectedMessage, cancellationToken);
        await PublishRejectedAsync(rejectedMessage, headers, cancellationToken);
    }
    private static (Rejected<SportMessage> message, Dictionary<string,string> headers) CreateSportRejectedWrite(TableViewMessage originalMessage, SportMessage? sportMessage, RejectedReason reason)
    {
        Rejected<SportMessage> rejectedMessage = sportMessage is null
            ? RejectedFactory.CreateFromTombStone<SportMessage>(originalMessage, reason)
            : RejectedFactory.CreateFromPayload(sportMessage, originalMessage, reason);

        var correlationId = HeaderOrDefault(originalMessage, "correlation-id", Guid.NewGuid().ToString("D"));
        var causationId = HeaderOrDefault(originalMessage, "message-id", originalMessage.BrokerMessageId.ToString());
        var rejectedMessageId = rejectedMessage.RejectedId.ToString("D");


        var headers = CreateRejectedHeaders(nameof(Rejected<SportMessage>),
                                            "sport-rejected",
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
    private async Task PublishRejectedAsync(Rejected<SportMessage> rejectedMessage, Dictionary<string,string> headers, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        var tags = new[]
        {
            ProjectorStoreTelemetry.StoreTag,
            new KeyValuePair<string, object?>("topic", rejectedMessage.OriginalTopic),
            new KeyValuePair<string, object?>("partition", rejectedMessage.OriginalPartitionId),
            new KeyValuePair<string, object?>("entity_type", headers["event-type"] ?? "na" ),
            new KeyValuePair<string, object?>("message_type", "sports")
        };

        using var activity = ProjectorStoreTelemetry.StartActivity("projection.rejected.publish",
                                                                   rejectedMessage.OriginalTopic,
                                                                   rejectedMessage.OriginalPartitionId,
                                                                   operation: "Publish");

        activity?.SetTag("entity_type", rejectedMessage.OriginalTopic == PulsarTopics.Sports ? PulsarTopics.SportsRejected : PulsarTopics.CategoriesRejected);

        try
        {
            await _rejectedMessagePublisher.PublishAsync(rejectedMessage, headers, cancellationToken);
            KeyValuePair<string, object?>[] successTags = [..tags, new KeyValuePair<string, object?>("result", "success")];
            ProjectorStoreTelemetry.RejectedPublished.Add(1, successTags);
            ProjectorStoreTelemetry.RejectedPublishDuration.Record(stopwatch.Elapsed.TotalMilliseconds, successTags);
            activity?.SetTag("result", "success");
        }
        catch (Exception exception)
        {
            KeyValuePair<string, object?>[] errorTags = [..tags, new KeyValuePair<string, object?>("result", "error")];
            ProjectorStoreTelemetry.RejectedPublishErrors.Add(1, errorTags);
            activity?.SetTag("result", "error");
            activity?.SetStatus(ActivityStatusCode.Error, exception.GetType().Name);
            throw;
        }
    }

    private static async Task SaveRejectedAsync(ITableViewUnitOfWork<SportMessage> uow,
                                                Rejected<SportMessage> input,
                                                CancellationToken cancellationToken)
        => await uow.RejectedStorage.SaveRejectedRecordAsync(rejectedProjection: new RejectedProjection(input.OriginalMessageKey,
                                                                                                        input.OriginalTopic,
                                                                                                        input.OriginalPartitionId,
                                                                                                        input.Reason,
                                                                                                        DateTimeOffset.UtcNow),
                                                              cancellationToken);

    private static RejectedReason? ValidateMessage(SportMessage message)
    {
        if (string.IsNullOrWhiteSpace(message.Id))
        {
            return new RejectedReason("id_empty", "Sport id is required");
        }

        if (string.IsNullOrWhiteSpace(message.Name))
        {
            return new RejectedReason("name_empty", "Sport name is required");
        }

        return message.Version < 0
            ? new RejectedReason("version_negative", "Sport version cannot be negative")
            : null;
    }
}
