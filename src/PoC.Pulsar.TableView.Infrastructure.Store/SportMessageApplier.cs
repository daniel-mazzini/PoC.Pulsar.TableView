using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Entities;
using PoC.Pulsar.TableView.Domain.Storages;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Infrastructure.Store.Observability;
using PoC.Pulsar.TableView.Infrastructure.Store.Publisher;
using PoC.Pulsar.TableView.Infrastructure.Store.Readers;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;

namespace PoC.Pulsar.TableView.Infrastructure.Store;

internal class SportMessageApplier : IProjectorMessageApplier<SportMessage>
{
    private readonly IRejectedMessagePublisher _rejectedMessagePublisher;

    public SportMessageApplier(IRejectedMessagePublisher rejectedMessagePublisher)
    {
        _rejectedMessagePublisher = rejectedMessagePublisher;
    }

    // TableViewMessage input, ProcessPhase processPhase, ITableViewUnitOfWork<TMessage> tableViewUnitOfWork, Func<ReadOnlySequence<byte>, TMessage> serialize,  CancellationToken cancellationToken
    public async ValueTask<ProjectionApplyResult> ApplyAsync(TableViewMessage input, ProcessPhase processPhase, ITableViewUnitOfWork<SportMessage> tableViewUnitOfWork, Func<ReadOnlySequence<byte>, SportMessage> serialize, CancellationToken cancellationToken)
    {

        using var activity = ProjectorStoreTelemetry.StartActivity("projection.sport.apply",
                                                                   input.TopicName,
                                                                   input.PartitionId,
                                                                   phase: processPhase.Name);

        if (input.Data.Length == 0)
        {
            await WhenArriveTombStone(tableViewUnitOfWork, input.Key!, cancellationToken);
            return new ProjectionApplyResult.Applied(input.Key!, true);
        }

        var message = serialize(input.Data);
        activity?.SetTag("entity_type", message.GetType().Name);

        var rejectedMessage = ValidateMessage(message);
        if (rejectedMessage is not null)
        {
            await SaveSportAsRejectedAsync(tableViewUnitOfWork, input, message, rejectedMessage, cancellationToken);
            activity?.SetTag("result", "rejected");
            return new ProjectionApplyResult.Rejected(message.Id, rejectedMessage);
        }

        // Sport type is not checked like pending

        // 
        await tableViewUnitOfWork.MessageStorage.UpsertAsync(message, cancellationToken);
        await tableViewUnitOfWork.CheckpointStorage.SaveCheckpointAsync(input.TopicName, input.PartitionId, input.MessageId, cancellationToken);
        return new ProjectionApplyResult.Applied(message.Id, true);
    }

    private static async Task WhenArriveTombStone(ITableViewUnitOfWork<SportMessage> unitOfWork, string messageKey, CancellationToken cancellationToken)
    {
        var deletedValue = await unitOfWork.MessageStorage.TryLoadAsync(messageKey, cancellationToken);
        if (deletedValue is not null)
        {
            await unitOfWork.MessageStorage.DeleteAsync(messageKey, cancellationToken);
        }
    }

    private async ValueTask SaveSportAsRejectedAsync(ITableViewUnitOfWork<SportMessage> uow, TableViewMessage originalMesage, SportMessage message, string rejectedMessage, CancellationToken cancellationToken)
    {
        await SaveRejectedAsync(uow, originalMesage, message.Id, rejectedMessage, cancellationToken);
        await PublishRejectedAsync(CreateSportRejectedWrite(originalMesage, message, rejectedMessage), cancellationToken);
    }
    private static RejectedMessageWrite CreateSportRejectedWrite(TableViewMessage originalMessage, SportMessage sportMessage, string reason)
    {
        var timestamp = DateTimeOffset.UtcNow;
        var rejectedMessageId = Guid.NewGuid();
        var messageId = rejectedMessageId.ToString("D");
        var correlationId = HeaderOrDefault(originalMessage, "correlation-id", Guid.NewGuid().ToString("D"));
        var causationId = HeaderOrDefault(originalMessage, "message-id", originalMessage.MessageId.ToString());
        var output = new SportRejectedMessage(rejectedMessageId,
                                              originalMessage.TopicName,
                                              originalMessage.Key ?? sportMessage.Id,
                                              originalMessage.MessageId.ToString(),
                                              nameof(SportMessage),
                                              HeaderOrDefault(originalMessage, "event-type", "unknown"),
                                              sportMessage.Id,
                                              reason,
                                              reason,
                                              sportMessage,
                                              timestamp,
                                              correlationId,
                                              causationId,
                                              messageId);

        var headers = CreateRejectedHeaders(nameof(SportRejectedMessage), "sport-rejected", timestamp, correlationId, causationId, messageId, originalMessage);
        return new RejectedMessageWrite(originalMessage.TopicName, originalMessage.PartitionId, originalMessage.Key ?? sportMessage.Id, output, headers, timestamp, nameof(SportRejectedMessage), "sport-rejected");
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
    private async Task PublishRejectedAsync(RejectedMessageWrite rejectedMessageWrite, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        var tags = new[]
        {
            ProjectorStoreTelemetry.StoreTag,
            new KeyValuePair<string, object?>("topic", rejectedMessageWrite.Topic),
            new KeyValuePair<string, object?>("entity_type", rejectedMessageWrite.Topic == PulsarTopics.Sports ? PulsarTopics.Sports : PulsarTopics.Categories),
            new KeyValuePair<string, object?>("message_type", rejectedMessageWrite.MessageType),
            new KeyValuePair<string, object?>("event_type", rejectedMessageWrite.EventType),
            new KeyValuePair<string, object?>("result", "success")
        };

        using var activity = ProjectorStoreTelemetry.StartActivity("projection.rejected.publish",
                                                                   rejectedMessageWrite.Topic,
                                                                   operation: "Publish");

        activity?.SetTag("entity_type", rejectedMessageWrite.Topic == PulsarTopics.SportsRejected ? PulsarTopics.SportsRejected : PulsarTopics.CategoriesRejected);
        activity?.SetTag("message_type", rejectedMessageWrite.MessageType);
        activity?.SetTag("event_type", rejectedMessageWrite.EventType);

        try
        {
            await _rejectedMessagePublisher.PublishAsync(rejectedMessageWrite, cancellationToken);
            ProjectorStoreTelemetry.RejectedPublished.Add(1, tags);
            ProjectorStoreTelemetry.RejectedPublishDuration.Record(stopwatch.Elapsed.TotalMilliseconds, tags);
            activity?.SetTag("result", "success");
        }
        catch (Exception exception)
        {
            ProjectorStoreTelemetry.RejectedPublishErrors.Add(1,
                                                              ProjectorStoreTelemetry.StoreTag,
                                                              new KeyValuePair<string, object?>("topic", rejectedMessageWrite.Topic),
                                                              new KeyValuePair<string, object?>("entity_type", rejectedMessageWrite.Topic == PulsarTopics.Sports ? PulsarTopics.Sports : PulsarTopics.Categories),
                                                              new KeyValuePair<string, object?>("message_type", rejectedMessageWrite.MessageType),
                                                              new KeyValuePair<string, object?>("event_type", rejectedMessageWrite.EventType),
                                                              new KeyValuePair<string, object?>("result", "error"));
            activity?.SetTag("result", "error");
            activity?.SetStatus(ActivityStatusCode.Error, exception.GetType().Name);
            throw;
        }
    }

    private static async Task SaveRejectedAsync(ITableViewUnitOfWork<SportMessage> uow,
                                                TableViewMessage input,
                                                string sportId,
                                                string reason,
                                                CancellationToken cancellationToken)
        => await uow.RejectedStorage.SaveRejectedRecordAsync(rejectedProjection: new RejectedProjection(input.Key ?? sportId,
                                                                                                        input.TopicName,
                                                                                                        input.PartitionId,
                                                                                                        reason,
                                                                                                        DateTimeOffset.UtcNow),
                                                              cancellationToken);

    private static string? ValidateMessage(SportMessage message)
    {
        if (string.IsNullOrWhiteSpace(message.Id))
        {
            return "id_empty";
        }

        if (string.IsNullOrWhiteSpace(message.Name))
        {
            return "name_empty";
        }

        return message.Version < 0 ? "version_negative" : null;
    }
}
