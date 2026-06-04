using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Microsoft.IO;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Rejected;
using PoC.Pulsar.TableView.Domain.Serializers;
using PoC.Pulsar.TableView.Infrastructure.Store.Observability;
using PoC.Pulsar.TableView.Infrastructure.Store.Readers;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Publisher;

[ExcludeFromCodeCoverage(Justification = "Integration adapter around real DotPulsar producers.")]
public sealed class DotPulsarRejectedMessagePublisher : IRejectedMessagePublisher, IAsyncDisposable
{
    private const int Memory1K = 1024;

    private readonly IProducer<ReadOnlySequence<byte>> _sportRejectedProducer;
    private readonly IProducer<ReadOnlySequence<byte>> _categoryRejectedProducer;
    private readonly IAvroSerializer _avroSerializer;

    public DotPulsarRejectedMessagePublisher(IPulsarClient client, string topicNamespace, IAvroSerializer avroSerializer)
    {
        _sportRejectedProducer = client.NewProducer(Schema.ByteSequence)
            .Topic(PulsarTopics.Qualify(topicNamespace, PulsarTopics.SportsRejected))
            .Create();
        _categoryRejectedProducer = client.NewProducer(Schema.ByteSequence)
            .Topic(PulsarTopics.Qualify(topicNamespace, PulsarTopics.CategoriesRejected))
            .Create();
        _avroSerializer = avroSerializer;
    }

    public async Task PublishAsync<TMessage>(Rejected<TMessage> rejected, Dictionary<string, string> headers, CancellationToken cancellationToken)
    {
        using var activity = ProjectorStoreTelemetry.StartActivity("projection.rejected.publish",
                                                                    PulsarTopics.CountryTaxonomyViews,
                                                                    operation: "Publish");
        activity?.SetTag("message_type", nameof(Rejected<TMessage>));

        var metadata = new MessageMetadata
        {
            Key = rejected.OriginalMessageKey,
            EventTimeAsDateTimeOffset = rejected.RejectedAt.ToUniversalTime(),
            DeliverAtTimeAsDateTimeOffset = DateTimeOffset.UtcNow
        };
        foreach (var h in headers) metadata[h.Key] = h.Value;

        if (rejected is Rejected<SportMessage> sportRejected)
        {
            activity?.SetTag("event_type", "sport-rejected");
            await SendOne(_sportRejectedProducer, ToMessage(sportRejected), activity, metadata, cancellationToken);
            return;
        }

        if (rejected is Rejected<RawCategoryMessage> categoryRejected)
        {
            activity?.SetTag("event_type", "category-rejected");
            await SendOne(_categoryRejectedProducer, ToMessage(categoryRejected), activity, metadata, cancellationToken);
            return;
        }

        throw new NotSupportedException($"Rejected payload type {typeof(TMessage).Name} is not supported.");
    }

    private async Task SendOne<TMessage>(IProducer<ReadOnlySequence<byte>> producer, TMessage message, Activity? activity, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(Memory1K);
        try
        {
            using var stream = new MemoryStream(buffer);
            _avroSerializer.Serialize(message, stream);
            int bytesWritten = (int)stream.Position;
            var sequence = new ReadOnlySequence<byte>(buffer.AsMemory(0, bytesWritten));
            await producer.Send(metadata, sequence, cancellationToken);
            activity?.SetTag("result", "success");
            return;
        }
        catch (NotSupportedException)
        {
            activity?.SetTag("fallback_triggered", true);
        }
        catch (Exception exception)
        {
            activity?.SetTag("result", "error");
            activity?.SetStatus(ActivityStatusCode.Error, exception.GetType().Name);
            throw;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        using RecyclableMemoryStream dynamicStream = MemoryManager.Instance.GetStream();
        _avroSerializer.Serialize(message, dynamicStream);
        await producer.Send(metadata, dynamicStream.GetReadOnlySequence(), cancellationToken);
        activity?.SetTag("result", "success_slowpath");
    }

    private static SportRejectedMessage ToMessage(Rejected<SportMessage> rejected)
        => new(rejected.RejectedId,
               rejected.OriginalTopic,
               rejected.OriginalPartitionId,
               rejected.OriginalBrokerMessageId,
               rejected.OriginalMessageKey,
               new RejectedReasonMessage(rejected.Reason.Code, rejected.Reason.Description),
               rejected.OriginalPayload,
               rejected.RejectedAt.ToUniversalTime(),
               rejected.OriginalCorrelationId,
               rejected.OriginalCausationId,
               rejected.OriginalMessageId);

    private static RawCategoryRejectedMessage ToMessage(Rejected<RawCategoryMessage> rejected)
        => new(rejected.RejectedId,
               rejected.OriginalTopic,
               rejected.OriginalPartitionId,
               rejected.OriginalBrokerMessageId,
               rejected.OriginalMessageKey,
               new RejectedReasonMessage(rejected.Reason.Code, rejected.Reason.Description),
               rejected.OriginalPayload,
               rejected.RejectedAt.ToUniversalTime(),
               rejected.OriginalCorrelationId,
               rejected.OriginalCausationId,
               rejected.OriginalMessageId);

    public async ValueTask DisposeAsync()
    {
        await _sportRejectedProducer.DisposeAsync();
        await _categoryRejectedProducer.DisposeAsync();
    }
}
