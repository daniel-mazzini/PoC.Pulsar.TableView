using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Microsoft.IO;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Entities;
using PoC.Pulsar.TableView.Infrastructure.Store.Observability;
using PoC.Pulsar.TableView.Infrastructure.Store.Publisher;
using PoC.Pulsar.TableView.Infrastructure.Store.Readers;
using PoC.Pulsar.TableView.Infrastructure.Store.Serialization;
using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;

namespace PoC.PulsarReader.PropertyTaxonomyProjector.Projection;

[ExcludeFromCodeCoverage(Justification = "Integration adapter around real DotPulsar producers.")]
internal sealed class DotPulsarRejectedMessagePublisher : IRejectedMessagePublisher, IAsyncDisposable
{
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

    public async Task PublishAsync(RejectedMessageWrite write, CancellationToken cancellationToken)
    {
        using var activity = ProjectorStoreTelemetry.StartActivity("projection.rejected.publish",
                                                                   PulsarTopics.CountryTaxonomyViews,
                                                                   operation: "Publish");
        activity?.SetTag("entity_type", "rejected_entity_view");
        activity?.SetTag("message_type", nameof(RejectedMessageWrite));
        

        var metadata = new MessageMetadata
                                {
                                    Key = write.MessageKey,
                                    EventTimeAsDateTimeOffset = write.Timestamp,
                                    DeliverAtTimeAsDateTimeOffset = DateTimeOffset.UtcNow
                                };
        foreach (var h in write.Headers) metadata[h.Key] = h.Value;
        var producer = write.Topic == PulsarTopics.SportsRejected ? _sportRejectedProducer : _categoryRejectedProducer;
        activity?.SetTag("event_type", write.Topic == PulsarTopics.SportsRejected ? "sport-rejected" : "category-rejected");

        await SendOne(producer, write, activity, metadata, cancellationToken);
    }
    private const int memory_1K = 1024;
    private async Task SendOne(IProducer<ReadOnlySequence<byte>> producer, RejectedMessageWrite message, Activity? activity, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(memory_1K);
        try
        {
            using var stream = new MemoryStream(buffer);
            Serialize(message, stream);
            int bytesWritten = (int)stream.Position;
            var sequence = new ReadOnlySequence<byte>(buffer.AsMemory(0, bytesWritten));
            await producer.Send(metadata, sequence, cancellationToken);
            activity?.SetTag("result", "success");
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
        // not so good performance, but more secure option, used as fallback 
        using RecyclableMemoryStream dynamicStream = MemoryManager.Instance.GetStream();
        Serialize(message, dynamicStream);
        await producer.Send(metadata, dynamicStream.GetReadOnlySequence(), cancellationToken);
        activity?.SetTag("result", "success_slowpath");
    }
    private void Serialize(RejectedMessageWrite write, Stream stream)
    {
        switch (write.Message)
        {
            case SportRejectedMessage sm:
                _avroSerializer.Serialize(sm, stream);
                break;

            case CategoryRejectedMessage cm:
                _avroSerializer.Serialize(cm, stream);
                break;

            default:
                throw new InvalidOperationException($"Type not expected '{write.Message.GetType().Name}'");
        }
    }
    public async ValueTask DisposeAsync()
    {
        await _sportRejectedProducer.DisposeAsync();
        await _categoryRejectedProducer.DisposeAsync();
    }
}