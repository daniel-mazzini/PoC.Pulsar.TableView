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

    public async Task PublishAsync<TMessage>(RejectedMessage<TMessage> write, Dictionary<string, string> headers, CancellationToken cancellationToken)
    {
        using var activity = ProjectorStoreTelemetry.StartActivity("projection.rejected.publish",
                                                                   PulsarTopics.CountryTaxonomyViews,
                                                                   operation: "Publish");
        activity?.SetTag("message_type", nameof(RejectedMessage<TMessage>));
        

        var metadata = new MessageMetadata
                                {
                                    Key = write.OriginalMessageKey,
                                    EventTimeAsDateTimeOffset = write.RejectedAt,
                                    DeliverAtTimeAsDateTimeOffset = DateTimeOffset.UtcNow
                                };
        foreach (var h in headers) metadata[h.Key] = h.Value;
        var producer = write.OriginalTopic == PulsarTopics.Sports ? _sportRejectedProducer : _categoryRejectedProducer;
        activity?.SetTag("event_type", write.OriginalTopic == PulsarTopics.SportsRejected ? "sport-rejected" : "category-rejected");

        await SendOne(producer, write, activity, metadata, cancellationToken);
    }
    private const int memory_1K = 1024;
    private async Task SendOne<TMessage>(IProducer<ReadOnlySequence<byte>> producer, RejectedMessage<TMessage> message, Activity? activity, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(memory_1K);
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
        // not so good performance, but more secure option, used as fallback 
        using RecyclableMemoryStream dynamicStream = MemoryManager.Instance.GetStream();
        _avroSerializer.Serialize(message, dynamicStream);
        await producer.Send(metadata, dynamicStream.GetReadOnlySequence(), cancellationToken);
        activity?.SetTag("result", "success_slowpath");
    }
    public async ValueTask DisposeAsync()
    {
        await _sportRejectedProducer.DisposeAsync();
        await _categoryRejectedProducer.DisposeAsync();
    }

}