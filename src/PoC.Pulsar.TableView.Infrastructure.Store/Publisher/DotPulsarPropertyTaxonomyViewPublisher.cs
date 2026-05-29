using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Microsoft.IO;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Infrastructure.Store.Observability;
using PoC.Pulsar.TableView.Infrastructure.Store.Readers;
using PoC.Pulsar.TableView.Infrastructure.Store.Serialization;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Publisher;

[ExcludeFromCodeCoverage(Justification = "Integration adapter around a real DotPulsar producer.")]
public sealed class DotPulsarPropertyTaxonomyViewPublisher : ITaxonomyViewPublisher, IAsyncDisposable
{
    private const string EventType = "country-taxonomy-updated";
    private readonly IProducer<ReadOnlySequence<byte>> _producer;
    private readonly IAvroSerializer _avroSerializer;

    public DotPulsarPropertyTaxonomyViewPublisher(IPulsarClient client, string topicNamespace, IAvroSerializer avroSerializer)
    {
        _producer = client.NewProducer(Schema.ByteSequence)
            .Topic(PulsarTopics.Qualify(topicNamespace, PulsarTopics.CountryTaxonomyViews))
            .Create();
        _avroSerializer = avroSerializer;
    }
    public async ValueTask PublishAsync(GeoTaxonomyViewMessage taxonomy, CancellationToken cancellationToken)
    {
        using var activity = ProjectorStoreTelemetry.StartActivity("projection.country_taxonomy_view.publish",
                                                                   PulsarTopics.CountryTaxonomyViews,
                                                                   operation: "Publish");
        activity?.SetTag("entity_type", "country_taxonomy_view");
        activity?.SetTag("message_type", nameof(GeoTaxonomyViewMessage));
        activity?.SetTag("event_type", EventType);

        var headers = GetUpdateHeaders(Guid.CreateVersion7());
        var metadata = new MessageMetadata
        {
            Key = taxonomy.SportId,
            EventTimeAsDateTimeOffset = taxonomy.Timestamp,
            DeliverAtTimeAsDateTimeOffset = DateTimeOffset.UtcNow
        };

        foreach (var header in headers)
        {
            metadata[header.Key] = header.Value;
        }

        await SendOne(taxonomy, activity, metadata, cancellationToken);
    }
    
    private const int memory_10K = 10240;
    private async Task SendOne(GeoTaxonomyViewMessage message, Activity? activity, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(memory_10K);
        try
        {
            using var stream = new MemoryStream(buffer);
            _avroSerializer.Serialize(message, stream);

            int bytesWritten = (int)stream.Position;
            var sequence = new ReadOnlySequence<byte>(buffer.AsMemory(0, bytesWritten));
            await _producer.Send(metadata, sequence, cancellationToken);
            activity?.SetTag("result", "success");
        }
        catch(NotSupportedException)
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
        await _producer.Send(metadata, dynamicStream.GetReadOnlySequence(), cancellationToken);
        activity?.SetTag("result", "success_slowpath");
    }

    public ValueTask DisposeAsync() => _producer.DisposeAsync();

    

    public async ValueTask PublishDeleteMessageAsync(string sportId, DateTimeOffset eventTimeStamp, CancellationToken cancellationToken)
    {
        
        var metadata = new MessageMetadata
        {
            Key = sportId,
            EventTimeAsDateTimeOffset = eventTimeStamp,
            DeliverAtTimeAsDateTimeOffset = DateTimeOffset.UtcNow
        };
        foreach (var header in GetUpdateHeaders(Guid.CreateVersion7()))
        {
            metadata[header.Key] = header.Value;
        }
        await _producer.Send(metadata, new ReadOnlySequence<byte>([]), cancellationToken);
    }

    public async ValueTask PublishListMessage(IEnumerable<GeoTaxonomyViewMessage> taxonomies, CancellationToken cancellationToken)
    {
        using var activity = ProjectorStoreTelemetry.StartActivity("projection.country_taxonomy_view.publish_list",
                                                                   PulsarTopics.CountryTaxonomyViews,
                                                                   operation: "PublishList");
        activity?.SetTag("entity_type", "country_taxonomy_view");
        activity?.SetTag("message_type", nameof(GeoTaxonomyViewMessage));
        activity?.SetTag("event_type", EventType);

        foreach (var view in taxonomies)
        {
            var metadata = new MessageMetadata
            {
                Key = view.SportId,
                EventTimeAsDateTimeOffset = view.Timestamp,
                DeliverAtTimeAsDateTimeOffset = DateTimeOffset.UtcNow
            };
            
            foreach (var header in GetUpdateHeaders(Guid.CreateVersion7()))
            {
                metadata[header.Key] = header.Value;
            }

            await SendOne(view, activity, metadata, cancellationToken);
        }

    }

    private Dictionary<string,string> GetUpdateHeaders(Guid messageId)
    {
        return new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["type"] = nameof(GeoTaxonomyViewMessage),
            ["event-type"] = "country-taxonomy-updated",
            ["message-id"] = messageId.ToString("D")
        };
    }

    

    
}