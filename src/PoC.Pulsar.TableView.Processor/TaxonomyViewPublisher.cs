using System.Buffers;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using PoC.Pulsar.TableView.Contracts;

namespace PoC.Pulsar.TableView.Processor;


public sealed class TaxonomyViewPublisher : ITaxonomyViewPublisher, IAsyncDisposable
{
    private readonly IProducer<ReadOnlySequence<byte>> _producer;
    private readonly ITaxonomyViewPublisher _publisher;
    private readonly IAvroSerializer<GeoTaxonomyMessage> _avroSerializer; // Tu serializador Avro

    public TaxonomyViewPublisher(IPulsarClient client, 
                                 string outputTopic, 
                                 ITaxonomyViewPublisher publisher,
                                 IAvroSerializer<GeoTaxonomyMessage> avroSerializer)
    {
        _publisher = publisher;
        _avroSerializer = avroSerializer;
        
        // Seguimos usando secuencias de bytes crudas para DotPulsar
        _producer = client.NewProducer(Schema.ByteSequence)
            .Topic(outputTopic)
            .Create();
    }

    public async ValueTask PublishAsync(GeoTaxonomyMessage taxonomy, CancellationToken cancellationToken = default)
    {
        // 1. Serializamos a Avro en lugar de JSON
        byte[] avroBytes = _avroSerializer.Serialize(taxonomy);
        
        // 2. Lo pasamos al formato que DotPulsar necesita
        var sequence = new ReadOnlySequence<byte>(avroBytes);

        await _producer.Send(new MessageMetadata { Key = taxonomy.SportId }, sequence, cancellationToken);
    }

    public async ValueTask DeleteAsync(string sportId, CancellationToken cancellationToken = default)
    {
        var emptySequence = new ReadOnlySequence<byte>(Array.Empty<byte>());
        await _producer.Send(new MessageMetadata { Key = sportId }, emptySequence, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (_producer is not null)
        {
            await _producer.DisposeAsync().ConfigureAwait(false);
        }
    }

}
