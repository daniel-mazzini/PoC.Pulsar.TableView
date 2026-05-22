using System.Buffers;
using System.IO.Pipelines;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using PoC.Pulsar.TableView.Contracts;

namespace PoC.Pulsar.TableView.Processor;


public sealed class TaxonomyViewPublisher : ITaxonomyViewPublisher, IAsyncDisposable
{
    private readonly IProducer<ReadOnlySequence<byte>> _producer;
    private readonly IAvroSerializer<GeoTaxonomyMessage> _avroSerializer; // Tu serializador Avro

    public TaxonomyViewPublisher(IPulsarClient client,
                                 string outputTopic,
                                 IAvroSerializer<GeoTaxonomyMessage> avroSerializer)
    {
        _avroSerializer = avroSerializer;
        _producer = client.NewProducer(Schema.ByteSequence)
            .Topic(outputTopic)
            .Create();
    }

    public async ValueTask PublishAsync(GeoTaxonomyMessage taxonomy, CancellationToken cancellationToken)
    {
        // Zero allocation serialization using PipeWriter
        var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: 0));
        _avroSerializer.Serialize(taxonomy, pipe.Writer);
        await pipe.Writer.CompleteAsync();

        ReadResult result = await pipe.Reader.ReadAsync(cancellationToken);

        try
        {
            await _producer.Send(new MessageMetadata { Key = taxonomy.SportId }, result.Buffer, cancellationToken);
        }
        finally
        {
            pipe.Reader.AdvanceTo(result.Buffer.End);
            pipe.Reader.Complete();
        }
    }

    public async ValueTask PublishDeleteMessageAsync(string sportId, CancellationToken cancellationToken)
    {
        var emptySequence = new ReadOnlySequence<byte>([]);
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
