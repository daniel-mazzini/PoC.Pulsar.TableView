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
        byte[] buffer = ArrayPool<byte>.Shared.Rent(65536);

        try
        {
            using var stream = new MemoryStream(buffer);
            _avroSerializer.Serialize(taxonomy, stream);
            int bytesWritten = (int)stream.Position;
            ReadOnlyMemory<byte> validMemory = buffer.AsMemory(0, bytesWritten);
            var sequence = new ReadOnlySequence<byte>(validMemory);
            await _producer.Send(new MessageMetadata { Key = taxonomy.SportId }, sequence, cancellationToken);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
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
