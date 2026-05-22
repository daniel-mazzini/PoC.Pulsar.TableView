using System.Buffers;
using System.IO.Pipelines;
using Microsoft.IO;

namespace PoC.Pulsar.TableView.Processor.Benchmarks;

internal static class AvroBenchmarkSerialization
{
    private const int BufferSize = 64 * 1024;
    private static readonly RecyclableMemoryStreamManager StreamManager = new();

    public static async ValueTask<long> serialize_to_pipe_writer<T>(DefaultAvroSerializer<T> serializer, T message)
        where T : class
    {
        var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: 0));

        serializer.Serialize(message, pipe.Writer);
        await pipe.Writer.CompleteAsync();

        var result = await pipe.Reader.ReadAsync();

        try
        {
            return result.Buffer.Length;
        }
        finally
        {
            pipe.Reader.AdvanceTo(result.Buffer.End);
            await pipe.Reader.CompleteAsync();
        }
    }

    public static long serialize_to_array_pool_stream<T>(DefaultAvroSerializer<T> serializer, T message)
        where T : class
    {
        var buffer = ArrayPool<byte>.Shared.Rent(BufferSize);

        try
        {
            using var stream = new MemoryStream(buffer);
            serializer.Serialize(message, stream);

            return stream.Position;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public static long serialize_to_recyclable_memory_stream<T>(DefaultAvroSerializer<T> serializer, T message)
        where T : class
    {
        using var stream = (RecyclableMemoryStream)StreamManager.GetStream();
        serializer.Serialize(message, stream);

        return stream.GetReadOnlySequence().Length;
    }
}
