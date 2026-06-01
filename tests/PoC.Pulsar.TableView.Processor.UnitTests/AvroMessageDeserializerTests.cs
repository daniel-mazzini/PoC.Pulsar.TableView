using System.Buffers;
using System.IO.Pipelines;
using Microsoft.IO;
using PoC.Pulsar.TableView.Contracts;
using Xunit;

namespace PoC.Pulsar.TableView.Processor.UnitTests;

public sealed class AvroMessageDeserializerTests
{
    private static readonly RecyclableMemoryStreamManager StreamManager = new();

    [Fact]
    public void serialize_and_deserialize_should_round_trip_sport_message()
    {
        var serializer = new DefaultAvroSerializer<SportMessage>("SportMessage.avsc");
        var message = new SportMessage
        {
            Id = "sport-1",
            Provider = "provider-a",
            EntityCoverage = "global",
            Name = "Football",
            Version = 3,
            SportType = "team",
            ExternalEntities =
            [
                new ExternalEntity
                {
                    Id = "ext-1",
                    Provider = "provider-b",
                    EntityCoverage = "regional",
                    DefaultName = "Futbol"
                }
            ]
        };

        var bytes = serializer.Serialize(message);
        var roundTrip = serializer.Deserialize(new ReadOnlySequence<byte>(bytes));

        Assert.Equal(message.Id, roundTrip.Id);
        Assert.Equal(message.Provider, roundTrip.Provider);
        Assert.Equal(message.EntityCoverage, roundTrip.EntityCoverage);
        Assert.Equal(message.Name, roundTrip.Name);
        Assert.Equal(message.Version, roundTrip.Version);
        Assert.Equal(message.SportType, roundTrip.SportType);
        Assert.Single(roundTrip.ExternalEntities);
        Assert.Equal("ext-1", roundTrip.ExternalEntities[0].Id);
    }

    [Fact]
    public async Task byte_array_pipe_writer_array_pool_stream_and_recyclable_stream_serialization_should_match_for_sport_message()
    {
        var serializer = new DefaultAvroSerializer<SportMessage>("SportMessage.avsc");
        var message = new SportMessage
        {
            Id = "sport-1",
            Provider = "provider-a",
            EntityCoverage = "global",
            Name = "Football",
            Version = 3,
            SportType = "team",
            ExternalEntities =
            [
                new ExternalEntity
                {
                    Id = "ext-1",
                    Provider = "provider-b",
                    EntityCoverage = "regional",
                    DefaultName = "Futbol"
                }
            ]
        };

        var byteArrayBytes = serializer.Serialize(message);
        var pipeWriterBytes = await serialize_with_pipe_writer(serializer, message);
        var streamBytes = serialize_with_array_pool_stream(serializer, message);
        var recyclableStreamBytes = serialize_with_recyclable_memory_stream(serializer, message);
        var roundTrip = serializer.Deserialize(new ReadOnlySequence<byte>(pipeWriterBytes));

        Assert.Equal(byteArrayBytes, pipeWriterBytes);
        Assert.Equal(byteArrayBytes, streamBytes);
        Assert.Equal(byteArrayBytes, recyclableStreamBytes);
        Assert.Equal(message.Id, roundTrip.Id);
        Assert.Equal(message.Provider, roundTrip.Provider);
        Assert.Equal(message.EntityCoverage, roundTrip.EntityCoverage);
        Assert.Equal(message.Name, roundTrip.Name);
        Assert.Equal(message.Version, roundTrip.Version);
        Assert.Equal(message.SportType, roundTrip.SportType);
        Assert.Single(roundTrip.ExternalEntities);
        Assert.Equal("ext-1", roundTrip.ExternalEntities[0].Id);
    }

    [Fact]
    public async Task byte_array_pipe_writer_array_pool_stream_and_recyclable_stream_serialization_should_match_for_raw_category_message()
    {
        var serializer = new DefaultAvroSerializer<RawCategoryMessage>("RawCategoryMessage.avsc");
        var message = new RawCategoryMessage
        {
            Id = "category-1",
            Provider = "provider-a",
            EntityCoverage = "global",
            Name = "England",
            Version = 5,
            SportId = "sport-1",
            ParentId = "parent-1",
            SportType = "team",
            CountryCode = "GB",
            Gender = "mixed",
            ExternalEntities =
            [
                new ExternalEntity
                {
                    Id = "ext-1",
                    Provider = "provider-b",
                    EntityCoverage = "regional",
                    DefaultName = "England"
                }
            ]
        };

        var byteArrayBytes = serializer.Serialize(message);
        var pipeWriterBytes = await serialize_with_pipe_writer(serializer, message);
        var streamBytes = serialize_with_array_pool_stream(serializer, message);
        var recyclableStreamBytes = serialize_with_recyclable_memory_stream(serializer, message);
        var roundTrip = serializer.Deserialize(new ReadOnlySequence<byte>(pipeWriterBytes));

        Assert.Equal(byteArrayBytes, pipeWriterBytes);
        Assert.Equal(byteArrayBytes, streamBytes);
        Assert.Equal(byteArrayBytes, recyclableStreamBytes);
        Assert.Equal(message.Id, roundTrip.Id);
        Assert.Equal(message.Provider, roundTrip.Provider);
        Assert.Equal(message.EntityCoverage, roundTrip.EntityCoverage);
        Assert.Equal(message.Name, roundTrip.Name);
        Assert.Equal(message.Version, roundTrip.Version);
        Assert.Equal(message.SportId, roundTrip.SportId);
        Assert.Equal(message.ParentId, roundTrip.ParentId);
        Assert.Equal(message.SportType, roundTrip.SportType);
        Assert.Equal(message.CountryCode, roundTrip.CountryCode);
        Assert.Equal(message.Gender, roundTrip.Gender);
        Assert.Single(roundTrip.ExternalEntities);
        Assert.Equal("ext-1", roundTrip.ExternalEntities[0].Id);
    }

    [Fact]
    public async Task byte_array_pipe_writer_array_pool_stream_and_recyclable_stream_serialization_should_match_for_geo_taxonomy_view_message()
    {
        var serializer = new DefaultAvroSerializer<GeoTaxonomyViewMessage>("GeoTaxonomyViewMessage.avsc");
        var message = GeoTaxonomyViewMessage.Create(
            new SportMessage
            {
                Id = "sport-1",
                Name = "Football",
                SportType = "team"
            },
            [new GeoTaxonomyNode("category-1", "GB"), new GeoTaxonomyNode("category-2", "ES")],
            version: 3);

        var byteArrayBytes = serializer.Serialize(message);
        var pipeWriterBytes = await serialize_with_pipe_writer(serializer, message);
        var streamBytes = serialize_with_array_pool_stream(serializer, message);
        var recyclableStreamBytes = serialize_with_recyclable_memory_stream(serializer, message);
        var roundTrip = serializer.Deserialize(new ReadOnlySequence<byte>(pipeWriterBytes));

        Assert.Equal(byteArrayBytes, pipeWriterBytes);
        Assert.Equal(byteArrayBytes, streamBytes);
        Assert.Equal(byteArrayBytes, recyclableStreamBytes);
        Assert.Equal(message.SportId, roundTrip.SportId);
        Assert.Equal(message.SportName, roundTrip.SportName);
        Assert.Equal(message.SportType, roundTrip.SportType);
        Assert.Equal(message.Version, roundTrip.Version);
        Assert.Equal(2, roundTrip.GeoCategories.Count);
        Assert.Equal(["ES", "GB"], roundTrip.GeoCategories.Select(category => category.CountryCode).OrderBy(code => code));
    }

    private static async Task<byte[]> serialize_with_pipe_writer<T>(DefaultAvroSerializer<T> serializer, T message)
        where T : class
    {
        var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: 0));

        serializer.Serialize(message, pipe.Writer);
        await pipe.Writer.CompleteAsync();

        var result = await pipe.Reader.ReadAsync();

        try
        {
            return result.Buffer.ToArray();
        }
        finally
        {
            pipe.Reader.AdvanceTo(result.Buffer.End);
            await pipe.Reader.CompleteAsync();
        }
    }

    private static byte[] serialize_with_array_pool_stream<T>(DefaultAvroSerializer<T> serializer, T message)
        where T : class
    {
        var buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);

        try
        {
            using var stream = new MemoryStream(buffer);
            serializer.Serialize(message, stream);

            return buffer.AsSpan(0, (int)stream.Position).ToArray();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static byte[] serialize_with_recyclable_memory_stream<T>(DefaultAvroSerializer<T> serializer, T message)
        where T : class
    {
        using var stream = (RecyclableMemoryStream)StreamManager.GetStream();
        serializer.Serialize(message, stream);

        return stream.GetReadOnlySequence().ToArray();
    }
}
