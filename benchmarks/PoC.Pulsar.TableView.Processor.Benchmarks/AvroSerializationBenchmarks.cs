using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using PoC.Pulsar.TableView.Contracts;

namespace PoC.Pulsar.TableView.Processor.Benchmarks;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
public class GeoTaxonomyAvroSerializationBenchmarks
{
    private DefaultAvroSerializer<GeoTaxonomyMessage> _serializer = null!;
    private GeoTaxonomyMessage _message = null!;

    [Params(1, 10, 100, 1_000)]
    public int CategoryCount { get; set; }

    [GlobalSetup] 
    public void global_setup()
    {
        _serializer = new DefaultAvroSerializer<GeoTaxonomyMessage>("GeoTaxonomyMessage.avsc");
        _message = create_message(CategoryCount);
    }

    [Benchmark(Baseline = true)]
    public int serialize_to_byte_array()
    {
        var bytes = _serializer.Serialize(_message);

        return bytes.Length;
    }

    [Benchmark]
    public async ValueTask<long> serialize_to_pipe_writer()
    {
        return await AvroBenchmarkSerialization.serialize_to_pipe_writer(_serializer, _message);
    }

    [Benchmark]
    public long serialize_to_array_pool_stream()
    {
        return AvroBenchmarkSerialization.serialize_to_array_pool_stream(_serializer, _message);
    }

    [Benchmark]
    public long serialize_to_recyclable_memory_stream()
    {
        return AvroBenchmarkSerialization.serialize_to_recyclable_memory_stream(_serializer, _message);
    }

    private static GeoTaxonomyMessage create_message(int categoryCount)
    {
        var categories = new List<GeoTaxonomyNode>(categoryCount);

        for (var index = 0; index < categoryCount; index++)
        {
            categories.Add(new GeoTaxonomyNode
            {
                CountryCode = $"C{index % 1_000:000}"
            });
        }

        return new GeoTaxonomyMessage
        {
            SportId = "sport-1",
            SportName = "Football",
            SportType = "team",
            Version = 3,
            GeoCategories = categories
        };
    }
}

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
public class SportAvroSerializationBenchmarks
{
    private DefaultAvroSerializer<SportMessage> _serializer = null!;
    private SportMessage _message = null!;

    [Params(0, 1, 10, 100)]
    public int ExternalEntityCount { get; set; }

    [GlobalSetup]
    public void global_setup()
    {
        _serializer = new DefaultAvroSerializer<SportMessage>("SportMessage.avsc");
        _message = create_message(ExternalEntityCount);
    }

    [Benchmark(Baseline = true)]
    public int serialize_to_byte_array()
    {
        return _serializer.Serialize(_message).Length;
    }

    [Benchmark]
    public async ValueTask<long> serialize_to_pipe_writer()
    {
        return await AvroBenchmarkSerialization.serialize_to_pipe_writer(_serializer, _message);
    }

    [Benchmark]
    public long serialize_to_array_pool_stream()
    {
        return AvroBenchmarkSerialization.serialize_to_array_pool_stream(_serializer, _message);
    }

    [Benchmark]
    public long serialize_to_recyclable_memory_stream()
    {
        return AvroBenchmarkSerialization.serialize_to_recyclable_memory_stream(_serializer, _message);
    }

    private static SportMessage create_message(int externalEntityCount)
    {
        return new SportMessage
        {
            Id = "sport-1",
            Provider = "provider-a",
            EntityCoverage = "global",
            Name = "Football",
            Version = 3,
            SportType = "team",
            ExternalEntities = create_external_entities(externalEntityCount)
        };
    }

    private static List<ExternalEntity> create_external_entities(int count)
    {
        var entities = new List<ExternalEntity>(count);

        for (var index = 0; index < count; index++)
        {
            entities.Add(new ExternalEntity
            {
                Id = $"ext-{index}",
                Provider = "provider-b",
                EntityCoverage = "regional",
                DefaultName = $"External {index}"
            });
        }

        return entities;
    }
}

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
public class RawCategoryAvroSerializationBenchmarks
{
    private DefaultAvroSerializer<RawCategoryMessage> _serializer = null!;
    private RawCategoryMessage _message = null!;

    [Params(false, true)]
    public bool IncludeNullableFields { get; set; }

    [Params(0, 1, 10, 100)]
    public int ExternalEntityCount { get; set; }

    [GlobalSetup]
    public void global_setup()
    {
        _serializer = new DefaultAvroSerializer<RawCategoryMessage>("RawCategoryMessage.avsc");
        _message = create_message(IncludeNullableFields, ExternalEntityCount);
    }

    [Benchmark(Baseline = true)]
    public int serialize_to_byte_array()
    {
        return _serializer.Serialize(_message).Length;
    }

    [Benchmark]
    public async ValueTask<long> serialize_to_pipe_writer()
    {
        return await AvroBenchmarkSerialization.serialize_to_pipe_writer(_serializer, _message);
    }

    [Benchmark]
    public long serialize_to_array_pool_stream()
    {
        return AvroBenchmarkSerialization.serialize_to_array_pool_stream(_serializer, _message);
    }

    [Benchmark]
    public long serialize_to_recyclable_memory_stream()
    {
        return AvroBenchmarkSerialization.serialize_to_recyclable_memory_stream(_serializer, _message);
    }

    private static RawCategoryMessage create_message(bool includeNullableFields, int externalEntityCount)
    {
        return new RawCategoryMessage
        {
            Id = "category-1",
            Provider = "provider-a",
            EntityCoverage = "global",
            Name = "England",
            Version = 5,
            SportId = "sport-1",
            ParentId = includeNullableFields ? "parent-1" : null,
            SportType = includeNullableFields ? "team" : null,
            CountryCode = includeNullableFields ? "GB" : null,
            Gender = includeNullableFields ? "mixed" : null,
            ExternalEntities = create_external_entities(externalEntityCount)
        };
    }

    private static List<ExternalEntity> create_external_entities(int count)
    {
        var entities = new List<ExternalEntity>(count);

        for (var index = 0; index < count; index++)
        {
            entities.Add(new ExternalEntity
            {
                Id = $"ext-{index}",
                Provider = "provider-b",
                EntityCoverage = "regional",
                DefaultName = $"External {index}"
            });
        }

        return entities;
    }
}
