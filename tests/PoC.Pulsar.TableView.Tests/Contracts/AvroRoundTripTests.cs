using System.Reflection;
using Avro;
using Avro.Generic;
using Avro.IO;
using PoC.Pulsar.TableView.Contracts;
using Xunit;

namespace PoC.Pulsar.TableView.Tests.Contracts;

public sealed class AvroRoundTripTests
{
    [Fact]
    public void sport_message_round_trips_base_properties()
    {
        var message = new SportMessage
        {
            Id = "sport-1",
            Provider = "provider-a",
            EntityCoverage = "global",
            Name = "Football",
            Version = 3,
            SportType = "team",
            ExternalEntities = [
                new ExternalEntity
                {
                    Id = "ext-1",
                    Provider = "provider-b",
                    EntityCoverage = "regional",
                    DefaultName = "Futbol"
                }
            ]
        };

        var roundTrip = RoundTrip(message, "SportMessage.avsc");

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
    public void raw_category_message_round_trips_base_properties()
    {
        var message = new RawCategoryMessage
        {
            Id = "category-1",
            Provider = "provider-a",
            EntityCoverage = "global",
            Name = "Premier League",
            Version = 7,
            SportId = "sport-1",
            ParentId = "parent-1",
            SportType = "team",
            CountryCode = "GB",
            Gender = "male",
            ExternalEntities = [
                new ExternalEntity
                {
                    Id = "ext-2",
                    Provider = "provider-b",
                    EntityCoverage = "regional",
                    DefaultName = "Premier League"
                }
            ]
        };

        var roundTrip = RoundTrip(message, "RawCategoryMessage.avsc");

        Assert.Equal(message.Id, roundTrip.Id);
        Assert.Equal(message.Name, roundTrip.Name);
        Assert.Equal(message.Version, roundTrip.Version);
        Assert.Equal(message.SportId, roundTrip.SportId);
        Assert.Equal(message.ParentId, roundTrip.ParentId);
        Assert.Equal(message.SportType, roundTrip.SportType);
        Assert.Equal(message.CountryCode, roundTrip.CountryCode);
        Assert.Equal(message.Gender, roundTrip.Gender);
        Assert.Single(roundTrip.ExternalEntities);
        Assert.Equal("ext-2", roundTrip.ExternalEntities[0].Id);
    }

    [Fact]
    public void geo_taxonomy_message_round_trips_nested_nodes()
    {
        var message = new GeoTaxonomyMessage
        {
            SportId = "sport-1",
            SportName = "Football",
            SportType = "team",
            GeoCategories = [
                new GeoTaxonomyNode
                {
                    CountryCode = "GB"
                }
            ]
        };

        var roundTrip = RoundTrip(message, "GeoTaxonomyMessage.avsc");

        Assert.Equal(message.SportId, roundTrip.SportId);
        Assert.Equal(message.SportName, roundTrip.SportName);
        Assert.Equal(message.SportType, roundTrip.SportType);
        Assert.Single(roundTrip.GeoCategories);
        Assert.Equal("GB", roundTrip.GeoCategories[0].CountryCode);
    }

    [Fact]
    public void input_schemas_include_inherited_fields()
    {
        var sportSchema = ParseSchema("SportMessage.avsc");
        var categorySchema = ParseSchema("RawCategoryMessage.avsc");

        Assert.Contains(sportSchema.Fields, field => field.Name == nameof(Entity.Id));
        Assert.Contains(sportSchema.Fields, field => field.Name == nameof(OfferHierarchyEntity.Name));
        Assert.Contains(sportSchema.Fields, field => field.Name == nameof(OfferHierarchyEntity.Version));
        Assert.Contains(sportSchema.Fields, field => field.Name == nameof(SportMessage.SportType));

        Assert.Contains(categorySchema.Fields, field => field.Name == nameof(Entity.Id));
        Assert.Contains(categorySchema.Fields, field => field.Name == nameof(OfferHierarchyEntity.Name));
        Assert.Contains(categorySchema.Fields, field => field.Name == nameof(OfferHierarchyEntity.Version));
        Assert.Contains(categorySchema.Fields, field => field.Name == nameof(RawCategoryMessage.SportId));
    }

    private static T RoundTrip<T>(T value, string schemaFileName)
        where T : class, new()
    {
        var schema = ParseSchema(schemaFileName);
        var record = ToRecord(value, schema);

        using var stream = new MemoryStream();
        var writer = new GenericDatumWriter<GenericRecord>(schema);
        var encoder = new BinaryEncoder(stream);
        writer.Write(record, encoder);

        stream.Position = 0;
        var reader = new GenericDatumReader<GenericRecord>(schema, schema);
        var decoder = new BinaryDecoder(stream);
        var decoded = reader.Read(null!, decoder);

        return FromRecord<T>(decoded, schema);
    }

    private static RecordSchema ParseSchema(string schemaFileName)
    {
        var schemaPath = Path.Combine(AppContext.BaseDirectory, "Schemas", schemaFileName);
        var schemaJson = File.ReadAllText(schemaPath);

        return (RecordSchema)Schema.Parse(schemaJson);
    }

    private static GenericRecord ToRecord(object value, RecordSchema schema)
    {
        var record = new GenericRecord(schema);
        var properties = value.GetType()
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .ToDictionary(property => property.Name, property => property);

        foreach (var field in schema.Fields)
        {
            var property = properties[field.Name];
            record.Add(field.Name, ToAvroValue(property.GetValue(value), field.Schema));
        }

        return record;
    }

    private static object? ToAvroValue(object? value, Schema schema)
    {
        schema = UnwrapNullable(schema);

        return schema switch
        {
            RecordSchema recordSchema => ToRecord(value!, recordSchema),
            ArraySchema arraySchema => ToAvroArray(value, arraySchema),
            _ => value
        };
    }

    private static Array ToAvroArray(object? value, ArraySchema arraySchema)
    {
        var values = new List<object?>();
        var enumerable = (System.Collections.IEnumerable?)value;

        if (enumerable is null)
        {
            return Array.Empty<object>();
        }

        foreach (var item in enumerable)
        {
            values.Add(ToAvroValue(item, arraySchema.ItemSchema));
        }

        return values.ToArray();
    }

    private static T FromRecord<T>(GenericRecord record, RecordSchema schema)
        where T : class, new()
    {
        var instance = new T();
        var properties = typeof(T)
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .ToDictionary(property => property.Name, property => property);

        foreach (var field in schema.Fields)
        {
            var property = properties[field.Name];
            var value = FromAvroValue(record[field.Name], field.Schema, property.PropertyType);
            property.SetValue(instance, value);
        }

        return instance;
    }

    private static object? FromAvroValue(object? value, Schema schema, Type targetType)
    {
        schema = UnwrapNullable(schema);

        if (value is null)
        {
            return null;
        }

        return schema switch
        {
            RecordSchema recordSchema => FromRecord(value, recordSchema, targetType),
            ArraySchema arraySchema => FromAvroArray(value, arraySchema, targetType),
            _ => value
        };
    }

    private static object FromRecord(object value, RecordSchema schema, Type targetType)
    {
        var instance = Activator.CreateInstance(targetType)
            ?? throw new InvalidOperationException($"Could not create {targetType.FullName}.");

        var properties = targetType
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .ToDictionary(property => property.Name, property => property);

        foreach (var field in schema.Fields)
        {
            var property = properties[field.Name];
            var fieldValue = ((GenericRecord)value)[field.Name];
            property.SetValue(instance, FromAvroValue(fieldValue, field.Schema, property.PropertyType));
        }

        return instance;
    }

    private static object FromAvroArray(object value, ArraySchema arraySchema, Type targetType)
    {
        var elementType = targetType.IsArray
            ? targetType.GetElementType()!
            : targetType.GetGenericArguments().Single();

        var listType = typeof(List<>).MakeGenericType(elementType);
        var list = (System.Collections.IList)Activator.CreateInstance(listType)!;

        foreach (var item in (System.Collections.IEnumerable)value)
        {
            list.Add(FromAvroValue(item, arraySchema.ItemSchema, elementType));
        }

        return targetType.IsArray ? ToArray(list, elementType) : list;
    }

    private static object ToArray(System.Collections.IList list, Type elementType)
    {
        var array = Array.CreateInstance(elementType, list.Count);

        for (var i = 0; i < list.Count; i++)
        {
            array.SetValue(list[i], i);
        }

        return array;
    }

    private static Schema UnwrapNullable(Schema schema)
    {
        if (schema is UnionSchema unionSchema)
        {
            return unionSchema.Schemas.First(candidate => candidate.Tag != Schema.Type.Null);
        }

        return schema;
    }
}
