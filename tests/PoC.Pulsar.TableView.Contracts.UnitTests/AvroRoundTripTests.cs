using Chr.Avro.Representation;
using Chr.Avro.Serialization;
using PoC.Pulsar.TableView.Contracts;
using Xunit;
using BinaryReader = Chr.Avro.Serialization.BinaryReader;
using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

namespace PoC.Pulsar.TableView.Contracts.UnitTests;

public sealed class AvroRoundTripTests
{
    private static readonly System.Reflection.BindingFlags MemberVisibility = System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public;

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
    public void geo_taxonomy_view_message_round_trips_nested_nodes()
    {
        var message = GeoTaxonomyViewMessage.Create(
            new SportMessage
            {
                Id = "sport-1",
                Name = "Football",
                SportType = "team"
            },
            [new GeoTaxonomyNode("category-1", "GB")],
            version: 3);

        var roundTrip = RoundTrip(message, "GeoTaxonomyViewMessage.avsc");

        Assert.Equal(message.SportId, roundTrip.SportId);
        Assert.Equal(message.SportName, roundTrip.SportName);
        Assert.Equal(message.SportType, roundTrip.SportType);
        Assert.Equal(message.Version, roundTrip.Version);
        Assert.Single(roundTrip.GeoCategories);
        Assert.Equal("GB", roundTrip.GeoCategories.First().CountryCode);
    }

    [Fact]
    public void input_schemas_include_inherited_fields()
    {
        var sportSchema = ParseSchema("SportMessage.avsc");
        var categorySchema = ParseSchema("RawCategoryMessage.avsc");

        Assert.True(SchemaHasField(sportSchema, nameof(Entity.Id)));
        Assert.True(SchemaHasField(sportSchema, nameof(OfferHierarchyEntity.Name)));
        Assert.True(SchemaHasField(sportSchema, nameof(OfferHierarchyEntity.Version)));
        Assert.True(SchemaHasField(sportSchema, nameof(SportMessage.SportType)));

        Assert.True(SchemaHasField(categorySchema, nameof(Entity.Id)));
        Assert.True(SchemaHasField(categorySchema, nameof(OfferHierarchyEntity.Name)));
        Assert.True(SchemaHasField(categorySchema, nameof(OfferHierarchyEntity.Version)));
        Assert.True(SchemaHasField(categorySchema, nameof(RawCategoryMessage.SportId)));
    }

    private static T RoundTrip<T>(T value, string schemaFileName)
        where T : class
    {
        var schemaJson = ReadSchemaJson(schemaFileName);
        var schema = new JsonSchemaReader((IJsonDeserializerBuilder?)null).Read(schemaJson, new JsonSchemaReaderContext());
        var serializer = new BinarySerializerBuilder(MemberVisibility)
            .BuildDelegate<T>(schema, new BinarySerializerBuilderContext(null));
        var deserializer = new BinaryDeserializerBuilder(MemberVisibility)
            .BuildDelegate<T>(schema, new BinaryDeserializerBuilderContext(null));

        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        serializer(value, writer);

        stream.Position = 0;
        var reader = new BinaryReader(stream.ToArray());

        return deserializer(ref reader);
    }

    private static object ParseSchema(string schemaFileName)
    {
        var schemaJson = ReadSchemaJson(schemaFileName);

        return new JsonSchemaReader((IJsonDeserializerBuilder?)null).Read(schemaJson, new JsonSchemaReaderContext());
    }

    private static string ReadSchemaJson(string schemaFileName)
    {
        var schemaPath = Path.Combine(AppContext.BaseDirectory, "Schemas", schemaFileName);

        return File.ReadAllText(schemaPath);
    }

    private static bool SchemaHasField(object schema, string fieldName)
    {
        var fieldsProperty = schema.GetType().GetProperty("Fields")
            ?? throw new InvalidOperationException("Schema does not expose fields.");
        var fields = (System.Collections.IEnumerable)fieldsProperty.GetValue(schema)!;

        foreach (var field in fields)
        {
            var nameProperty = field.GetType().GetProperty("Name")
                ?? throw new InvalidOperationException("Schema field does not expose a name.");

            if (Equals(nameProperty.GetValue(field), fieldName))
            {
                return true;
            }
        }

        return false;
    }
}
