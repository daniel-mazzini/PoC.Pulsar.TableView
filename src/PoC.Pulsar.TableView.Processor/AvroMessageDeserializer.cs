using System.Buffers;
using System.Reflection;
using Avro;
using Avro.Generic;
using Avro.IO;

namespace PoC.Pulsar.TableView.Processor;

internal sealed class AvroMessageDeserializer<T>
    where T : class, new()
{
    private readonly RecordSchema _schema;

    public AvroMessageDeserializer(string schemaFileName)
    {
        var schemaPath = Path.Combine(AppContext.BaseDirectory, "Schemas", schemaFileName);
        _schema = (RecordSchema)Schema.Parse(File.ReadAllText(schemaPath));
    }

    public T Deserialize(ReadOnlySequence<byte> data)
    {
        using var stream = new MemoryStream(data.ToArray());
        var reader = new GenericDatumReader<GenericRecord>(_schema, _schema);
        var record = reader.Read(null!, new BinaryDecoder(stream));
        return FromRecord(record, _schema);
    }

    private static T FromRecord(GenericRecord record, RecordSchema schema)
    {
        var instance = new T();
        var properties = typeof(T)
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .ToDictionary(property => property.Name, property => property);

        foreach (var field in schema.Fields)
        {
            var property = properties[field.Name];
            property.SetValue(instance, FromAvroValue(record[field.Name], field.Schema, property.PropertyType));
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
            RecordSchema recordSchema => FromNestedRecord((GenericRecord)value, recordSchema, targetType),
            ArraySchema arraySchema => FromArray(value, arraySchema, targetType),
            _ => value
        };
    }

    private static object FromNestedRecord(GenericRecord record, RecordSchema schema, Type targetType)
    {
        var instance = Activator.CreateInstance(targetType)
            ?? throw new InvalidOperationException($"Could not create {targetType.FullName}.");

        var properties = targetType
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .ToDictionary(property => property.Name, property => property);

        foreach (var field in schema.Fields)
        {
            var property = properties[field.Name];
            property.SetValue(instance, FromAvroValue(record[field.Name], field.Schema, property.PropertyType));
        }

        return instance;
    }

    private static object FromArray(object value, ArraySchema arraySchema, Type targetType)
    {
        var elementType = targetType.GetGenericArguments().Single();
        var listType = typeof(List<>).MakeGenericType(elementType);
        var list = (System.Collections.IList)Activator.CreateInstance(listType)!;

        foreach (var item in (System.Collections.IEnumerable)value)
        {
            list.Add(FromAvroValue(item, arraySchema.ItemSchema, elementType));
        }

        return list;
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
