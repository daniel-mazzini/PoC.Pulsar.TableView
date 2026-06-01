using Chr.Avro.Abstract;
using Chr.Avro.Representation;
using System.IO;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Serialization;

public class AvroSchemaRegistry
{
    private readonly JsonSchemaReader _reader = new();
    private readonly ConcurrentDictionary<Type, Schema> _schemas = new();

    public void Register<T>(string filePath) where T : notnull
    {
        var json = File.ReadAllText(filePath);
        var schema = _reader.Read(json);
        //var schemaName = Path.GetFileNameWithoutExtension(filePath);
        _schemas.TryAdd(typeof(T), schema);
    }

    //public void RegisterDirectory(string directoryPath)
    //{
    //    var files = Directory.GetFiles(directoryPath, "*.avsc");
    //    foreach (var file in files)
    //    {
    //        var json = File.ReadAllText(file);
    //        var schema = _reader.Read(json);
    //        var schemaName = Path.GetFileNameWithoutExtension(file);
    //        _schemas[schemaName] = schema;
    //    }
    //}

    public AvroSerializer Build()
    {
        return new AvroSerializer(_schemas);
    }
}

