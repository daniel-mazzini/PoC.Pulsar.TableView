using Chr.Avro.Abstract;
using Chr.Avro.Representation;
using System.Collections.Concurrent;
using System.IO;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Serialization;

public class AvroSchemaRegistry
{
    private readonly JsonSchemaReader _reader = new();
    private readonly ConcurrentDictionary<string, Schema> _schemas = new();

    public void RegisterDirectory(string directoryPath)
    {
        var files = Directory.GetFiles(directoryPath, "*.avsc");
        foreach (var file in files)
        {
            var json = File.ReadAllText(file);
            var schema = _reader.Read(json);
            var schemaName = Path.GetFileNameWithoutExtension(file);
            _schemas[schemaName] = schema;
        }
    }

    public Schema GetSchema(string name) =>
        _schemas.TryGetValue(name, out var s) ? s : throw new Exception($"Esquema {name} no registrado.");
}

