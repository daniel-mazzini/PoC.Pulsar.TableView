using PoC.Pulsar.TableView.Cli.Samples;
using PoC.Pulsar.TableView.Contracts;
using Xunit;

namespace PoC.Pulsar.TableView.Cli.UnitTests.Samples;

public sealed class AvroSampleSchemaSerializerTests
{
    [Fact]
    public async Task serialize_async_should_use_resolved_schema_folder()
    {
        var schemaFolder = Path.Combine(AppContext.BaseDirectory, "AvroSchemas");
        Directory.CreateDirectory(schemaFolder);

        var repoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."));
        var sourceSchemaPath = Path.Combine(repoRoot, "src", "PoC.Pulsar.TableView.Contracts", "AvroSchemas", "SportMessage.avsc");
        File.Copy(sourceSchemaPath, Path.Combine(schemaFolder, "SportMessage.avsc"), overwrite: true);

        var serializer = new AvroSampleSchemaSerializer();
        var payload = await serializer.SerializeAsync(new SportMessage
        {
            Id = "sport-1",
            Provider = "provider-a",
            EntityCoverage = "global",
            Name = "Football",
            Version = 1,
            SportType = "team"
        }, "SportMessage.avsc");

        Assert.NotEmpty(payload);
    }
}
