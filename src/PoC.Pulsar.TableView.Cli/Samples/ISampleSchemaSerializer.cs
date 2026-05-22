namespace PoC.Pulsar.TableView.Cli.Samples;

internal interface ISampleSchemaSerializer
{
    Task<byte[]> SerializeAsync<T>(T value, string schemaFileName)
        where T : class;
}
