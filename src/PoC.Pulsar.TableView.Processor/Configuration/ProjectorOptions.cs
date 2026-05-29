namespace PoC.Pulsar.TableView.Processor.Configuration;

internal sealed record ProjectorOptions
{
    public const string DefaultServiceUrl = "pulsar://127.0.0.1:6650";
    public const string DefaultInputNamespace = "public/tableview-inputs";
    public const string DefaultOutputNamespace = "public/tableview-outputs";
    public const string DefaultStorePath = "/tmp/poc.pulsarreader/property-taxonomy-projector";

    public string ServiceUrl { get; init; } = DefaultServiceUrl;
    public string InputNamespace { get; init; } = DefaultInputNamespace;
    public string OutputNamespace { get; init; } = DefaultOutputNamespace;
    public string StorePath { get; init; } = DefaultStorePath;
}