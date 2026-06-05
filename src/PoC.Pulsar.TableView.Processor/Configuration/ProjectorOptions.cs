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

    public static ProjectorOptions FromEnvironment()
        => new()
        {
            ServiceUrl = Environment.GetEnvironmentVariable("PULSAR_SERVICE_URL") ?? DefaultServiceUrl,
            InputNamespace = Environment.GetEnvironmentVariable("PULSAR_INPUT_NAMESPACE") ?? DefaultInputNamespace,
            OutputNamespace = Environment.GetEnvironmentVariable("PULSAR_OUTPUT_NAMESPACE") ?? DefaultOutputNamespace,
            StorePath = Environment.GetEnvironmentVariable("PROJECTOR_STORE_PATH") ?? DefaultStorePath
        };
}
