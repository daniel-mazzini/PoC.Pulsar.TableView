namespace PoC.Pulsar.TableView.Cli.Configuration;

internal sealed class PulsarPublishOptions
{
    public const string SectionName = "Pulsar";

    public string ServiceUrl { get; init; } = "";

    public string InputNamespace { get; init; } = "";
}
