namespace PoC.Pulsar.TableView.Cli.Configuration;

internal sealed class TsavoriteViewerOptions
{
    public const string SectionName = "TsavoriteViewer";

    public string BaseUrl { get; init; } = "http://127.0.0.1:18080";
}
