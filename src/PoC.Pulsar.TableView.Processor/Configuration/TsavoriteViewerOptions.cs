namespace PoC.Pulsar.TableView.Processor.Configuration;

internal sealed record TsavoriteViewerOptions(bool Enabled, string Url)
{
    public const string DefaultUrl = "http://127.0.0.1:18080";

    public static TsavoriteViewerOptions FromEnvironment()
        => new(IsEnabled(Environment.GetEnvironmentVariable("TSAVORITE_VIEWER_ENABLED")),
               Environment.GetEnvironmentVariable("TSAVORITE_VIEWER_URL") ?? DefaultUrl);

    public static bool IsEnabled(string? value)
        => bool.TryParse(value, out var enabled) && enabled;
}
