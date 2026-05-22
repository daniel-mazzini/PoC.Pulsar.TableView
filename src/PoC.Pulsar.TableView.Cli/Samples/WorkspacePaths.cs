namespace PoC.Pulsar.TableView.Cli.Samples;

internal static class WorkspacePaths
{
    public static string ResolveSampleFolder()
    {
        return ResolveFolder("samples", "publish", "sports_mock_data.json");
    }

    public static string ResolveSchemaFolder()
    {
        return ResolveFolder("Schemas", "SportMessage.avsc");
    }

    private static string ResolveFolder(params string[] pathSegments)
    {
        var current = new DirectoryInfo(AppContext.BaseDirectory);

        while (current is not null)
        {
            var candidate = current.FullName;

            foreach (var segment in pathSegments)
            {
                candidate = Path.Combine(candidate, segment);
            }

            if (File.Exists(candidate))
            {
                return Path.GetDirectoryName(candidate) ?? throw new InvalidOperationException($"Could not resolve {string.Join('/', pathSegments)}.");
            }

            current = current.Parent;
        }

        throw new FileNotFoundException($"Could not find {string.Join('/', pathSegments)} from {AppContext.BaseDirectory}.");
    }
}
