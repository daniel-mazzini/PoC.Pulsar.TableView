using System.Text.Json;
using PoC.Pulsar.TableView.Contracts;

namespace PoC.Pulsar.TableView.Cli.Samples;

internal sealed class SampleDataLoader : ISampleDataLoader
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public IReadOnlyList<SportMessage> LoadSports()
    {
        return Load<SportMessage>(Path.Combine(WorkspacePaths.ResolveSampleFolder(), "sports_mock_data.json"));
    }

    public IReadOnlyList<RawCategoryMessage> LoadCategories()
    {
        return Load<RawCategoryMessage>(Path.Combine(WorkspacePaths.ResolveSampleFolder(), "categories_mock_data.json"));
    }

    private static IReadOnlyList<T> Load<T>(string path)
    {
        var json = File.ReadAllText(path);
        return JsonSerializer.Deserialize<List<T>>(json, JsonOptions)
            ?? throw new InvalidOperationException($"Could not load sample data from {path}.");
    }
}
