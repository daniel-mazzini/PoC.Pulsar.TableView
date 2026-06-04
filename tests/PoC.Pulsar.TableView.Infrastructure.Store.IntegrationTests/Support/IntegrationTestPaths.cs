using System.IO;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

internal static class IntegrationTestPaths
{
    private static readonly Lazy<string> ProjectRootPathValue = new(() =>
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..")));

    private static readonly Lazy<string> RepositoryRootPathValue = new(() =>
        Path.GetFullPath(Path.Combine(ProjectRootPath, "..", "..")));

    public static string ProjectRootPath => ProjectRootPathValue.Value;

    public static string RepositoryRootPath => RepositoryRootPathValue.Value;

    public static string TestStoresRootPath => EnsureDirectory(Path.Combine(ProjectRootPath, ".t"));

    public static string AvroSchemasRootPath => Path.Combine(RepositoryRootPath, "src", "PoC.Pulsar.TableView.Contracts", "AvroSchemas");

    public static string EnsureDirectory(string path)
    {
        Directory.CreateDirectory(path);
        return path;
    }
}
