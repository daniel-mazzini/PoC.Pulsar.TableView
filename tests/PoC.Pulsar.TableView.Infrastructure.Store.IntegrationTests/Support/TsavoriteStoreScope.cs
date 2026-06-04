using System.IO;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

internal sealed class TsavoriteStoreScope : IDisposable
{
    public TsavoriteStoreScope(string testName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(testName);
        var shortName = testName.Length <= 12 ? testName : testName[..12];
        StorePath = Path.Combine(IntegrationTestPaths.TestStoresRootPath, $"{shortName}-{Guid.CreateVersion7():N}");
        Directory.CreateDirectory(StorePath);
    }

    public string StorePath { get; }

    public void Dispose()
    {
        if (!Directory.Exists(StorePath))
        {
            return;
        }

        try
        {
            Directory.Delete(StorePath, recursive: true);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}
