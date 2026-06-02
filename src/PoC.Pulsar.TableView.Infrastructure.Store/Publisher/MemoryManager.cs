using Microsoft.IO;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Publisher;

public static class MemoryManager
{
    // this SHOULD be singleton
    public static readonly RecyclableMemoryStreamManager Instance = new();
}
