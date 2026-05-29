using Microsoft.IO;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Publisher;

public static class MemoryManager
{
    // Forma correcta sin DI: Estático y de solo lectura
    public static readonly RecyclableMemoryStreamManager Instance = new();
}
