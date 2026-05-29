using System.Buffers;
using System.Collections.Generic;
using System.Linq;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages;

internal static class StoredListMerge
{
    public static IReadOnlyList<Guid> Merge(IReadOnlyList<Guid> current, Guid id)
        => current.Contains(id) ? current : [.. current, id];

    public static IReadOnlyList<string> Merge(IReadOnlyList<string> current, string id)
        => current.Contains(id, StringComparer.Ordinal) ? current : [.. current, id];

    public static IReadOnlyList<int> Merge(IReadOnlyList<int> current, int id)
        => current.Contains(id) ? current : [.. current, id];

    public static IReadOnlyList<Guid> Remove(IReadOnlyList<Guid> current, Guid id)
        => current.Where(existing => existing != id).ToArray();

    public static IReadOnlyList<int> Remove(IReadOnlyList<int> current, int id)
        => current.Where(existing => existing != id).ToArray();

    public static IReadOnlyList<string> Remove(IReadOnlyList<string> current, string id)
        => current.Where(existing => !StringComparer.Ordinal.Equals(existing, id)).ToArray();
}
