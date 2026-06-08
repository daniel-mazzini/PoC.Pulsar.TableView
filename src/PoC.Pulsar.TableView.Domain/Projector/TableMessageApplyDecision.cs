using MemoryPack;

namespace PoC.Pulsar.TableView.Domain.Projector;

public enum TableMessageApplyKind
{
    NoOp,
    Created,
    Updated
}

[MemoryPackable]
public readonly partial record struct TableMessageApplyDecision(TableMessageApplyKind Kind, string? Reason = null)
{
    public static TableMessageApplyDecision Created() => new(TableMessageApplyKind.Created);

    public static TableMessageApplyDecision Updated() => new(TableMessageApplyKind.Updated);

    public static TableMessageApplyDecision NoOp(string reason) => new(TableMessageApplyKind.NoOp, reason);
}
