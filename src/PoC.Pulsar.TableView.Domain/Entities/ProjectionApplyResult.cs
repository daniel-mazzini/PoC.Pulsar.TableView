namespace PoC.Pulsar.TableView.Domain.Entities;

public abstract record ProjectionApplyResult(string Result)
{
    public sealed record Applied(string EntityId, bool Published) : ProjectionApplyResult("applied");

    public sealed record Pending(string EntityId, string Reason) : ProjectionApplyResult("pending");

    public sealed record Rejected(string EntityId, string Reason) : ProjectionApplyResult("rejected");
}