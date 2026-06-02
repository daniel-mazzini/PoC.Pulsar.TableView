namespace PoC.Pulsar.TableView.Domain.Projector;

public abstract record ProjectionApplyResult(string Result)
{
    public sealed record Applied(string EntityId, bool Published) : ProjectionApplyResult("applied");

    public sealed record Rejected(string EntityId, string Reason) : ProjectionApplyResult("rejected");
}