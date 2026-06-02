namespace PoC.Pulsar.TableView.Domain.Rejected;

public interface IRejectedStorage
{
    ValueTask SaveRejectedRecordAsync(RejectedProjection rejectedProjection, CancellationToken cancellationToken);
}

