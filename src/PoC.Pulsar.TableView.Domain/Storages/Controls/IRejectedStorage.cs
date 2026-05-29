using PoC.Pulsar.TableView.Domain.Entities;

namespace PoC.Pulsar.TableView.Domain.Storages.Controls;

public interface IRejectedStorage
{
    Task SaveRejectedRecordAsync(RejectedProjection rejectedProjection, CancellationToken cancellationToken);
}

