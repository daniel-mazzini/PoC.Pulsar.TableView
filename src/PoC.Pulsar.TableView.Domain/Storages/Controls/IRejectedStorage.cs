using PoC.Pulsar.TableView.Domain.Entities;

namespace PoC.Pulsar.TableView.Domain.Storages.Controls;

public interface IRejectedStorage
{
    ValueTask SaveRejectedRecordAsync(RejectedProjection rejectedProjection, CancellationToken cancellationToken);
}

