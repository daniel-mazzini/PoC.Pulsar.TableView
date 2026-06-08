namespace PoC.Pulsar.TableView.Domain.Storages.StateStore;

public interface IStateSession : IDisposable
{
    Guid SessionId { get; }
}