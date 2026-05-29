namespace PoC.Pulsar.TableView.Domain.Storages;

public abstract record Event<T>(string Key);
public record EventDeleted<T>(string Key, T CurrentValue) : Event<T>(Key);
public record EventCreated<T>(string Key, T NewValue) : Event<T>(Key);
public record EventUpdated<T>(string Key, T NewValue, T CurrentValue) : Event<T>(Key);
