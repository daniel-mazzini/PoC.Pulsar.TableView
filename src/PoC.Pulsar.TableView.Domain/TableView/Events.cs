namespace PoC.Pulsar.TableView.Domain.TableView;

public abstract record TableEntryChange<T>();

public sealed record Added<T>(T Current,long Version) : TableEntryChange<T>();

public sealed record Updated<T>(T Previous,T Current, long Version) : TableEntryChange<T>();

public sealed record Deleted<T>(T Previous) : TableEntryChange<T>();

public sealed record TableEntryCreated<T>(string Key, T NewValue) : TableEntryChange<T>;

public sealed record TableEntryUpdated<T>(string Key, T NewValue, T CurrentValue) : TableEntryChange<T>;

public sealed record EventDeleted<T>(string Key, T CurrentValue) : TableEntryChange<T>;
