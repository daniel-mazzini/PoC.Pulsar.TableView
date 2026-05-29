namespace PoC.Pulsar.TableView.Domain.Entities.Sports;

public sealed record Sport(SportId Id, string Name, string SportType, int Version, DateTimeOffset UpdateAt);
