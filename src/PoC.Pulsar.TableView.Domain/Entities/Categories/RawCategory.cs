using PoC.Pulsar.TableView.Domain.Entities.Sports;

namespace PoC.Pulsar.TableView.Domain.Entities.Categories;
public sealed record RawCategory(CategoryId CategoryId, SportId SportId, CategoryId? ParentId, string Name, string? SportType, string? CountryCode, string? Gender, int Version, DateTimeOffset UpdateAt);
