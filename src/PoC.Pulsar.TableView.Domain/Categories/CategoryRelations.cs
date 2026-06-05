using PoC.Pulsar.TableView.Domain.Sports;

namespace PoC.Pulsar.TableView.Domain.Categories;

/// <summary>
/// Represents the relation keys that a category contributes to the index.
/// </summary>
/// <param name="CategoryId">The category being indexed.</param>
/// <param name="SportId">The sport that owns the category.</param>
/// <param name="ParentCategoryId">The optional parent category used for hierarchical lookups.</param>
public readonly record struct CategoryRelations(CategoryId CategoryId, SportId SportId, CategoryId? ParentCategoryId);

