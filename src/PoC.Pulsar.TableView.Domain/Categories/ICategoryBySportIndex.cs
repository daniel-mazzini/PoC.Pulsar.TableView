using PoC.Pulsar.TableView.Domain.Sports;

namespace PoC.Pulsar.TableView.Domain.Categories;

/// <summary>
/// Maintains lookup indexes for category relations by sport and by parent category.
/// </summary>
/// <remarks>
/// Implementations are expected to keep membership semantics only: a category is either present or absent
/// for a given relation key. Query methods return the categories currently related to the requested sport or
/// parent category.
/// </remarks>
public interface ICategoryRelationIndex
{
    /// <summary>
    /// Adds the current category relations to the index.
    /// </summary>
    /// <param name="current">The relations that must exist after indexing.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    ValueTask IndexCategoryAsync(CategoryRelations current, CancellationToken cancellationToken);

    /// <summary>
    /// Replaces a category's previously indexed relations with the current ones.
    /// </summary>
    /// <param name="previous">The relations currently stored in the index, if any.</param>
    /// <param name="current">The relations that must remain indexed after the replacement.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    ValueTask ReplaceCategoryRelationsAsync(CategoryRelations? previous, CategoryRelations current,
        CancellationToken cancellationToken);

    /// <summary>
    /// Removes the provided category relations from the index.
    /// </summary>
    /// <param name="current">The relations to remove.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    ValueTask RemoveCategoryRelationsAsync(CategoryRelations current, CancellationToken cancellationToken);

    /// <summary>
    /// Gets the categories currently indexed for the specified sport.
    /// </summary>
    /// <param name="sportId">The sport to query.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A set containing the category ids related to the sport.</returns>
    ValueTask<IReadOnlySet<CategoryId>> GetCategoriesBySportAsync(SportId sportId, CancellationToken cancellationToken);

    /// <summary>
    /// Gets the categories currently indexed for the specified parent category.
    /// </summary>
    /// <param name="parentCategoryId">The parent category to query.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A set containing the child category ids related to the parent category.</returns>
    ValueTask<IReadOnlySet<CategoryId>> GetCategoriesByParentAsync(CategoryId parentCategoryId, CancellationToken cancellationToken);

    /// <summary>
    /// Determines whether the category is currently indexed for the specified sport.
    /// </summary>
    /// <param name="sportId">The sport to query.</param>
    /// <param name="categoryId">The category to look for.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns><see langword="true"/> when the relation exists; otherwise <see langword="false"/>.</returns>
    ValueTask<bool> HasCategoryBySportAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken);

    /// <summary>
    /// Determines whether the category is currently indexed under the specified parent category.
    /// </summary>
    /// <param name="parentCategoryId">The parent category to query.</param>
    /// <param name="categoryId">The category to look for.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns><see langword="true"/> when the relation exists; otherwise <see langword="false"/>.</returns>
    ValueTask<bool> HasCategoryByParentAsync(CategoryId parentCategoryId, CategoryId categoryId, CancellationToken cancellationToken);

    /// <summary>
    /// Removes all indexed category relations.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    ValueTask ClearAsync(CancellationToken cancellationToken);
}

