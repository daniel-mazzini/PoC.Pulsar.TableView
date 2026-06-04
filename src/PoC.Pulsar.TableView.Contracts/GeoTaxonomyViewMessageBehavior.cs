namespace PoC.Pulsar.TableView.Contracts;

public static class GeoTaxonomyViewMessageBehavior
{
    extension(GeoTaxonomyViewMessage view)
    {
        public GeoTaxonomyViewMessage AddOrUpdateCategory(GeoTaxonomyNode node)
        {
            var item = view.GeoCategories.FirstOrDefault(c => c.CategoryId == node.CategoryId);
            var newCategories = item is not null
                            ? view.GeoCategories.Remove(item).Add(node)
                            : view.GeoCategories.Add(node);

            return view with { GeoCategories = newCategories, Version = view.Version + 1 };
        }
    }
}




