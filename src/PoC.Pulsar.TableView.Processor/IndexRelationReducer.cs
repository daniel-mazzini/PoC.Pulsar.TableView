using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Sports;
using System.Collections.Immutable;

namespace PoC.Pulsar.TableView.Processor;

public enum IndexRelation1toManyAction { AddItem, RemoveItem, ClearKey }

public readonly record struct IndexRelationUpdateCommand(SportId SportId, CategoryId? CategoryId, IndexRelation1toManyAction Action);

public static class IndexRelationReducer
{
    public static ImmutableSortedSet<CategoryId> Reduce(ImmutableSortedSet<CategoryId>? current, IndexRelationUpdateCommand command)
    {
        var set = current ?? []; 

        return command.Action switch
        {
            IndexRelation1toManyAction.AddItem => set.Add(command.CategoryId!.Value),

            IndexRelation1toManyAction.RemoveItem => set.Remove(command.CategoryId!.Value),

            IndexRelation1toManyAction.ClearKey => [],

            _ => throw new InvalidOperationException($"Unknown action {command.Action}")
        };
    }

}



