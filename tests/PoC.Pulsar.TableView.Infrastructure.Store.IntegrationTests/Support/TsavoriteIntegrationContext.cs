using PoC.Pulsar.TableView.Domain.Serializers;
using PoC.Pulsar.TableView.Infrastructure.Store.Serialization;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.UnitOfWorks;
using System.Text;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

internal sealed class TsavoriteIntegrationContext : IDisposable
{
    private readonly TsavoriteStoreScope _storeScope;
    private readonly List<IDisposable> _ownedSessions = [];

    public TsavoriteIntegrationContext(string testName)
    {
        _storeScope = new TsavoriteStoreScope(testName);
        StateSerializer = new MemoryPackWrapper();
        Engine = new TsavoriteEngine(_storeScope.StorePath);
        MetadataStorage = new MetadataStorage(Engine, StateSerializer);
        UnitOfWorkFactory = new UnitOfWorkFactory(Engine,
                                                 MetadataStorage,
                                                 StateSerializer,
                                                 new InMemoryGeoTaxonomyViewStorage());
    }

    public ITsavoriteEngine Engine { get; }

    public IStateSerializer StateSerializer { get; }

    public string StorePath => _storeScope.StorePath;

    public MetadataStorage MetadataStorage { get; }

    public UnitOfWorkFactory UnitOfWorkFactory { get; }

    public SportTableViewUnitOfWork CreateSportUnitOfWork()
        => new(Engine, MetadataStorage, StateSerializer);

    public RawCategoryTableViewUnitOfWork CreateCategoryUnitOfWork()
        => new(Engine, MetadataStorage, StateSerializer);

    public SportMessageStorage CreateSportMessageStorage()
        => new(new PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session.TsavoriteSessionWrapper(Engine), StateSerializer);

    public CategoryMessageStorage CreateCategoryMessageStorage()
        => new(Engine, StateSerializer);

    public CheckpointStorage CreateCheckpointStorage()
        => new(new PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session.TsavoriteSessionWrapper(Engine), StateSerializer, MetadataStorage);

    public RejectedStorage CreateRejectedStorage()
        => new(new PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session.TsavoriteSessionWrapper(Engine), StateSerializer);

    public TsavoriteCategoryRelationIndex CreateCategoryRelationIndex()
    {
        var session = new TsavoriteSessionWrapper(Engine);
        _ownedSessions.Add(session);
        return new TsavoriteCategoryRelationIndex(session, StateSerializer);
    }

    public TsavoriteCategoryPendingIndex CreateCategoryPendingIndex()
    {
        var session = new TsavoriteSessionWrapper(Engine);
        _ownedSessions.Add(session);
        return new TsavoriteCategoryPendingIndex(session, StateSerializer);
    }

    public T? ReadSingleByPrefix<T>(string prefix)
    {
        T? result = default;
        Engine.ScanByPrefix(Encoding.UTF8.GetBytes(prefix), (_, value) => result = StateSerializer.Deserialize<T>(value));
        return result;
    }

    public Dictionary<string, T> ReadAllByPrefix<T>(string prefix)
    {
        var result = new Dictionary<string, T>(StringComparer.Ordinal);
        Engine.ScanByPrefix(Encoding.UTF8.GetBytes(prefix), (key, value) =>
        {
            var deserialized = StateSerializer.Deserialize<T>(value);
            if (deserialized is not null)
            {
                result[Encoding.UTF8.GetString(key)] = deserialized;
            }
        });

        return result;
    }

    public void Dispose()
    {
        foreach (var session in _ownedSessions)
        {
            session.Dispose();
        }

        MetadataStorage.Dispose();
        Engine.Dispose();
        _storeScope.Dispose();
    }
}
