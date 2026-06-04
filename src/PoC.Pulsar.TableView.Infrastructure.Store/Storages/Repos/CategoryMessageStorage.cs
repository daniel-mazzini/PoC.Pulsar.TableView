using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Filter;
using PoC.Pulsar.TableView.Domain.Serializers;
using PoC.Pulsar.TableView.Domain.Storages.Entities;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;
using System.Collections.Generic;
using System.Text;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;

public sealed class CategoryMessageStorage : TsavoriteRepositoryBase, IMessageStorage<string, RawCategoryMessage>, IDisposable
{
    private readonly ITsavoriteSessionProvider _sessionProvider;
    private bool _disposed;
    private readonly bool _ownsSession;
    private static readonly byte[] CategoryMessagePrefixBytes = Encoding.UTF8.GetBytes(StorageKey.CategoryMessagePrefix.Value);

    public CategoryMessageStorage(ITsavoriteEngine engine, IStateSerializer serializer)
        : base(serializer)
    {
        ArgumentNullException.ThrowIfNull(engine);
        _sessionProvider = new TsavoriteSessionWrapper(engine);
        _ownsSession = true;
    }

    public CategoryMessageStorage(IStateSession session, IStateSerializer serializer)
        : base(serializer)
    {
        ArgumentNullException.ThrowIfNull(session);
        _sessionProvider = (ITsavoriteSessionProvider)session;
    }

    public async ValueTask DeleteAsync(string id, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var session = _sessionProvider.GetLightSession();
        await DeleteFromSessionAsync(session,
                                      StorageKey.CategoryMessage(id),
                                      cancellationToken);
    }

    public async ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        var keys = new List<string>();
        _sessionProvider.Engine.ScanByPrefix(CategoryMessagePrefixBytes, (key, _) => keys.Add(Encoding.UTF8.GetString(key)));

        foreach (var key in keys)
        {
            await DeleteAsync(key[StorageKey.CategoryMessagePrefix.Value.Length..], cancellationToken);
        }
    }


    public async ValueTask<RawCategoryMessage?> TryLoadAsync(string id, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var session = _sessionProvider.GetLightSession();
        return await ReadFromSessionAsync<RawCategoryMessage, SpanByte, SpanByteAndMemory, SpanByteFunctions<Empty>>(session,
                                                                                                                      StorageKey.CategoryMessage(id),
                                                                                                                      cancellationToken);
    }

    public Dictionary<string, RawCategoryMessage> GetAll(IValuePredicate<RawCategoryMessage>? valuePredicate = null)
    {
        ThrowIfDisposed();

        Dictionary<string, RawCategoryMessage> result = [];
        _sessionProvider.Engine.ScanByPrefix(CategoryMessagePrefixBytes, (key, valueSpan) =>
        {
            var message = Serializer.Deserialize<RawCategoryMessage>(valueSpan);
            if (message is not null && (valuePredicate is null || valuePredicate.Match(message)))
            {
                result[message.Id] = message;
            }
        });

        return result;
    }

    public async ValueTask UpsertAsync(RawCategoryMessage message, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var session = _sessionProvider.GetLightSession();
        await UpsertIntoSessionAsync(session,
                                     StorageKey.CategoryMessage(message.Id),
                                     default,
                                     message,
                                     cancellationToken);
    }
    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        if (_ownsSession)
        {
            _sessionProvider.Dispose();
        }
        _disposed = true;
    }
}
