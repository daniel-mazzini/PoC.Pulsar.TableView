using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Filter;
using PoC.Pulsar.TableView.Domain.Serializers;
using PoC.Pulsar.TableView.Domain.Sports;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;
using System.Collections.Generic;
using System.Text;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;

public sealed class SportMessageStorage : TsavoriteRepositoryBase, ISportMessageStorage
{
    private bool _disposed;
    private readonly ITsavoriteSessionProvider _sessionProvider;

    public SportMessageStorage(IStateSession session, IStateSerializer serializer)
        : base(serializer)
    {
        _sessionProvider = (ITsavoriteSessionProvider)session;
    }

    public async ValueTask<SportMessage?> TryLoadAsync(string sportId, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var session = _sessionProvider.GetLightSession();
        return await ReadFromSessionAsync<SportMessage, SpanByte, SpanByteAndMemory, SpanByteFunctions<Empty>>(session,
                                                                                                               StorageKey.SportMessage(sportId),
                                                                                                               cancellationToken);
    }

    private static readonly byte[] SportMessagePrefixBytes = Encoding.UTF8.GetBytes(StorageKey.SportMessagePrefix.Value);

    public Dictionary<string, SportMessage> GetAll(IValuePredicate<SportMessage>? valuePredicate = null)
    {
        Dictionary<string, SportMessage> result = [];
        _sessionProvider.Engine.ScanByPrefixGetValueOnly(SportMessagePrefixBytes, (valueSpan) =>
        {
            var sportMessage = Serializer.Deserialize<SportMessage>(valueSpan);
            if (sportMessage != null)
            {
                if (valuePredicate == null || valuePredicate.Match(sportMessage))
                {
                    result.Add(sportMessage.Id, sportMessage);
                }
            }
        });

        return result;
    }
    public async ValueTask UpsertAsync(SportMessage message, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var session = _sessionProvider.GetLightSession();
        await UpsertIntoSessionAsync<SportMessage, SpanByte, SpanByteAndMemory, SpanByteFunctions<Empty>>(session,
                                                                                                            StorageKey.SportMessage(message.Id),
                                                                                                            default,
                                                                                                            message,
                                                                                                            cancellationToken);
    }

    public async ValueTask DeleteAsync(string sportId, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var session = _sessionProvider.GetLightSession();
        await DeleteFromSessionAsync<SpanByte, SpanByteAndMemory, SpanByteFunctions<Empty>>(session,
                                                                                                    StorageKey.SportMessage(sportId),
                                                                                                    cancellationToken);
    }

    public async ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        var keys = new List<string>();
        _sessionProvider.Engine.ScanByPrefix(SportMessagePrefixBytes, (key, _) => keys.Add(Encoding.UTF8.GetString(key)));

        foreach (var key in keys)
        {
            await DeleteAsync(key[StorageKey.SportMessagePrefix.Value.Length..], cancellationToken);
        }
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

        _disposed = true;
    }


}
