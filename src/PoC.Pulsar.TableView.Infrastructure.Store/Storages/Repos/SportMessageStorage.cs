using System.Buffers;
using System.Text;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Filter;
using PoC.Pulsar.TableView.Domain.Projector;
using PoC.Pulsar.TableView.Domain.Storages.Entities;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;

public sealed class SportMessageStorage : TsavoriteRepositoryBase, IMessageStorage<string, SportMessage>
{
    private bool _disposed;
    private readonly ITsavoriteSessionProvider _sessionProvider;
    private readonly TryApplySportMessageFunctions _tryApplyFunctions;

    public SportMessageStorage(IStateSession session, IStateSerializer serializer)
        : base(serializer)
    {
        _sessionProvider = (ITsavoriteSessionProvider)session;
        _tryApplyFunctions = new TryApplySportMessageFunctions(serializer);
    }

    public async ValueTask<SportMessage?> TryLoadAsync(string sportId, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var session = _sessionProvider.GetLightSession();
        return await ReadFromSessionAsync<SportMessage, SpanByte, SpanByteAndMemory, SpanByteFunctions<Empty>>(session,
                                                                                                               StorageKey.SportMessage(sportId),
                                                                                                               cancellationToken);
    }

    private static readonly byte[] SportMessagePrefixBytes = Encoding.UTF8.GetBytes(StorageKey.SportMessagePrefix);

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
    public async ValueTask<TableMessageApplyDecision> TryApplyAsync(SportMessage message, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        var serializedMessage = Serializer.Serialize(message);
        var session = _sessionProvider.GetSession<TryApplySportMessageCommand, SpanByteAndMemory, TryApplySportMessageFunctions>(_tryApplyFunctions);
        var storageKey = StorageKey.SportMessage(message.Id);
        var keyByteCount = storageKey.GetUtf8ByteCount();
        var valueByteCount = serializedMessage.Length;
        var rentedKeyArray = ArrayPool<byte>.Shared.Rent(keyByteCount);
        var rentedValueArray = ArrayPool<byte>.Shared.Rent(valueByteCount);
        var output = default(SpanByteAndMemory);

        try
        {
            var written = storageKey.WriteUtf8Bytes(rentedKeyArray.AsSpan(0, keyByteCount));

            if (written != keyByteCount)
            {
                throw new InvalidOperationException($"Storage key '{storageKey.Value}' expected {keyByteCount} bytes but wrote {written}.");
            }

            serializedMessage.CopyTo(rentedValueArray.AsSpan(0, valueByteCount));
            var keyMemory = rentedKeyArray.AsMemory(0, keyByteCount);
            var valueMemory = rentedValueArray.AsMemory(0, valueByteCount);
            using var pinnedKey = keyMemory.Pin();
            using var pinnedValue = valueMemory.Pin();
            var key = SpanByte.FromPinnedMemory(keyMemory);
            var value = SpanByte.FromPinnedMemory(valueMemory);
            var command = new TryApplySportMessageCommand(value, message.Version);
            var status = session.BasicContext.RMW(ref key, ref command, ref output, Empty.Default);

            if (status.IsPending)
            {
                if (!session.BasicContext.CompletePendingWithOutputs(out CompletedOutputIterator<SpanByte, SpanByte, TryApplySportMessageCommand, SpanByteAndMemory, Empty> completedOutputs,
                                                                     wait: true,
                                                                     spinWaitForCommit: false))
                {
                    throw new InvalidOperationException($"Tsavorite RMW for '{storageKey.Value}' did not complete successfully.");
                }

                using (completedOutputs)
                {
                    while (completedOutputs.Next())
                    {
                        output = completedOutputs.Current.Output;
                    }
                }
            }
            else if (!status.IsCompletedSuccessfully)
            {
                throw new InvalidOperationException($"Tsavorite RMW for '{storageKey.Value}' failed with status '{status}'.");
            }

            if (output.AsReadOnlySpan().Length == 0 || status.Record.Created)
            {
                var persisted = await TryLoadAsync(message.Id, cancellationToken);
                if (persisted is null || persisted.Version != message.Version)
                {
                    await UpsertAsync(message, cancellationToken);
                }

                return TableMessageApplyDecision.Created();
            }

            return TryApplyOutputCodec.Deserialize(output);
        }
        finally
        {
            output.Memory?.Dispose();
            ArrayPool<byte>.Shared.Return(rentedKeyArray);
            ArrayPool<byte>.Shared.Return(rentedValueArray);
        }
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
            await DeleteAsync(key[StorageKey.SportMessagePrefix.Length..], cancellationToken);
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
