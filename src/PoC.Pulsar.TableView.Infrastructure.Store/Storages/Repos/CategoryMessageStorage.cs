using System.Buffers;
using System.Text;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Filter;
using PoC.Pulsar.TableView.Domain.Projector;
using PoC.Pulsar.TableView.Domain.Storages.Messages;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos.Functions;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;

public sealed class CategoryMessageStorage : TsavoriteRepositoryBase, IMessageStorage<string, RawCategoryMessage>, IDisposable
{
    private readonly ITsavoriteSessionProvider _sessionProvider;
    private bool _disposed;
    private readonly bool _ownsSession;
    private readonly TryApplyRawCategoryMessageFunctions _tryApplyFunctions;
    private static readonly byte[] CategoryMessagePrefixBytes = Encoding.UTF8.GetBytes(StorageKey.CategoryMessagePrefix.Value);

    public CategoryMessageStorage(ITsavoriteEngine engine, IStateSerializer serializer)
        : base(serializer)
    {
        ArgumentNullException.ThrowIfNull(engine);
        _sessionProvider = new TsavoriteSessionWrapper(engine);
        _ownsSession = true;
        _tryApplyFunctions = new TryApplyRawCategoryMessageFunctions(serializer);
    }

    public CategoryMessageStorage(IStateSession session, IStateSerializer serializer)
        : base(serializer)
    {
        ArgumentNullException.ThrowIfNull(session);
        _sessionProvider = (ITsavoriteSessionProvider)session;
        _tryApplyFunctions = new TryApplyRawCategoryMessageFunctions(serializer);
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

    public async ValueTask<TableMessageApplyDecision> TryApplyAsync(RawCategoryMessage message, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        var serializedMessage = Serializer.Serialize(message);
        var session = _sessionProvider.GetSession<TryApplyRawCategoryMessageCommand, SpanByteAndMemory, TryApplyRawCategoryMessageFunctions>(_tryApplyFunctions);
        var storageKey = StorageKey.CategoryMessage(message.Id);
        var keyByteCount = storageKey.GetUtf8ByteCount();
        var valueByteCount = serializedMessage.Length;
        var rentedKeyArray = ArrayPool<byte>.Shared.Rent(keyByteCount);
        var rentedValueArray = ArrayPool<byte>.Shared.Rent(valueByteCount);

        var output = default(SpanByteAndMemory);
        try
        {
            // Write the key and value into buffer arrays so they can be pinned for the RMW operation
            storageKey.WriteUtf8Bytes(rentedKeyArray.AsSpan(0, keyByteCount));
            serializedMessage.CopyTo(rentedValueArray.AsSpan(0, valueByteCount));
            var keyMemory = rentedKeyArray.AsMemory(0, keyByteCount);
            var valueMemory = rentedValueArray.AsMemory(0, valueByteCount);
            using var pinnedKey = keyMemory.Pin();
            using var pinnedValue = valueMemory.Pin();

            var key = SpanByte.FromPinnedMemory(keyMemory);
            var value = SpanByte.FromPinnedMemory(valueMemory);
            var command = new TryApplyRawCategoryMessageCommand(value, message.Version);

            var status = session.BasicContext.RMW(ref key, ref command, ref output, Empty.Default);
            if (status.IsPending)
            {
                if (!session.BasicContext.CompletePendingWithOutputs(out CompletedOutputIterator<SpanByte, SpanByte, TryApplyRawCategoryMessageCommand, SpanByteAndMemory, Empty>? completedOutputs,
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
