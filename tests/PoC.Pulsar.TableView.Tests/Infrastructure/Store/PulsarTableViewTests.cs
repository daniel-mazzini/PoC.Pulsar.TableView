using System.Buffers;
using System.Text;
using DotPulsar;
using Microsoft.Extensions.Logging;
using PoC.Pulsar.TableView.Infrastructure.Store;
using Xunit;

namespace PoC.Pulsar.TableView.Tests.Infrastructure.Store;

public sealed class PulsarTableViewTests
{
    [Fact]
    public async Task start_bootstrap_async_should_apply_messages_until_high_watermark_and_stop()
    {
        var store = new InMemoryStateStore<string, string>();
        var logger = new ListLogger<PulsarTableView<string>>();
        var processedMessages = 0;

        var view = new PulsarTableView<string>(
            store,
            "persistent://public/default/sports",
            Deserialize,
            _ => ValueTask.FromResult<IReadOnlyDictionary<int, PulsarMessageId>>(new Dictionary<int, PulsarMessageId>
            {
                [0] = new(1, 3, 0)
            }),
            cancellationToken => ReadMessagesAsync(cancellationToken),
            (_, cancellationToken) => EmptyAsync(cancellationToken),
            logger);

        await view.StartBootstrapAsync();

        Assert.Equal(3, processedMessages);
        Assert.Null(view.Get("sport-1"));
        Assert.Equal("Tennis", view.Get("sport-2"));
        Assert.Single(await ToListAsync(view.GetAllAsync()));
        Assert.Equal(new PulsarMessageId(1, 3, 0), store.GetLastCheckpoint());
        Assert.Contains(logger.Messages, message => message.Contains("Starting bootstrap", StringComparison.Ordinal));
        Assert.Contains(logger.Messages, message => message.Contains("Bootstrap successfully completed", StringComparison.Ordinal));

        async IAsyncEnumerable<TableViewMessage> ReadMessagesAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            foreach (var message in new[]
                     {
                         CreateMessage("sport-1", "Football", new PulsarMessageId(1, 1, 0)),
                         CreateMessage("sport-2", "Tennis", new PulsarMessageId(1, 2, 0)),
                         CreateTombstone("sport-1", new PulsarMessageId(1, 3, 0)),
                         CreateMessage("sport-3", "Basketball", new PulsarMessageId(1, 4, 0))
                     })
            {
                cancellationToken.ThrowIfCancellationRequested();
                processedMessages++;
                yield return message;
                await Task.Yield();
            }
        }
    }

    [Fact]
    public async Task start_bootstrap_async_should_complete_without_reading_when_topic_has_no_messages()
    {
        var store = new InMemoryStateStore<string, string>();
        var logger = new ListLogger<PulsarTableView<string>>();
        var readInvoked = false;

        var view = new PulsarTableView<string>(
            store,
            "persistent://public/default/sports",
            Deserialize,
            _ => ValueTask.FromResult<IReadOnlyDictionary<int, PulsarMessageId>>(new Dictionary<int, PulsarMessageId>()),
            _ =>
            {
                readInvoked = true;
                return EmptyBootstrapAsync();
            },
            (_, cancellationToken) => EmptyAsync(cancellationToken),
            logger);

        await view.StartBootstrapAsync();

        Assert.False(readInvoked);
        Assert.Empty(await ToListAsync(view.GetAllAsync()));
        Assert.Contains(logger.Messages, message => message.Contains("No messages found", StringComparison.Ordinal));

        static async IAsyncEnumerable<TableViewMessage> EmptyBootstrapAsync()
        {
            await Task.Yield();
            yield break;
        }
    }

    [Fact]
    public async Task start_live_tail_async_should_apply_changes_and_emit_events()
    {
        var store = new InMemoryStateStore<string, string>();
        var logger = new ListLogger<PulsarTableView<string>>();
        var events = new List<Event<string>>();
        MessageId? capturedStartMessageId = null;

        var view = new PulsarTableView<string>(
            store,
            "persistent://public/default/sports",
            Deserialize,
            _ => ValueTask.FromResult<IReadOnlyDictionary<int, PulsarMessageId>>(new Dictionary<int, PulsarMessageId>
            {
                [0] = new(7, 11, 0)
            }),
            cancellationToken => BootstrapMessagesAsync(cancellationToken),
            (startMessageId, cancellationToken) => LiveMessagesAsync(startMessageId, cancellationToken),
            logger);

        using var subscription = view.OnUpdate.Subscribe(@event => events.Add(@event));

        await view.StartBootstrapAsync();
        await view.StartLiveTailAsync(CancellationToken.None);

        Assert.NotNull(capturedStartMessageId);
        Assert.Equal((ulong)7, capturedStartMessageId!.LedgerId);
        Assert.Equal((ulong)11, capturedStartMessageId.EntryId);
        Assert.Equal("Volleyball", view.Get("sport-2"));
        Assert.Null(view.Get("sport-1"));
        Assert.Equal(new PulsarMessageId(7, 13, 0), store.GetLastCheckpoint());

        var update = Assert.IsType<UpdateEvent<string>>(events[0]);
        Assert.Equal("sport-2", update.Key);
        Assert.Equal("Volleyball", update.NewValue);

        var delete = Assert.IsType<DeleteEvent<string>>(events[1]);
        Assert.Equal("sport-1", delete.Key);

        async IAsyncEnumerable<TableViewMessage> BootstrapMessagesAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return CreateMessage("sport-1", "Football", new PulsarMessageId(7, 11, 0));
            await Task.Yield();
        }

        async IAsyncEnumerable<TableViewMessage> LiveMessagesAsync(
            MessageId startMessageId,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            capturedStartMessageId = startMessageId;
            cancellationToken.ThrowIfCancellationRequested();
            yield return CreateMessage("sport-2", "Volleyball", new PulsarMessageId(7, 12, 0));
            await Task.Yield();
            yield return CreateTombstone("sport-1", new PulsarMessageId(7, 13, 0));
        }
    }

    private static string Deserialize(ReadOnlySequence<byte> data)
    {
        return Encoding.UTF8.GetString(data.ToArray());
    }

    private static TableViewMessage CreateMessage(string key, string value, PulsarMessageId messageId)
    {
        return new TableViewMessage(key, new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(value)), messageId);
    }

    private static TableViewMessage CreateTombstone(string key, PulsarMessageId messageId)
    {
        return new TableViewMessage(key, ReadOnlySequence<byte>.Empty, messageId);
    }

    private static async IAsyncEnumerable<TableViewMessage> EmptyAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await Task.Yield();
        yield break;
    }

    private static async Task<List<T>> ToListAsync<T>(IAsyncEnumerable<T> source)
    {
        var values = new List<T>();

        await foreach (var value in source)
        {
            values.Add(value);
        }

        return values;
    }

    private sealed class ListLogger<T> : ILogger<T>
    {
        public List<string> Messages { get; } = new();

        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull
        {
            return NullScope.Instance;
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            Messages.Add(formatter(state, exception));
        }

        private sealed class NullScope : IDisposable
        {
            public static NullScope Instance { get; } = new();

            public void Dispose()
            {
            }
        }
    }
}
