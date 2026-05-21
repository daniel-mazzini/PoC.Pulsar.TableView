using System.Buffers;
using System.Text;
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
            Deserialize,
            _ => ValueTask.FromResult<PulsarMessageId?>(new PulsarMessageId(1, 3)),
            cancellationToken => ReadMessagesAsync(cancellationToken),
            logger,
            "persistent://public/default/sports");

        await view.StartBootstrapAsync();

        Assert.Equal(3, processedMessages);
        Assert.Null(view.Get("sport-1"));
        Assert.Equal("Tennis", view.Get("sport-2"));
        Assert.Single(view.GetAll());
        Assert.Contains(logger.Messages, message => message.Contains("Captured bootstrap high-watermark", StringComparison.Ordinal));
        Assert.Contains(logger.Messages, message => message.Contains("Bootstrap stopped", StringComparison.Ordinal));

        async IAsyncEnumerable<PulsarTableView<string>.BootstrapMessage> ReadMessagesAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            foreach (var message in new[]
                     {
                         CreateMessage("sport-1", "Football", new PulsarMessageId(1, 1)),
                         CreateMessage("sport-2", "Tennis", new PulsarMessageId(1, 2)),
                         CreateTombstone("sport-1", new PulsarMessageId(1, 3)),
                         CreateMessage("sport-3", "Basketball", new PulsarMessageId(1, 4))
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
            Deserialize,
            _ => ValueTask.FromResult<PulsarMessageId?>(null),
            _ =>
            {
                readInvoked = true;
                return EmptyAsync();
            },
            logger,
            "persistent://public/default/sports");

        await view.StartBootstrapAsync();

        Assert.False(readInvoked);
        Assert.Empty(view.GetAll());
        Assert.Contains(logger.Messages, message => message.Contains("No high-watermark found", StringComparison.Ordinal));

        static async IAsyncEnumerable<PulsarTableView<string>.BootstrapMessage> EmptyAsync()
        {
            await Task.Yield();
            yield break;
        }
    }

    private static string Deserialize(ReadOnlySequence<byte> data)
    {
        return Encoding.UTF8.GetString(data.ToArray());
    }

    private static PulsarTableView<string>.BootstrapMessage CreateMessage(string key, string value, PulsarMessageId messageId)
    {
        return new PulsarTableView<string>.BootstrapMessage(key, new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(value)), messageId);
    }

    private static PulsarTableView<string>.BootstrapMessage CreateTombstone(string key, PulsarMessageId messageId)
    {
        return new PulsarTableView<string>.BootstrapMessage(key, ReadOnlySequence<byte>.Empty, messageId);
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
