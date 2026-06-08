using PoC.Pulsar.TableView.Cli.Commands;
using PoC.Pulsar.TableView.Cli.Tsavorite;
using Xunit;

namespace PoC.Pulsar.TableView.Cli.UnitTests.Commands;

public sealed class TsavoriteCommandRunnerTests
{
    [Fact]
    public async Task run_async_should_list_with_default_limit_when_limit_is_not_positive()
    {
        var viewerClient = new FakeTsavoriteViewerClient { ListResponse = "[]" };
        var runner = new TsavoriteCommandRunner(viewerClient);

        var result = await runner.RunAsync(new TsavoriteVerb
        {
            Operation = "list",
            Type = "sports",
            Limit = 0
        }, CancellationToken.None);

        Assert.Equal(0, result);
        Assert.Equal("sports", viewerClient.LastListType);
        Assert.Equal(100, viewerClient.LastListLimit);
        Assert.Null(viewerClient.LastGetKey);
    }

    [Fact]
    public async Task run_async_should_clamp_limit_to_maximum()
    {
        var viewerClient = new FakeTsavoriteViewerClient { ListResponse = "[]" };
        var runner = new TsavoriteCommandRunner(viewerClient);

        var result = await runner.RunAsync(new TsavoriteVerb
        {
            Operation = "list",
            Type = "categories",
            Limit = 999
        }, CancellationToken.None);

        Assert.Equal(0, result);
        Assert.Equal(500, viewerClient.LastListLimit);
    }

    [Fact]
    public async Task run_async_should_return_one_when_get_key_is_missing()
    {
        var viewerClient = new FakeTsavoriteViewerClient();
        var runner = new TsavoriteCommandRunner(viewerClient);

        var result = await runner.RunAsync(new TsavoriteVerb
        {
            Operation = "get",
            Type = "sports"
        }, CancellationToken.None);

        Assert.Equal(1, result);
        Assert.False(viewerClient.WasCalled);
    }

    [Fact]
    public async Task run_async_should_return_one_for_unknown_operation()
    {
        var viewerClient = new FakeTsavoriteViewerClient();
        var runner = new TsavoriteCommandRunner(viewerClient);

        var result = await runner.RunAsync(new TsavoriteVerb
        {
            Operation = "delete",
            Type = "sports"
        }, CancellationToken.None);

        Assert.Equal(1, result);
        Assert.False(viewerClient.WasCalled);
    }

    [Fact]
    public async Task run_async_should_return_list_output_for_get_operations()
    {
        var viewerClient = new FakeTsavoriteViewerClient { GetResponse = "{\"id\":\"sport-1\"}" };
        var runner = new TsavoriteCommandRunner(viewerClient);

        var result = await runner.RunAsync(new TsavoriteVerb
        {
            Operation = "get",
            Type = "sports",
            Key = "sport-1"
        }, CancellationToken.None);

        Assert.Equal(0, result);
        Assert.Equal("sports", viewerClient.LastGetType);
        Assert.Equal("sport-1", viewerClient.LastGetKey);
    }

    [Fact]
    public async Task run_async_should_throw_when_watch_interval_is_invalid()
    {
        var viewerClient = new FakeTsavoriteViewerClient();
        var runner = new TsavoriteCommandRunner(viewerClient);

        await Assert.ThrowsAsync<InvalidOperationException>(() => runner.RunAsync(new TsavoriteVerb
        {
            Operation = "list",
            Type = "sports",
            Watch = "10x"
        }, CancellationToken.None));
    }

    [Fact]
    public async Task run_async_should_repeat_list_when_watch_interval_is_set()
    {
        using var cancellationTokenSource = new CancellationTokenSource();
        var viewerClient = new FakeTsavoriteViewerClient
        {
            ListResponse = "[]",
            OnListCalled = () => cancellationTokenSource.Cancel()
        };
        var runner = new TsavoriteCommandRunner(viewerClient);

        await Assert.ThrowsAsync<TaskCanceledException>(() => runner.RunAsync(new TsavoriteVerb
        {
            Operation = "list",
            Type = "sports",
            Watch = "1s"
        }, cancellationTokenSource.Token));

        Assert.Equal(1, viewerClient.ListCallCount);
    }

    private sealed class FakeTsavoriteViewerClient : ITsavoriteViewerClient
    {
        public string? LastListType { get; private set; }

        public int LastListLimit { get; private set; }

        public string? LastGetType { get; private set; }

        public string? LastGetKey { get; private set; }

        public int ListCallCount { get; private set; }

        public bool WasCalled => ListCallCount > 0 || LastGetType is not null;

        public string ListResponse { get; init; } = "[]";

        public string GetResponse { get; init; } = "{}";

        public Action? OnListCalled { get; init; }

        public Task<string> ListAsync(string type, int limit, CancellationToken cancellationToken)
        {
            ListCallCount++;
            LastListType = type;
            LastListLimit = limit;
            OnListCalled?.Invoke();
            return Task.FromResult(ListResponse);
        }

        public Task<string> GetAsync(string type, string key, CancellationToken cancellationToken)
        {
            LastGetType = type;
            LastGetKey = key;
            return Task.FromResult(GetResponse);
        }
    }
}
