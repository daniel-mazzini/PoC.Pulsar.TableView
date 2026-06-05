using PoC.Pulsar.TableView.Cli.Commands;

namespace PoC.Pulsar.TableView.Cli.Tsavorite;

internal sealed class TsavoriteCommandRunner : ITsavoriteCommandRunner
{
    private const int DefaultLimit = 100;
    private const int MaxLimit = 500;

    private readonly ITsavoriteViewerClient _viewerClient;

    public TsavoriteCommandRunner(ITsavoriteViewerClient viewerClient)
    {
        _viewerClient = viewerClient;
    }

    public async Task<int> RunAsync(TsavoriteVerb verb, CancellationToken cancellationToken)
    {
        return verb.Operation.ToLowerInvariant() switch
        {
            "list" => await RunListAsync(verb, cancellationToken),
            "get" => await RunGetAsync(verb, cancellationToken),
            _ => Fail($"Unknown tsavorite operation '{verb.Operation}'. Use 'list' or 'get'.")
        };
    }

    private async Task<int> RunListAsync(TsavoriteVerb verb, CancellationToken cancellationToken)
    {
        var limit = BoundLimit(verb.Limit);
        var watchInterval = ParseWatchInterval(verb.Watch);

        if (watchInterval is null)
        {
            await PrintListAsync(verb.Type, limit, cancellationToken);
            return 0;
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            Console.WriteLine($"[{DateTimeOffset.Now:O}] tsavorite list {verb.Type} --limit {limit}");
            await PrintListAsync(verb.Type, limit, cancellationToken);
            await Task.Delay(watchInterval.Value, cancellationToken);
        }

        return 0;
    }

    private async Task<int> RunGetAsync(TsavoriteVerb verb, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(verb.Key))
        {
            return Fail("The get operation requires --key.");
        }

        var json = await _viewerClient.GetAsync(verb.Type, verb.Key, cancellationToken);
        Console.WriteLine(json);
        return 0;
    }

    private async Task PrintListAsync(string type, int limit, CancellationToken cancellationToken)
    {
        var json = await _viewerClient.ListAsync(type, limit, cancellationToken);
        Console.WriteLine(json);
    }

    private static int BoundLimit(int limit) => Math.Clamp(limit <= 0 ? DefaultLimit : limit, 1, MaxLimit);

    private static TimeSpan? ParseWatchInterval(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var suffix = value[^1];
        var numberText = value[..^1];
        if (!int.TryParse(numberText, out var amount) || amount <= 0)
        {
            throw new InvalidOperationException($"Invalid watch interval '{value}'. Use values like 1s, 20s or 1m.");
        }

        var interval = suffix switch
        {
            's' or 'S' => TimeSpan.FromSeconds(amount),
            'm' or 'M' => TimeSpan.FromMinutes(amount),
            _ => throw new InvalidOperationException($"Invalid watch interval '{value}'. Use values like 1s, 20s or 1m.")
        };

        return interval < TimeSpan.FromSeconds(1) ? TimeSpan.FromSeconds(1) : interval;
    }

    private static int Fail(string message)
    {
        Console.Error.WriteLine(message);
        return 1;
    }
}
