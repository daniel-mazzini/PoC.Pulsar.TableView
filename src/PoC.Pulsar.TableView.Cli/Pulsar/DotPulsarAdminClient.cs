using Microsoft.Extensions.Options;
using PoC.Pulsar.TableView.Cli.Configuration;

namespace PoC.Pulsar.TableView.Cli.Pulsar;

internal sealed class DotPulsarAdminClient : IPulsarAdminClient, IDisposable, IAsyncDisposable
{
    private readonly HttpClient _httpClient;

    public DotPulsarAdminClient(IOptions<PulsarPublishOptions> options)
    {
        var baseUrl = options.Value.AdminUrl.TrimEnd('/');
        _httpClient = new HttpClient
        {
            BaseAddress = new Uri(baseUrl, UriKind.Absolute)
        };
    }

    public async Task TriggerCompactionAsync(string topic, CancellationToken cancellationToken)
    {
        var path = BuildCompactionPath(topic);

        using var request = new HttpRequestMessage(HttpMethod.Put, path);
        using var response = await _httpClient.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"Pulsar compaction request failed with HTTP {(int)response.StatusCode}: {body}");
        }
    }

    public ValueTask DisposeAsync()
    {
        _httpClient.Dispose();
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        _httpClient.Dispose();
    }

    private static string BuildCompactionPath(string topic)
    {
        var parts = ParseTopic(topic);
        return $"admin/v2/persistent/{Uri.EscapeDataString(parts.Tenant)}/{Uri.EscapeDataString(parts.Namespace)}/{Uri.EscapeDataString(parts.TopicName)}/compaction";
    }

    private static (string Tenant, string Namespace, string TopicName) ParseTopic(string topic)
    {
        const string prefix = "persistent://";
        if (!topic.StartsWith(prefix, StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Unsupported topic '{topic}'. Use a fully-qualified persistent topic.");
        }

        var remainder = topic[prefix.Length..];
        var firstSlash = remainder.IndexOf('/');
        var lastSlash = remainder.LastIndexOf('/');

        if (firstSlash <= 0 || lastSlash <= firstSlash || lastSlash == remainder.Length - 1)
        {
            throw new InvalidOperationException($"Unsupported topic '{topic}'. Expected persistent://tenant/namespace/topic.");
        }

        return (
            remainder[..firstSlash],
            remainder[(firstSlash + 1)..lastSlash],
            remainder[(lastSlash + 1)..]);
    }
}
