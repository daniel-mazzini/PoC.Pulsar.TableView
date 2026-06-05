using Microsoft.Extensions.Options;
using PoC.Pulsar.TableView.Cli.Configuration;

namespace PoC.Pulsar.TableView.Cli.Tsavorite;

internal sealed class TsavoriteViewerClient : ITsavoriteViewerClient
{
    private readonly HttpClient _httpClient;

    public TsavoriteViewerClient(IOptions<TsavoriteViewerOptions> options)
    {
        var baseUrl = options.Value.BaseUrl.TrimEnd('/');
        _httpClient = new HttpClient
        {
            BaseAddress = new Uri(baseUrl, UriKind.Absolute)
        };
    }

    public async Task<string> ListAsync(string type, int limit, CancellationToken cancellationToken)
        => await SendAsync($"tsavorite/{Uri.EscapeDataString(type)}?limit={limit}", cancellationToken);

    public async Task<string> GetAsync(string type, string key, CancellationToken cancellationToken)
        => await SendAsync($"tsavorite/{Uri.EscapeDataString(type)}/{Uri.EscapeDataString(key)}", cancellationToken);

    private async Task<string> SendAsync(string path, CancellationToken cancellationToken)
    {
        using var response = await _httpClient.GetAsync(path, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"Tsavorite viewer request failed with HTTP {(int)response.StatusCode}: {body}");
        }

        return body;
    }
}
