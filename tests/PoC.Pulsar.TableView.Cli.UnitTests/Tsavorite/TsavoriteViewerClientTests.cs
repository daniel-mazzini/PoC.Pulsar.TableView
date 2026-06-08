using System.Reflection;
using Microsoft.Extensions.Options;
using PoC.Pulsar.TableView.Cli.Configuration;
using PoC.Pulsar.TableView.Cli.Tsavorite;
using Xunit;

namespace PoC.Pulsar.TableView.Cli.UnitTests.Tsavorite;

public sealed class TsavoriteViewerClientTests
{
    [Fact]
    public async Task list_async_should_call_expected_path()
    {
        var handler = new RecordingHttpMessageHandler(_ => new HttpResponseMessage(System.Net.HttpStatusCode.OK)
        {
            Content = new StringContent("[]")
        });
        var client = CreateClient("http://127.0.0.1:18080", handler);

        var result = await client.ListAsync("sports type", 25, CancellationToken.None);

        Assert.Equal("[]", result);
        Assert.Equal("/tsavorite/sports%20type?limit=25", handler.LastRequestPath);
    }

    [Fact]
    public async Task get_async_should_throw_when_viewer_request_fails()
    {
        var handler = new RecordingHttpMessageHandler(_ => new HttpResponseMessage(System.Net.HttpStatusCode.InternalServerError)
        {
            Content = new StringContent("boom")
        });
        var client = CreateClient("http://127.0.0.1:18080", handler);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => client.GetAsync("sports type", "sport/1", CancellationToken.None));

        Assert.Contains("HTTP 500", exception.Message);
        Assert.Contains("boom", exception.Message);
        Assert.Equal("/tsavorite/sports%20type/sport%2F1", handler.LastRequestPath);
    }

    private static TsavoriteViewerClient CreateClient(string baseUrl, HttpMessageHandler handler)
    {
        var client = new TsavoriteViewerClient(Options.Create(new TsavoriteViewerOptions
        {
            BaseUrl = baseUrl
        }));

        var field = typeof(TsavoriteViewerClient).GetField("_httpClient", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Could not locate HttpClient field.");

        field.SetValue(client, new HttpClient(handler)
        {
            BaseAddress = new Uri(baseUrl, UriKind.Absolute)
        });

        return client;
    }

    private sealed class RecordingHttpMessageHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, HttpResponseMessage> _responseFactory;

        public RecordingHttpMessageHandler(Func<HttpRequestMessage, HttpResponseMessage> responseFactory)
        {
            _responseFactory = responseFactory;
        }

        public string? LastRequestPath { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            LastRequestPath = request.RequestUri?.PathAndQuery;
            return Task.FromResult(_responseFactory(request));
        }
    }
}
