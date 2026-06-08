using System.Reflection;
using Microsoft.Extensions.Options;
using PoC.Pulsar.TableView.Cli.Configuration;
using PoC.Pulsar.TableView.Cli.Pulsar;
using Xunit;

namespace PoC.Pulsar.TableView.Cli.UnitTests.Pulsar;

public sealed class DotPulsarAdminClientTests
{
    [Fact]
    public async Task trigger_compaction_async_should_put_expected_admin_path()
    {
        var handler = new RecordingHttpMessageHandler(_ => new HttpResponseMessage(System.Net.HttpStatusCode.NoContent));
        await using var client = CreateClient("http://localhost:8080", handler);

        await client.TriggerCompactionAsync("persistent://tenant-a/namespace-a/topic name", CancellationToken.None);

        Assert.Equal(HttpMethod.Put, handler.LastRequestMethod);
        Assert.Equal("/admin/v2/persistent/tenant-a/namespace-a/topic%20name/compaction", handler.LastRequestPath);
    }

    [Fact]
    public async Task trigger_compaction_async_should_throw_when_admin_request_fails()
    {
        var handler = new RecordingHttpMessageHandler(_ => new HttpResponseMessage(System.Net.HttpStatusCode.BadRequest)
        {
            Content = new StringContent("bad request")
        });
        await using var client = CreateClient("http://localhost:8080", handler);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => client.TriggerCompactionAsync("persistent://tenant-a/namespace-a/topic-a", CancellationToken.None));

        Assert.Contains("HTTP 400", exception.Message);
        Assert.Contains("bad request", exception.Message);
    }

    [Fact]
    public void dispose_should_be_safe()
    {
        var handler = new RecordingHttpMessageHandler(_ => new HttpResponseMessage(System.Net.HttpStatusCode.NoContent));
        var client = CreateClient("http://localhost:8080", handler);

        client.Dispose();
    }

    private static DotPulsarAdminClient CreateClient(string adminUrl, HttpMessageHandler handler)
    {
        var client = new DotPulsarAdminClient(Options.Create(new PulsarPublishOptions
        {
            AdminUrl = adminUrl
        }));

        var field = typeof(DotPulsarAdminClient).GetField("_httpClient", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Could not locate HttpClient field.");

        field.SetValue(client, new HttpClient(handler)
        {
            BaseAddress = new Uri(adminUrl, UriKind.Absolute)
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

        public HttpMethod? LastRequestMethod { get; private set; }

        public string? LastRequestPath { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            LastRequestMethod = request.Method;
            LastRequestPath = request.RequestUri?.PathAndQuery;
            return Task.FromResult(_responseFactory(request));
        }
    }
}
