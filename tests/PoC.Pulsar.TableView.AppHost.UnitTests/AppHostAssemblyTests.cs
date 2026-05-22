using System.Reflection;
using Xunit;

namespace PoC.Pulsar.TableView.AppHost.UnitTests;

public sealed class AppHostAssemblyTests
{
    [Fact]
    public void app_host_assembly_should_be_loadable()
    {
        var assembly = Assembly.Load("PoC.Pulsar.TableView.AppHost");

        Assert.Equal("PoC.Pulsar.TableView.AppHost", assembly.GetName().Name);
    }
}
