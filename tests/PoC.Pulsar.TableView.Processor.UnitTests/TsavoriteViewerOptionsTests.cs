using PoC.Pulsar.TableView.Processor.Configuration;
using Xunit;

namespace PoC.Pulsar.TableView.Processor.UnitTests;

public sealed class TsavoriteViewerOptionsTests
{
    [Fact]
    public void is_enabled_should_return_false_when_value_is_missing()
    {
        Assert.False(TsavoriteViewerOptions.IsEnabled(null));
    }

    [Fact]
    public void is_enabled_should_return_false_when_value_is_false()
    {
        Assert.False(TsavoriteViewerOptions.IsEnabled("false"));
    }

    [Fact]
    public void is_enabled_should_return_true_when_value_is_true()
    {
        Assert.True(TsavoriteViewerOptions.IsEnabled("true"));
    }

    [Fact]
    public void is_enabled_should_ignore_bool_value_casing()
    {
        Assert.True(TsavoriteViewerOptions.IsEnabled("TRUE"));
    }
}
