using Microsoft.Extensions.DependencyInjection;
using PoC.Pulsar.TableView.Cli.Commands;
using PoC.Pulsar.TableView.Cli.Hosting;

using var host = CliHost.Create(args);

return host.Services
    .GetRequiredService<PublishSampleApplication>()
    .Run(args);
