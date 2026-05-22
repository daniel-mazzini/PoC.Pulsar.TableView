# Pulsar TableView PoC

## Run the Aspire environment

Start the local infrastructure with:

```powershell
dotnet run --project .\src\PoC.Pulsar.TableView.AppHost\PoC.Pulsar.TableView.AppHost.csproj
```

Aspire will start:

- Apache Pulsar standalone on `6650` and `8080`
- the initialization job that creates `public/tableview-inputs` and `public/tableview-outputs`
- Dekaf on `8090`
- the `cli` seeding executable after the Pulsar init job completes

## Verify Pulsar and the topics

After Aspire is up, run:

```powershell
curl.exe http://localhost:8080/admin/v2/persistent/public/tableview-outputs
```

That should return the three topics:

- `persistent://public/tableview-inputs/sports`
- `persistent://public/tableview-inputs/categories`
- `persistent://public/tableview-outputs/taxonomy-view`

You can also open Dekaf at:

```powershell
http://localhost:8090
```

## Seed Pulsar with sample data

Run the CLI directly:

```powershell
$env:Pulsar__ServiceUrl = "pulsar://localhost:6650"
$env:Pulsar__InputNamespace = "public/tableview-inputs"
dotnet run --project .\src\PoC.Pulsar.TableView.Cli\PoC.Pulsar.TableView.Cli.csproj -- publish-sample
```

Use `--help` if you want to see the available command shape.

Or start Aspire and let the `cli` executable run after Pulsar is ready:

```powershell
dotnet run --project .\src\PoC.Pulsar.TableView.AppHost\PoC.Pulsar.TableView.AppHost.csproj
```

The CLI reads the JSON files from `samples\publish`, writes Avro payloads to the input topics, and republishes each sample three times with the same key and increasing `Version` values.

## Run serialization benchmarks

The repository includes BenchmarkDotNet benchmarks for the serialization paths in `DefaultAvroSerializer<T>`:

- `Serialize(T obj)`, which returns a `byte[]`
- `Serialize(T obj, PipeWriter writer)`, used by the processor publish flow
- `Serialize(T obj, Stream stream)`, used with a rented `ArrayPool<byte>` buffer
- `Serialize(T obj, Stream stream)`, used with `RecyclableMemoryStream.GetReadOnlySequence()`

Build the benchmark project in Release mode:

```powershell
dotnet build .\benchmarks\PoC.Pulsar.TableView.Processor.Benchmarks\PoC.Pulsar.TableView.Processor.Benchmarks.csproj -c Release
```

Run all benchmarks:

```powershell
dotnet run -c Release --project .\benchmarks\PoC.Pulsar.TableView.Processor.Benchmarks\PoC.Pulsar.TableView.Processor.Benchmarks.csproj
```

To run only the Avro serialization benchmarks:

```powershell
dotnet run -c Release --project .\benchmarks\PoC.Pulsar.TableView.Processor.Benchmarks\PoC.Pulsar.TableView.Processor.Benchmarks.csproj -- --filter *AvroSerializationBenchmarks*
```

The benchmark output includes:

- execution time
- allocated bytes per operation
- Gen0, Gen1, and Gen2 collections
- comparison between the `byte[]`, `PipeWriter`, `Stream` with `ArrayPool<byte>`, and `RecyclableMemoryStream` paths
- stress scenarios using different `GeoTaxonomyMessage` category counts

BenchmarkDotNet writes detailed reports under:

```powershell
.\BenchmarkDotNet.Artifacts\
```

For reliable numbers:

- close unnecessary applications before running
- use `-c Release`
- avoid debugging while benchmarks run
- compare results from the same machine
