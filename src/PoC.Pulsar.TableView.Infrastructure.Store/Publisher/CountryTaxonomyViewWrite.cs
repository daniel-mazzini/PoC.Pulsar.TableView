using PoC.Pulsar.TableView.Contracts;
using System.Collections.Generic;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Publisher;

public sealed record CountryTaxonomyViewWrite(string Key, GeoTaxonomyViewMessage Message, IReadOnlyDictionary<string, string> Headers, DateTimeOffset Timestamp);
