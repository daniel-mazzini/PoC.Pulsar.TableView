using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Storages.Entities;

namespace PoC.Pulsar.TableView.Domain.Sports;

public interface ISportMessageStorage : IMessageStorage<string, SportMessage>;
