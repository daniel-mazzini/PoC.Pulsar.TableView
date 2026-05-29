using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Entities.Sports;

namespace PoC.Pulsar.TableView.Domain.Storages.Entities;

public interface ISportMessageStorage : IMessageStorage<string, SportMessage>;
