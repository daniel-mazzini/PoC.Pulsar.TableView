using PoC.Pulsar.TableView.Contracts;

namespace PoC.Pulsar.TableView.Domain.Storages.Entities;
public interface ICategoryMessageStorage : IMessageStorage<string, RawCategoryMessage>, IDisposable;
