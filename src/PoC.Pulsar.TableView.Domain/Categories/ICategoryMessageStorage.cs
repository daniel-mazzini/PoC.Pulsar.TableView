using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Storages.Entities;

namespace PoC.Pulsar.TableView.Domain.Categories;
public interface ICategoryMessageStorage : IMessageStorage<string, RawCategoryMessage>, IDisposable;
