using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Data.Tables;

namespace Hsp.Azure.Table.Orm;

public class DictionaryStorageCache : IStorageCache
{

  readonly Dictionary<string, List<TableEntity>> _cache = new();

  readonly ConcurrentDictionary<string, SemaphoreSlim> _locks = new();


  public virtual async Task<bool> IsLoaded(TableMetadata metadata)
  {
    if (!_cache.TryGetValue(metadata.Name, out _))
      return await ValueTask.FromResult(false);
    return await ValueTask.FromResult(true);
  }

  public virtual Task Reset(TableMetadata metadata)
  {
    _cache.Remove(metadata.Name);
    return Task.CompletedTask;
  }

  public virtual Task SetItems(TableMetadata metadata, IEnumerable<TableEntity> items)
  {
    _cache.Remove(metadata.Name);
    _cache.Add(metadata.Name, new List<TableEntity>(items));
    return Task.CompletedTask;
  }

  public virtual async Task<IEnumerable<TableEntity>> GetItems(TableMetadata metadata)
  {
    if (!_cache.TryGetValue(metadata.Name, out var items))
      return await ValueTask.FromResult(Array.Empty<TableEntity>());
    return await ValueTask.FromResult(items);
  }

  public virtual Task Initialize()
  {
    return Task.CompletedTask;
  }

  public async Task LockCache(TableMetadata metadata)
  {
    var semaphore = _locks.GetOrAdd(metadata.Name, _ => new SemaphoreSlim(1, 1));
    await semaphore.WaitAsync();
  }

  public Task UnlockCache(TableMetadata metadata)
  {
    var semaphore = _locks.GetOrAdd(metadata.Name, _ => new SemaphoreSlim(1, 1));
    semaphore.Release();
    return Task.CompletedTask;
  }

}