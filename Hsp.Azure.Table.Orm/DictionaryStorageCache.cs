using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Data.Tables;

namespace Hsp.Azure.Table.Orm;

/// <summary>
/// A simple storage cache that uses a dictionary to store entities.
/// </summary>
public class DictionaryStorageCache : IStorageCache
{

  readonly Dictionary<string, List<TableEntity>> _cache = new();

  readonly ConcurrentDictionary<string, SemaphoreSlim> _locks = new();


  /// <inheritdoc />
  public virtual async Task<bool> IsLoaded(TableMetadata metadata)
  {
    if (!_cache.TryGetValue(metadata.Name, out _))
      return await ValueTask.FromResult(false);
    return await ValueTask.FromResult(true);
  }

  /// <inheritdoc />
  public virtual Task Reset(TableMetadata metadata)
  {
    _cache.Remove(metadata.Name);
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public virtual Task SetItems(TableMetadata metadata, IEnumerable<TableEntity> items)
  {
    _cache.Remove(metadata.Name);
    _cache.Add(metadata.Name, new List<TableEntity>(items));
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public virtual async Task<IEnumerable<TableEntity>> GetItems(TableMetadata metadata)
  {
    if (!_cache.TryGetValue(metadata.Name, out var items))
      return await ValueTask.FromResult(Array.Empty<TableEntity>());
    return await ValueTask.FromResult(items);
  }

  /// <inheritdoc />
  public virtual Task Initialize()
  {
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public async Task LockCache(TableMetadata metadata)
  {
    var semaphore = _locks.GetOrAdd(metadata.Name, _ => new SemaphoreSlim(1, 1));
    await semaphore.WaitAsync();
  }

  /// <inheritdoc />
  public Task UnlockCache(TableMetadata metadata)
  {
    var semaphore = _locks.GetOrAdd(metadata.Name, _ => new SemaphoreSlim(1, 1));
    semaphore.Release();
    return Task.CompletedTask;
  }

}