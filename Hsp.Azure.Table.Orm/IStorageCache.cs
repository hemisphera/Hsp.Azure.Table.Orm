using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Data.Tables;

namespace Hsp.Azure.Table.Orm;

/// <summary>
/// An interface for classes that implement caching entities.
/// </summary>
public interface IStorageCache
{

  /// <summary>
  /// Indicates if a given table is loaded in the cache.
  /// </summary>
  /// <param name="metadata">The table to look for.</param>
  /// <returns>true or false</returns>
  Task<bool> IsLoaded(TableMetadata metadata);

  /// <summary>
  /// Deletes the cache for the given table.
  /// </summary>
  /// <param name="metadata">The table to remove from the cache.</param>
  Task Reset(TableMetadata metadata);

  /// <summary>
  /// Sets the cache contents for a given table.
  /// </summary>
  /// <param name="metadata">The table to set the cache for.</param>
  /// <param name="items">The items to place into the cache.</param>
  Task SetItems(TableMetadata metadata, IEnumerable<TableEntity> items);

  /// <summary>
  /// Reads the cache contents for a given table.
  /// </summary>
  /// <param name="metadata">The table to read the cache contents for.</param>
  /// <returns>The items</returns>
  Task<IEnumerable<TableEntity>> GetItems(TableMetadata metadata);

  /// <summary>
  /// Initializes the cache.
  /// </summary>
  Task Initialize();

  /// <summary>
  /// Locks the cache for a given table for cross-thread operations.
  /// </summary>
  /// <param name="metadata">The table to lock.</param>
  Task LockCache(TableMetadata metadata);

  /// <summary>
  /// Unlocks the cache for a given table for cross-thread operations.
  /// </summary>
  /// <param name="metadata">The table to unlock.</param>
  Task UnlockCache(TableMetadata metadata);


  /// <summary>
  /// Load the the cache for a given table.
  /// </summary>
  /// <param name="metadata">The table to load the cache for.</param>
  /// <param name="loadFunc">A loader function that provides the items to be placed into the cache.</param>
  /// <returns></returns>
  public async Task LoadCache(TableMetadata metadata, Func<TableMetadata, Task<TableEntity[]>> loadFunc)
  {
    try
    {
      await LockCache(metadata);
      if (await IsLoaded(metadata)) return;
      await SetItems(metadata, await loadFunc(metadata));
    }
    finally
    {
      await UnlockCache(metadata);
    }
  }

}