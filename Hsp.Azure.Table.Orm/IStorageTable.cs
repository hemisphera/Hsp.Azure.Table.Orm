using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Hsp.Azure.Table.Orm;

public interface IStorageTable<T>
{

  /// <summary>
  /// Sets a server-side filter on the table that will be used for the next read or delete operation.
  /// </summary>
  /// <param name="field">The name of field to filter to filter for.</param>
  /// <param name="value">The value to filter for.</param>
  /// <param name="comparison">The query comparison to use when filtering.</param>
  /// <returns></returns>
  IStorageTable<T> SetRange(string field, object value, string comparison = null);

  /// <summary>
  /// Read filtered the entities from the storage.
  /// Cacheable tables are loaded from the cache and do not support server-side filtering.
  /// Allows setting a client-side filter after records have been retrieved from the storage.
  /// </summary>
  /// <param name="filter">A client-side filter that is applied to the entities after they have been retrieved from the storage.</param>
  /// <returns></returns>
  Task<T[]> Read(Predicate<T> filter = null);

  Task Store(IEnumerable<T> entities);

  Task<int> DeleteAll(Predicate<T> filter = null);

}