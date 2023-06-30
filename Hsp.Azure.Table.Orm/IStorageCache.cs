using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Data.Tables;

namespace Hsp.Azure.Table.Orm;

public interface IStorageCache
{

  Task<bool> IsLoaded(TableMetadata metadata);

  Task Reset(TableMetadata metadata);

  Task SetItems(TableMetadata metadata, IEnumerable<TableEntity> items);

  Task<IEnumerable<TableEntity>> GetItems(TableMetadata metadata);

  Task Initialize();

  Task LockCache(TableMetadata metadata);

  Task UnlockCache(TableMetadata metadata);


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