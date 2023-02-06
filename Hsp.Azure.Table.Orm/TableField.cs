using System;
using System.Reflection;

namespace Hsp.Azure.Table.Orm
{

  public class TableField
  {

    public string StorageName { get; }

    public PropertyInfo Property { get; }

    public Type StorageType { get; }


    public bool IsRowKey => StorageName == TableMetadata.RowKeyName;

    public bool IsPartitionKey => StorageName == TableMetadata.PartitionKeyName;


    public TableField(string storageName, PropertyInfo property)
    {
      StorageName = storageName;
      Property = property;
      StorageType = property?.PropertyType;
      if (storageName == TableMetadata.RowKeyName || storageName == TableMetadata.PartitionKeyName)
        StorageType = typeof(string);
    }

  }

}