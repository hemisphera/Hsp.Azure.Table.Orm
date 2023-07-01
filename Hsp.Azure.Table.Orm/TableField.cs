using System;
using System.Reflection;

namespace Hsp.Azure.Table.Orm;

/// <summary>
/// Metadata about a table field.
/// </summary>
public sealed class TableField
{

  /// <summary>
  /// The name this field has on the storage.
  /// </summary>
  public string StorageName { get; }

  /// <summary>
  /// The class property this field is mapped to.
  /// </summary>
  public PropertyInfo Property { get; }

  /// <summary>
  /// The data type this property has on the storage.
  /// </summary>
  public Type StorageType { get; }

  /// <summary>
  /// Indicats if this field is the row key.
  /// </summary>
  public bool IsRowKey => StorageName == TableMetadata.RowKeyName;

  /// <summary>
  /// Indicats if this field is the partition key.
  /// </summary>
  public bool IsPartitionKey => StorageName == TableMetadata.PartitionKeyName;

  /// <summary>
  /// </summary>
  /// <param name="storageName"></param>
  /// <param name="property"></param>
  public TableField(string storageName, PropertyInfo property)
  {
    StorageName = storageName;
    Property = property;
    StorageType = property?.PropertyType;
    if (storageName == TableMetadata.RowKeyName || storageName == TableMetadata.PartitionKeyName)
      StorageType = typeof(string);
  }

}