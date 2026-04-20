using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Azure.Data.Tables;

namespace Hsp.Azure.Table.Orm;

/// <summary>
/// Factory class to create record accessors.
/// </summary>
public class RecordFactory
{
  private readonly string _connectionString;
  private readonly Dictionary<Type, TableMetadata> _metadataStore = [];


  /// <summary>
  /// </summary>
  /// <param name="connectionString"></param>
  public RecordFactory(string connectionString)
  {
    _connectionString = connectionString;
  }


  /// <summary>
  /// Maps a registered entity to a TableEntity for storage.
  /// </summary>
  /// <param name="item">The entity to map.</param>
  /// <returns>>The mapped TableEntity.</returns>
  public TableEntity ModelToTableEntity(object item)
  {
    var metadata = GetMetadata(item);
    var entity = new TableEntity(metadata.GetPartitionKey(item), metadata.GetRowKey(item));
    foreach (var field in metadata.Fields)
    {
      if (field.Property == null) continue;
      var entityValue = field.Property.GetValue(item);
      if (entityValue is DateTime dt)
        entityValue = dt.ToUniversalTime();
      entity.Add(field.StorageName, entityValue);
    }

    return entity;
  }

  /// <summary>
  /// Maps a TableEntity to an instance of the given type.
  /// </summary>
  /// <typeparam name="T">The type to map to.</typeparam>
  /// <param name="entity">The TableEntity to map.</param>
  /// <returns>The mapped instance.</returns>
  public T ModelFromTableEntity<T>(TableEntity entity) where T : class, new()
  {
    var metadata = GetMetadata<T>();
    var item = new T();
    metadata.SetPartitionKey(item, entity.PartitionKey);
    metadata.SetRowKey(item, entity.RowKey);
    foreach (var field in metadata.Fields)
    {
      if (field.Property == null) continue;
      var entityValue = TryConvertEntityValue(entity[field.StorageName], field.Property.PropertyType);
      field.Property.SetValue(item, entityValue);
    }

    return item;
  }

  private static object TryConvertEntityValue(object value, Type targetType)
  {
    if (value is DateTimeOffset dto)
      if (targetType == typeof(DateTime) || targetType == typeof(DateTime?))
        value = dto.ToLocalTime().DateTime;
    return value;
  }


  /// <summary>
  /// Creates an instance for the given entity type.
  /// This entity must have his metadata registered first.
  /// </summary>
  /// <returns>The record instance.</returns>
  public Record<T> OpenRecord<T>() where T : class, new()
  {
    return new Record<T>(this);
  }

  /// <summary>
  /// Registers a in the metadata store.
  /// </summary>
  /// <typeparam name="T">The type of the table.</typeparam>
  /// <param name="name">The name of the table.</param>
  /// <param name="fixedPartitionKey">If specified, the table will use this fixed partition key.</param>
  /// <param name="create">If true, the table will be created if it does not exist. Default is false.</param>
  /// <returns>The registered metadata.</returns>
  public TableMetadata RegisterTable<T>(string name, string? fixedPartitionKey = null, bool create = false)
  {
    return RegisterTable(typeof(T), name, fixedPartitionKey, create);
  }

  /// <summary>
  /// Registers a in the metadata store.
  /// </summary>
  /// <param name="type">The type of the table.</param>
  /// <param name="name">The name of the table.</param>
  /// <param name="fixedPartitionKey">If specified, the table will use this fixed partition key.</param>
  /// <param name="create">If true, the table will be created if it does not exist. Default is false.</param>
  /// <returns>The registered metadata.</returns>
  public TableMetadata RegisterTable(Type type, string name, string? fixedPartitionKey = null, bool create = false)
  {
    var metadata = new TableMetadata
    {
      Name = name,
      EntityType = type,
      FixedPartitionKey = fixedPartitionKey,
      RowKeyField = new TableField(TableMetadata.RowKeyName, type.GetProperties().First(p => p.GetCustomAttribute<RowKeyAttribute>() != null)),
      PartitionKeyField = TableMetadata.GetPartitionKeyField(type),
      Fields = type.GetProperties().Select(p =>
      {
        var attr = p.GetCustomAttribute<FieldAttribute>();
        if (attr == null) return null;
        var fm = new TableField(string.IsNullOrEmpty(attr.Name) ? p.Name : attr.Name, p);
        return fm;
      }).OfType<TableField>().ToArray()
    };

    if (metadata.PartitionKeyField.Property == null && string.IsNullOrEmpty(metadata.FixedPartitionKey))
      throw new InvalidOperationException($"No partition key and no fixed partition key was found on type '{type}'.");
    if (metadata.RowKeyField.Property == null)
      throw new InvalidOperationException($"No row key was found on type '{type}'.");

    _metadataStore.Add(type, metadata);

    if (create)
    {
      CreateTableClient(metadata).CreateIfNotExists();
    }

    return metadata;
  }


  /// <summary>
  /// Retrieves metadata for the given type.
  /// </summary>
  /// <param name="type">The type.</param>
  /// <returns>The metadata.</returns>
  public TableMetadata GetMetadata(Type type)
  {
    return _metadataStore[type];
  }

  /// <summary>
  /// Retrieves metadata for the type the given item is of.
  /// </summary>
  /// <param name="item">The item.</param>
  /// <returns>The metadata.</returns>
  public TableMetadata GetMetadata(object item)
  {
    return _metadataStore[item.GetType()];
  }

  /// <summary>
  /// Retrieves metadata for a table with the given name.
  /// </summary>
  /// <param name="tableName">The name of the table.</param>
  /// <returns>The metadata.</returns>
  public TableMetadata GetMetadataByName(string tableName)
  {
    return _metadataStore.Values.First(item => item.Name == tableName);
  }

  /// <summary>
  /// Retrieves metadata for the given type.
  /// </summary>
  /// <typeparam name="T">The type of the table.</typeparam>
  /// <returns>The metadata.</returns>
  public TableMetadata GetMetadata<T>()
  {
    return _metadataStore[typeof(T)];
  }

  internal TableClient CreateTableClient(TableMetadata metadata)
  {
    return new TableClient(_connectionString, metadata.Name);
  }
}