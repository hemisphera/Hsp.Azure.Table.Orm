using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.Xml;
using Azure.Data.Tables;

namespace Hsp.Azure.Table.Orm;

/// <summary>
/// Metadata of a table.
/// </summary>
public class TableMetadata
{

  /// <summary>
  /// The name of the partition key field.
  /// </summary>
  public const string PartitionKeyName = "PartitionKey";

  /// <summary>
  /// The name of the row key field.
  /// </summary>
  public const string RowKeyName = "RowKey";


  private static Dictionary<Type, TableMetadata> MetadataStore { get; } = new Dictionary<Type, TableMetadata>();

  /// <summary>
  /// Registers a in the metadata store.
  /// </summary>
  /// <typeparam name="T">The type of the table.</typeparam>
  /// <param name="name">The name of the table.</param>
  /// <param name="fixedPartitionKey">If specified, the table will use this fixed partition key.</param>
  /// <returns>The registered metadata.</returns>
  public static TableMetadata Register<T>(string name, string fixedPartitionKey = null)
  {
    return Register(typeof(T), name, fixedPartitionKey);
  }

  /// <summary>
  /// Registers a in the metadata store.
  /// </summary>
  /// <param name="type">The type of the table.</param>
  /// <param name="name">The name of the table.</param>
  /// <param name="fixedPartitionKey">If specified, the table will use this fixed partition key.</param>
  /// <returns>The registered metadata.</returns>
  public static TableMetadata Register(Type type, string name, string fixedPartitionKey = null)
  {
    var metadata = new TableMetadata
    {
      Name = name,
      EntityType = type,
      FixedPartitionKey = fixedPartitionKey,
      RowKeyField = new TableField(RowKeyName, type.GetProperties().First(p => p.GetCustomAttribute<RowKeyAttribute>() != null)),
      PartitionKeyField = GetPartitionKeyField(type),
      IsCached = type.GetCustomAttribute<CacheableAttribute>() != null,
      Fields = type.GetProperties().Select(p =>
      {
        var attr = p.GetCustomAttribute<FieldAttribute>();
        if (attr == null) return null;
        var fm = new TableField(String.IsNullOrEmpty(attr.Name) ? p.Name : attr.Name, p);
        return fm;
      }).Where(f => f != null).ToArray()
    };

    if (metadata.PartitionKeyField?.Property == null && String.IsNullOrEmpty(metadata.FixedPartitionKey))
      throw new InvalidOperationException($"No partition key and no fixed partition key was found on type '{type}'.");
    if (metadata.RowKeyField?.Property == null)
      throw new InvalidOperationException($"No row key was found on type '{type}'.");

    MetadataStore.Add(type, metadata);
    return metadata;
  }

  private static TableField GetPartitionKeyField(Type type)
  {
    var property = type.GetProperties().FirstOrDefault(p => p.GetCustomAttribute<PartitionKeyAttribute>() != null);
    //if (property == null) return null;
    return new TableField(PartitionKeyName, property);
  }

  /// <summary>
  /// The name of the table. This is the name that will be used in the Azure Table Storage.
  /// </summary>
  public string Name { get; private init; }

  /// <summary>
  /// The type that is used to represent the table.
  /// </summary>
  public Type EntityType { get; init; }

  /// <summary>
  /// If specified, the table will use this fixed partition key.
  /// </summary>
  public string FixedPartitionKey { get; private init; }

  /// <summary>
  /// The field that represents the partition key.
  /// </summary>
  public TableField PartitionKeyField { get; private init; }

  /// <summary>
  /// The field that represents the row key.
  /// </summary>
  public TableField RowKeyField { get; private init; }

  /// <summary>
  /// All fields of the table.
  /// </summary>
  public TableField[] Fields { get; private init; }

  /// <summary>
  /// Specifies if the table is cached.
  /// </summary>
  public bool IsCached { get; private init; }


  private TableMetadata()
  {
  }

  /// <summary>
  /// Retrieves the partition key for a given object.
  /// </summary>
  /// <param name="item">The object.</param>
  /// <returns>The partition key.</returns>
  public string GetPartitionKey(object item)
  {
    if (!String.IsNullOrEmpty(FixedPartitionKey))
      return FixedPartitionKey;
    return ConvertToString(PartitionKeyField.Property, item);
  }

  /// <summary>
  /// Sets the partition key on a given object.
  /// </summary>
  /// <param name="item">The object.</param>
  /// <param name="value">The partition key.</param>
  public void SetPartitionKey(object item, string value)
  {
    if (!String.IsNullOrEmpty(FixedPartitionKey))
      return;
    PartitionKeyField.Property.SetValue(item, ConvertFromString(PartitionKeyField.Property.PropertyType, value));
  }

  /// <summary>
  /// Retrieves the row key for a given object.
  /// </summary>
  /// <param name="item">The object.</param>
  /// <returns>The row key.</returns>
  public string GetRowKey(object item)
  {
    return ConvertToString(RowKeyField.Property, item);
  }

  /// <summary>
  /// Sets the row key on a given object.
  /// </summary>
  /// <param name="item">The object.</param>
  /// <param name="value">The row key.</param>
  public void SetRowKey(object item, string value)
  {
    RowKeyField.Property.SetValue(item, ConvertFromString(RowKeyField.Property.PropertyType, value));
  }


  /// <summary>
  /// Retrieves metadata for the given type.
  /// </summary>
  /// <param name="type">The type.</param>
  /// <returns>The metadata.</returns>
  public static TableMetadata Get(Type type)
  {
    return MetadataStore[type];
  }

  /// <summary>
  /// Retrieves metadata for the type the given item is of.
  /// </summary>
  /// <param name="item">The item.</param>
  /// <returns>The metadata.</returns>
  public static TableMetadata Get(object item)
  {
    return MetadataStore[item.GetType()];
  }

  /// <summary>
  /// Retrieves metadata for a table with the given name.
  /// </summary>
  /// <param name="tableName">The name of the table.</param>
  /// <returns>The metadata.</returns>
  public static TableMetadata GetByName(string tableName)
  {
    return MetadataStore.Values.FirstOrDefault(item => item.Name == tableName);
  }

  /// <summary>
  /// Retrieves metadata for the given type.
  /// </summary>
  /// <typeparam name="T">The type of the table.</typeparam>
  /// <returns>The metadata.</returns>
  public static TableMetadata Get<T>()
  {
    return MetadataStore[typeof(T)];
  }


  internal static object ConvertFromString(Type targetType, string stringValue, bool rethrow = false)
  {
    if (targetType == typeof(double)) return TryConvert(() => XmlConvert.ToDouble(stringValue), 0, rethrow);
    if (targetType == typeof(int)) return TryConvert(() => XmlConvert.ToInt32(stringValue), 0, rethrow);
    if (targetType == typeof(DateTime)) return TryConvert(() => XmlConvert.ToDateTime(stringValue, XmlDateTimeSerializationMode.Utc), DateTime.MinValue, rethrow);
    if (targetType == typeof(bool)) return TryConvert(() => XmlConvert.ToBoolean(stringValue), false, rethrow);
    if (targetType == typeof(Guid)) return TryConvert(() => XmlConvert.ToGuid(stringValue), Guid.Empty, rethrow);
    if (targetType == typeof(string)) return stringValue;
    throw new NotSupportedException($"The type '{targetType}' is not supported.");
  }

  internal static string ConvertToString(object theValue, bool rethrow = false)
  {
    if (theValue == null) return null;
    if (theValue is double) return TryConvert(() => XmlConvert.ToString((double)theValue), XmlConvert.ToString((double)0), rethrow);
    if (theValue is int) return TryConvert(() => XmlConvert.ToString((int)theValue), XmlConvert.ToString(0), rethrow);
    if (theValue is DateTime) return TryConvert(() => XmlConvert.ToString((DateTime)theValue, XmlDateTimeSerializationMode.Utc), XmlConvert.ToString(DateTime.MinValue, XmlDateTimeSerializationMode.Utc), rethrow);
    if (theValue is bool) return TryConvert(() => XmlConvert.ToString((bool)theValue), XmlConvert.ToString(false), rethrow);
    if (theValue is Guid) return TryConvert(() => XmlConvert.ToString((Guid)theValue), XmlConvert.ToString(Guid.Empty), rethrow);
    if (theValue is string value) return value;
    throw new NotSupportedException($"The type '{theValue.GetType()}' is not supported.");
  }

  internal static string ConvertToString(PropertyInfo property, object item, bool rethrow = false)
  {
    return ConvertToString(property.GetValue(item), rethrow);
  }

  private static T TryConvert<T>(Func<T> func, T defaultValue, bool rethrow)
  {
    try
    {
      return func();
    }
    catch
    {
      if (rethrow) throw;
      return defaultValue;
    }
  }

  /// <summary>
  /// Returns a field on the table, by the name of the property on the model.
  /// </summary>
  /// <param name="name">The property name</param>
  /// <returns>The field.</returns>
  public TableField GetFieldByModelName(string name)
  {
    if (PartitionKeyField?.Property?.Name == name) return PartitionKeyField;
    if (RowKeyField.Property.Name == name) return RowKeyField;
    return Fields.FirstOrDefault(f => f.Property.Name == name);
  }

  /// <summary>
  /// Returns a field on the table, by the name of the field on the storage.
  /// </summary>
  /// <param name="name">The storage field name</param>
  /// <returns>The field.</returns>
  public TableField GetFieldByStorageName(string name)
  {
    if (name == PartitionKeyName) return PartitionKeyField;
    if (name == RowKeyName) return RowKeyField;
    return Fields.FirstOrDefault(f => f.StorageName == name);
  }

  /// <summary>
  /// Lists all metadata items that were registered.
  /// </summary>
  /// <returns></returns>
  public static IEnumerable<TableMetadata> List()
  {
    return MetadataStore.Values;
  }

  /// <summary>
  /// Creates the table on the storage, if it does not exist.
  /// </summary>
  /// <param name="connectionString">The connection string to the storage account.</param>
  public async Task CreateTable(string connectionString)
  {
    var cl = new TableClient(connectionString, Name);
    await cl.CreateIfNotExistsAsync();
  }

}