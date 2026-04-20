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


  internal static TableField GetPartitionKeyField(Type type)
  {
    var property = type.GetProperties().FirstOrDefault(p => p.GetCustomAttribute<PartitionKeyAttribute>() != null);
    return new TableField(PartitionKeyName, property);
  }

  /// <summary>
  /// The name of the table. This is the name that will be used in the Azure Table Storage.
  /// </summary>
  public required string Name { get; init; }

  /// <summary>
  /// The type that is used to represent the table.
  /// </summary>
  public required Type EntityType { get; init; }

  /// <summary>
  /// If specified, the table will use this fixed partition key.
  /// </summary>
  public required string? FixedPartitionKey { get; init; }

  /// <summary>
  /// The field that represents the partition key.
  /// This will be null if a fixed partition key is used.
  /// </summary>
  public required TableField PartitionKeyField { get; init; }

  /// <summary>
  /// The field that represents the row key.
  /// </summary>
  public required TableField RowKeyField { get; init; }

  /// <summary>
  /// All fields of the table.
  /// </summary>
  public TableField[] Fields { get; init; } = [];


  internal TableMetadata()
  {
  }

  /// <summary>
  /// Retrieves the partition key for a given object.
  /// </summary>
  /// <param name="item">The object.</param>
  /// <returns>The partition key.</returns>
  public string? GetPartitionKey(object item)
  {
    if (!string.IsNullOrEmpty(FixedPartitionKey)) return FixedPartitionKey;
    ArgumentNullException.ThrowIfNull(PartitionKeyField.Property);
    return ConvertToString(PartitionKeyField.Property, item);
  }

  /// <summary>
  /// Sets the partition key on a given object.
  /// </summary>
  /// <param name="item">The object.</param>
  /// <param name="value">The partition key.</param>
  public void SetPartitionKey(object item, string value)
  {
    if (!string.IsNullOrEmpty(FixedPartitionKey)) return;
    ArgumentNullException.ThrowIfNull(PartitionKeyField.Property);
    PartitionKeyField.Property.SetValue(item, ConvertFromString(PartitionKeyField.Property.PropertyType, value));
  }

  /// <summary>
  /// Retrieves the row key for a given object.
  /// </summary>
  /// <param name="item">The object.</param>
  /// <returns>The row key.</returns>
  public string? GetRowKey(object item)
  {
    ArgumentNullException.ThrowIfNull(RowKeyField.Property);
    return ConvertToString(RowKeyField.Property, item);
  }

  /// <summary>
  /// Sets the row key on a given object.
  /// </summary>
  /// <param name="item">The object.</param>
  /// <param name="value">The row key.</param>
  public void SetRowKey(object item, string value)
  {
    ArgumentNullException.ThrowIfNull(RowKeyField.Property);
    RowKeyField.Property.SetValue(item, ConvertFromString(RowKeyField.Property.PropertyType, value));
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

  internal static string? ConvertToString(object? theValue, bool rethrow = false)
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

  internal static string? ConvertToString(PropertyInfo property, object item, bool rethrow = false)
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
    if (PartitionKeyField.Property?.Name == name) return PartitionKeyField;
    if (RowKeyField.Property?.Name == name) return RowKeyField;
    return Fields.FirstOrDefault(f => f.Property?.Name == name) ?? throw new FieldNotFoundException(this, name, false);
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
    return Fields.FirstOrDefault(f => f.StorageName == name) ?? throw new FieldNotFoundException(this, name, true);
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