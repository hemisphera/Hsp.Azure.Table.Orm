using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Xml;

namespace Hsp.Azure.Table.Orm
{

  public class TableMetadata
  {

    public const string PartitionKeyName = "PartitionKey";

    public const string RowKeyName = "RowKey";


    private static Dictionary<Type, TableMetadata> MetadataStore { get; } = new Dictionary<Type, TableMetadata>();


    public static TableMetadata Register<T>(string name, string fixedPartitionKey = null)
    {
      return Register(typeof(T), name, fixedPartitionKey);
    }

    public static TableMetadata Register(Type type, string name, string fixedPartitionKey = null)
    {
      var metadata = new TableMetadata
      {
        Name = name,
        FixedPartitionKey = fixedPartitionKey,
        RowKeyField = new TableField(RowKeyName, type.GetProperties().First(p => p.GetCustomAttribute<RowKeyAttribute>() != null)),
        PartitionKeyField = GetPartitionKeyField(type),
        Fields = type.GetProperties().Select(p =>
        {
          var attr = p.GetCustomAttribute<FieldAttribute>();
          if (attr == null) return null;
          var fm = new TableField(String.IsNullOrEmpty(attr.Name) ? p.Name : attr.Name, p);
          return fm;
        }).Where(f => f != null).ToArray()
      };

      if (metadata.PartitionKeyField == null && String.IsNullOrEmpty(metadata.FixedPartitionKey))
        throw new InvalidOperationException($"No partition key and no fixed partition key was found on type '{type}'.");
      if (metadata.RowKeyField == null)
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


    public string Name { get; private set; }

    public string FixedPartitionKey { get; private set; }


    public TableField PartitionKeyField { get; private set; }

    public TableField RowKeyField { get; private set; }

    public TableField[] Fields { get; private set; }


    private TableMetadata()
    {
    }


    public string GetPartitionKey(object item)
    {
      if (!String.IsNullOrEmpty(FixedPartitionKey))
        return FixedPartitionKey;
      return ConvertToString(PartitionKeyField.Property, item);
    }

    public void SetPartitionKey(object item, string value)
    {
      if (!String.IsNullOrEmpty(FixedPartitionKey))
        return;
      PartitionKeyField.Property.SetValue(item, ConvertFromString(PartitionKeyField.Property.PropertyType, value));
    }

    public string GetRowKey(object item)
    {
      return ConvertToString(RowKeyField.Property, item);
    }

    public void SetRowKey(object item, string value)
    {
      RowKeyField.Property.SetValue(item, ConvertFromString(RowKeyField.Property.PropertyType, value));
    }


    public static TableMetadata Get(Type type)
    {
      return MetadataStore[type];
    }

    public static TableMetadata Get(object item)
    {
      return MetadataStore[item.GetType()];
    }

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

    public TableField GetFieldByModelName(string name)
    {
      if (PartitionKeyField?.Property?.Name == name) return PartitionKeyField;
      if (RowKeyField.Property.Name == name) return RowKeyField;
      return Fields.FirstOrDefault(f => f.Property.Name == name);
    }

    public static IEnumerable<TableMetadata> List()
    {
      return MetadataStore.Values;
    }

  }

}