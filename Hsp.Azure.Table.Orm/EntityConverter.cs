using System;
using Azure.Data.Tables;

namespace Hsp.Azure.Table.Orm
{

  public static class EntityConverter
  {

    public static TableEntity ToEntity(object item)
    {
      var metadata = TableMetadata.Get(item);
      var entity = new TableEntity(metadata.GetPartitionKey(item), metadata.GetRowKey(item));
      foreach (var field in metadata.Fields)
        entity.Add(field.StorageName, field.Property.GetValue(item));
      return entity;
    }

    public static T FromEntity<T>(TableEntity entity) where T : class, new()
    {
      var metadata = TableMetadata.Get<T>();
      var item = new T();
      metadata.SetPartitionKey(item, entity.PartitionKey);
      metadata.SetRowKey(item, entity.RowKey);
      foreach (var field in metadata.Fields)
      {
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

  }

}
