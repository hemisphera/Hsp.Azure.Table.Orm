using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure.Data.Tables;

namespace Hsp.Azure.Table.Orm
{

  public class Record<T> : IStorageTable<T> where T : class, new()
  {

    private TableMetadata Metadata { get; }

    private string ConnectionString { get; }


    public static Record<T> Create(string connectionString)
    {
      return new Record<T>(connectionString);
    }

    private List<string> Filters { get; } = new List<string>();


    private Record(string connectionString)
    {
      ConnectionString = connectionString;
      Metadata = TableMetadata.Get<T>();
    }


    public IStorageTable<T> SetRange(string name, object value, string comparison = null)
    {
      var field = Metadata.GetFieldByModelName(name);
      Filters.Add(CreateFilter(field, value, comparison));
      return this;
    }

    private static string CreateFilter(TableField field, object value, string comparison = null)
    {
      var actualValue = field.IsPartitionKey || field.IsRowKey
        ? TableMetadata.ConvertToString(value, true)
        : value;

      if (String.IsNullOrEmpty(comparison)) comparison = "eq";

      switch (actualValue)
      {
        case Guid guid:
          return FilterHelper.GenerateFilterConditionForGuid(field.StorageName, comparison, guid);
        case bool b:
          return FilterHelper.GenerateFilterConditionForBool(field.StorageName, comparison, b);
        case string str:
          return FilterHelper.GenerateFilterCondition(field.StorageName, comparison, str);
        case DateTime dt:
          return FilterHelper.GenerateFilterConditionForDate(field.StorageName, comparison, dt);
        case int iv:
          return FilterHelper.GenerateFilterConditionForInt(field.StorageName, comparison, iv);
        case double iv:
          return FilterHelper.GenerateFilterConditionForDouble(field.StorageName, comparison, iv);
        default:
          throw new NotSupportedException($"The type {value.GetType()} is not supported.");
      }
    }

    private async Task<TableEntity[]> ReadEntities()
    {
      var table = new TableClient(ConnectionString, Metadata.Name);

      var filters = Filters?.ToList() ?? new List<string>();
      if (!String.IsNullOrEmpty(Metadata.FixedPartitionKey))
        filters.Insert(0, CreateFilter(Metadata.PartitionKeyField, Metadata.FixedPartitionKey));
      var filterString =
        filters.Any()
          ? filters.Aggregate((result, current) => FilterHelper.CombineFilters(result, "and", current))
          : null;

      var items = await table.QueryAsync<TableEntity>(filterString).ToArrayAsync();
      return items.ToArray();
    }

    public async Task<T[]> Read()
    {
      var entities = await ReadEntities();
      return entities.Select(EntityConverter.FromEntity<T>).ToArray();
    }

    public async Task Store(IEnumerable<T> entities)
    {
      var table = new TableClient(ConnectionString, Metadata.Name);
      var items = entities.Select(EntityConverter.ToEntity);

      var operations = items.Select(i => new TableTransactionAction(TableTransactionActionType.UpsertReplace, i)).ToArray();
      if (!operations.Any()) return;
      await table.SubmitTransactionAsync(operations);
    }

    public async Task<int> Delete()
    {
      var table = new TableClient(ConnectionString, Metadata.Name);
      var items = await ReadEntities();
      var operations = items.Select(
        i => new TableTransactionAction(TableTransactionActionType.Delete, i)).ToArray();
      if (!operations.Any()) return 0;
      await table.SubmitTransactionAsync(operations);
      return items.Length;
    }

  }

}