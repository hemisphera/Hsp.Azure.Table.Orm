using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Data.Tables;

namespace Hsp.Azure.Table.Orm;

public class Record<T> : IStorageTable<T> where T : class, new()
{

  private TableMetadata Metadata { get; }

  private string ConnectionString { get; }

  private IStorageCache Cache { get; init; }


  public static Record<T> Create(string connectionString, IStorageCache cache = null)
  {
    return new Record<T>(connectionString)
    {
      Cache = cache
    };
  }

  private List<string> Filters { get; } = new List<string>();


  private Record(string connectionString)
  {
    ConnectionString = connectionString;
    Metadata = TableMetadata.Get<T>();
  }

  /// <inheritdoc />>
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

  private async Task<TableEntity[]> ReadEntities(bool pointersOnly = false)
  {
    var useCache = Metadata.IsCached && Cache != null;

    var filters = Filters?.ToList() ?? new List<string>();
    if (!String.IsNullOrEmpty(Metadata.FixedPartitionKey) && !useCache)
      filters.Insert(0, CreateFilter(Metadata.PartitionKeyField, Metadata.FixedPartitionKey));
    var filterString =
      filters.Any()
        ? filters.Aggregate((result, current) => FilterHelper.CombineFilters(result, "and", current))
        : null;

    var loadFields = pointersOnly
      ? new[] { TableMetadata.PartitionKeyName, TableMetadata.RowKeyName }
      : null;

    if (useCache)
    {
      await EnsureCacheLoaded();
      if (filters.Any())
        throw new NotSupportedException("Filters are not supported for cacheable tables.");
      return (await Cache.GetItems(Metadata)).ToArray();
    }

    var table = new TableClient(ConnectionString, Metadata.Name);
    var items = await table.QueryAsync<TableEntity>(filterString, null, loadFields).ToArrayAsync();
    return items.ToArray();
  }


  /// <inheritdoc />>
  public async Task<T[]> Read(Predicate<T> filter = null)
  {
    var entities = await ReadEntities();
    return entities.Select(EntityConverter.FromEntity<T>)
      .Where(e => filter?.Invoke(e) != false)
      .ToArray();
  }

  public IStorageTable<T> Reset()
  {
    Filters.Clear();
    return this;
  }

  public async Task Store(IEnumerable<T> entities)
  {
    var table = new TableClient(ConnectionString, Metadata.Name);
    var items = entities.Select(EntityConverter.ToEntity);

    var operations = items.Select(i => new TableTransactionAction(TableTransactionActionType.UpsertReplace, i)).ToArray();
    if (!operations.Any()) return;
    await table.SubmitTransactionAsync(operations);

    await ResetCache();
  }

  public async Task<int> DeleteAll(Predicate<T> filter = null)
  {
    TableEntity[] entities;
    if (filter != null)
    {
      var allEntities = await Read(filter);
      entities = allEntities.Select(e => new TableEntity(Metadata.GetPartitionKey(e), Metadata.GetRowKey(e))).ToArray();
    }
    else
      entities = await ReadEntities(true);

    return await DeleteInternal(entities);
  }

  public async Task<int> Delete(IEnumerable<T> entities)
  {
    var tempTableEntities = entities.Select(e => new TableEntity(Metadata.GetPartitionKey(e), Metadata.GetRowKey(e))).ToArray();
    return await DeleteInternal(tempTableEntities);
  }

  private async Task<int> DeleteInternal(IReadOnlyCollection<TableEntity> entities)
  {
    var table = new TableClient(ConnectionString, Metadata.Name);
    var operations = entities.Select(
      i => new TableTransactionAction(TableTransactionActionType.Delete, i)).ToArray();
    if (!operations.Any()) return 0;
    await table.SubmitTransactionAsync(operations);

    await ResetCache();

    return entities.Count;
  }


  private async Task ResetCache()
  {
    if (Cache != null && Metadata.IsCached)
      await Cache.Reset(Metadata);
  }

  private async Task EnsureCacheLoaded()
  {
    if (Cache == null || !Metadata.IsCached) return;
    await Cache.LoadCache(Metadata, async _ =>
    {
      var rec2 = Create(ConnectionString);
      return await rec2.ReadEntities();
    });
  }

}