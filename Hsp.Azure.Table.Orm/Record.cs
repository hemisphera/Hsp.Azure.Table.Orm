using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Azure.Data.Tables;

namespace Hsp.Azure.Table.Orm;

/// <summary>
/// Provides access to a single table in a storage account.
/// </summary>
/// <typeparam name="T">The entity type to open.</typeparam>
public class Record<T> where T : class, new()
{

  private TableMetadata Metadata { get; }

  private string ConnectionString { get; }

  private IStorageCache Cache { get; }


  /// <summary>
  /// Creates an instance for the given entity type.
  /// This entity must have his metadata registered first.
  /// </summary>
  /// <param name="connectionString">The connection string to the storage account.</param>
  /// <param name="cache">An otpional cache to be used.</param>
  /// <returns>The record instance.</returns>
  public static Record<T> Create(string connectionString, IStorageCache cache = null)
  {
    return new Record<T>(connectionString, cache);
  }

  private List<string> Filters { get; } = new List<string>();



  private Record(string connectionString, IStorageCache cache)
  {
    Metadata = TableMetadata.Get<T>();
    ConnectionString = connectionString;
    Cache = cache;
  }



  private static string GetMemberName(Expression<Func<T, object>> exp)
  {
    var memberExp = exp.Body as MemberExpression;
    if (memberExp == null && exp.Body is UnaryExpression uex)
      memberExp = uex.Operand as MemberExpression;
    if (memberExp == null)
      throw new NotSupportedException($"The expression {exp} is not supported.");
    var memberName = memberExp.Member.Name;
    return memberName;
  }



  /// <summary>
  /// Sets a server-side filter on the record by specifying the name of the property to filter for.
  /// </summary>
  /// <param name="name">The name of the property of the class to filter for. This will be automatically translated to the storage field.</param>
  /// <param name="value">The value to filter for.</param>
  /// <param name="comparison">The comparison to apply.</param>
  /// <returns>Itself</returns>
  public Record<T> SetRange(string name, object value, string comparison = null)
  {
    var field = Metadata.GetFieldByModelName(name);
    Filters.Add(CreateFilter(field, value, comparison));
    return this;
  }

  /// <summary>
  /// Sets a server-side filter on the record by specifying the name of the property to filter for.
  /// </summary>
  /// <param name="exp">An expression from which the property of the class is extracted.</param>
  /// <param name="value">The value to filter for.</param>
  /// <param name="comparison">The comparison to apply.</param>
  /// <returns>Itself</returns>
  public Record<T> SetRange(Expression<Func<T, object>> exp, object value, string comparison = null)
  {
    var memberName = GetMemberName(exp);
    return SetRange(memberName, value, comparison);
  }

  /// <summary>
  /// Sets a server-side filter on the record by specifying the name of the property to filter for, but only if the expression is true.
  /// </summary>
  /// <param name="eval">Only if this evaluates to true, the filter will be set.</param>
  /// <param name="name">The name of the property of the class to filter for. This will be automatically translated to the storage field.</param>
  /// <param name="value">The value to filter for.</param>
  /// <param name="comparison">The comparison to apply.</param>
  /// <returns>Itself</returns>
  public Record<T> SetRangeIf(bool eval, string name, object value, string comparison = null)
  {
    return eval
      ? SetRange(name, value, comparison)
      : this;
  }

  /// <summary>
  /// Sets a server-side filter on the record by specifying the name of the property to filter for, but only if the expression is true.
  /// </summary>
  /// <param name="eval">Only if this evaluates to true, the filter will be set.</param>
  /// <param name="exp">An expression from which the property of the class is extracted.</param>
  /// <param name="value">The value to filter for.</param>
  /// <param name="comparison">The comparison to apply.</param>
  /// <returns>Itself</returns>
  public Record<T> SetRangeIf(bool eval, Expression<Func<T, object>> exp, object value, string comparison = null)
  {
    var memberName = GetMemberName(exp);
    return SetRangeIf(eval, memberName, value, comparison);
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
        throw new ServerFiltersNotSupportedException(Metadata);
      return (await Cache.GetItems(Metadata)).ToArray();
    }

    var table = new TableClient(ConnectionString, Metadata.Name);
    var items = await table.QueryAsync<TableEntity>(filterString, null, loadFields).ToArrayAsync();
    return items.ToArray();
  }


  /// <summary>
  /// Returns all entities from the table that match the current server-side filters and, if provided, the client-side filters.
  /// </summary>
  /// <param name="clientSideFilter">An optional client-side filter that will be applied to the records after they have been read from the storage.</param>
  /// <returns>Itself</returns>
  public async Task<T[]> Read(Predicate<T> clientSideFilter = null)
  {
    var entities = await ReadEntities();
    return entities.Select(EntityConverter.FromEntity<T>)
      .Where(e => clientSideFilter?.Invoke(e) != false)
      .ToArray();
  }

  /// <summary>
  /// Returns the first entity from the table that matches the current server-side filters and, if provided, the client-side filters.
  /// </summary>
  /// <param name="clientSideFilter">An optional client-side filter that will be applied to the records after they have been read from the storage.</param>
  /// <returns>Itself</returns>
  public async Task<T> ReadFirst(Predicate<T> clientSideFilter = null)
  {
    var all = await Read(clientSideFilter);
    return all.FirstOrDefault();
  }

  /// <summary>
  /// Resets the record, removing all server-side filters and fields to be loaded.
  /// </summary>
  /// <returns>Itself</returns>
  public Record<T> Reset()
  {
    Filters.Clear();
    return this;
  }

  /// <summary>
  /// Stores the given entities on the table.
  /// </summary>
  /// <param name="entities">The entities to store.</param>
  public async Task Store(IEnumerable<T> entities)
  {
    var table = new TableClient(ConnectionString, Metadata.Name);
    var items = entities.Select(EntityConverter.ToEntity);

    var operations = items.Select(i => new TableTransactionAction(TableTransactionActionType.UpsertReplace, i)).ToArray();
    if (!operations.Any()) return;
    await table.SubmitTransactionAsync(operations);

    await ResetCache();
  }

  /// <summary>
  /// Stores the given entities on the table.
  /// </summary>
  /// <param name="entities">The entities to store.</param>
  public async Task Store(params T[] entities)
  {
    await Store((IEnumerable<T>)entities);
  }

  /// <summary>
  /// Deletes all entities from the table that match the current server-side filters and, if provided, the client-side filters.
  /// If you provide a client-side filter, records must first be fully read from the storage which might impact performance.
  /// </summary>
  /// <param name="filter"></param>
  /// <returns></returns>
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

  /// <summary>
  /// Deletes the given entities from the storage. This will not respect any server-side filters.
  /// </summary>
  /// <param name="entities">The entities to be deleted.</param>
  /// <returns>The number of entities that were actually deleted.</returns>
  public async Task<int> Delete(IEnumerable<T> entities)
  {
    var tempTableEntities = entities.Select(e => new TableEntity(Metadata.GetPartitionKey(e), Metadata.GetRowKey(e))).ToArray();
    return await DeleteInternal(tempTableEntities);
  }

  /// <summary>
  /// Deletes the given entities from the storage. This will not respect any server-side filters.
  /// </summary>
  /// <param name="entities">The entities to be deleted.</param>
  /// <returns>The number of entities that were actually deleted.</returns>
  public async Task<int> Delete(params T[] entities)
  {
    return await Delete((IEnumerable<T>)entities);
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