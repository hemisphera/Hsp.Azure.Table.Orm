using System;

namespace Hsp.Azure.Table.Orm;


/// <summary>
/// </summary>
public class ServerFiltersNotSupportedException : NotSupportedException
{

  /// <summary>
  /// The table.
  /// </summary>
  public TableMetadata Table { get; }


  /// <summary>
  /// </summary>
  /// <param name="table"></param>
  public ServerFiltersNotSupportedException(TableMetadata table)
    : base($"Server-side filters are not supported for the cached table '{table.Name}'.")
  {
    Table = table;
  }

}