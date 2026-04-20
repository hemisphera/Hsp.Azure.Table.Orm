using System;

namespace Hsp.Azure.Table.Orm;

/// <summary>
/// Exception thrown when a field is not found in the table metadata.
/// </summary>
public class FieldNotFoundException : Exception
{
  /// <summary>
  /// The table for which the field was not found.
  /// </summary>
  public TableMetadata Table { get; }

  /// <summary>
  /// The name of the field that was not found.
  /// </summary>
  public string FieldName { get; }

  /// <summary>
  /// Specifies if the field name is the storage name (true) or the model name (false).
  /// </summary>
  public bool IsStorageName { get; }


  /// <summary>
  /// </summary>
  /// <param name="table"></param>
  /// <param name="fieldName"></param>
  /// <param name="isStorageName"></param>
  public FieldNotFoundException(TableMetadata table, string fieldName, bool isStorageName)
    : base(MakeMessage(table, fieldName, isStorageName))
  {
    Table = table;
    FieldName = fieldName;
    IsStorageName = isStorageName;
  }

  private static string MakeMessage(TableMetadata table, string name, bool isStorageName)
  {
    var type = isStorageName ? "storage" : "model";
    return $"Field {name} ({type}) was not found on {table.Name}";
  }
}