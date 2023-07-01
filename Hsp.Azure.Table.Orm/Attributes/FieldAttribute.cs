using System;

namespace Hsp.Azure.Table.Orm;

/// <summary>
/// Apply this attribute to properties that you want to map as fields on the Azure Table Storage.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public class FieldAttribute : Attribute
{

  /// <summary>
  /// The name of the field on the Azure Table Storage.
  /// If this is not specified, the name of the property this attribute is applied to will be used.
  /// </summary>
  public string Name { get; set; }

}