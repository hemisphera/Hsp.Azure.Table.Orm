using System;

namespace Hsp.Azure.Table.Orm;

/// <summary>
/// Apply this attribute to a property to mark it as the row key.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public class RowKeyAttribute : Attribute
{
}