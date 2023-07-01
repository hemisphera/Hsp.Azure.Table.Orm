using System;

namespace Hsp.Azure.Table.Orm;

/// <summary>
/// Apply this attribute to a class to enable caching for that table.
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public class CacheableAttribute : Attribute
{
}