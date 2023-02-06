using System;

namespace Hsp.Azure.Table.Orm
{
  [AttributeUsage(AttributeTargets.Property)]
  public class FieldAttribute : Attribute
  {

    public string Name { get; set; }

  }
}