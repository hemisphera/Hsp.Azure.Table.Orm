using System;

namespace Hsp.Azure.Table.Orm
{

  [AttributeUsage(AttributeTargets.Property)]
  public class PartitionKeyAttribute : Attribute
  {
  }

}