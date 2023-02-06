using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Hsp.Azure.Table.Orm
{

    public static class StorageHelpers
  {

    public static async Task Store<T>(this IStorageTable<T> rec, T item)
    {
      await rec.Store(new[] { item });
    }

    public static IStorageTable<T> SetRange<T>(this IStorageTable<T> rec, Expression<Func<T, object>> exp, object value, string comparison = null)
    {
      var memberName = GetMemberName(exp);
      return rec.SetRange(memberName, value, comparison);
    }

    public static IStorageTable<T> SetRangeIf<T>(this IStorageTable<T> rec, bool expr, string name, object value, string comparison = null)
    {
      if (expr)
        rec.SetRange(name, value, comparison);
      return rec;
    }

    public static IStorageTable<T> SetRangeIf<T>(this IStorageTable<T> rec, bool expr, Expression<Func<T, object>> exp, object value, string comparison = null)
    {
      var memberName = GetMemberName(exp);
      return rec.SetRangeIf(expr, memberName, value, comparison);
    }

    public static async Task<T> FindFirst<T>(this IStorageTable<T> rec)
    {
      var all = await rec.Read();
      return all.FirstOrDefault();
    }

    private static string GetMemberName<T>(Expression<Func<T, object>> exp)
    {
      var memberExp = exp.Body as MemberExpression;
      if (memberExp == null && exp.Body is UnaryExpression uex)
        memberExp = uex.Operand as MemberExpression;
      if (memberExp == null)
        throw new NotSupportedException($"The expression {exp} is not supported.");
      var memberName = memberExp.Member.Name;
      return memberName;
    }


    public static DateTime? ToDayRangeFrom(this DateTime? dt)
    {
      if (dt == null) return null;
      var dtv = dt.Value;
      return new DateTime(dtv.Year, dtv.Month, dtv.Day, 0, 0, 0, dtv.Kind);
    }

    public static DateTime? ToDayRangeTo(this DateTime? dt)
    {
      if (dt == null) return null;
      var dtv = dt.Value;
      return new DateTime(dtv.Year, dtv.Month, dtv.Day, 23, 59, 59, dtv.Kind);
    }

    public static bool IsInDateRangeFrom(this DateTime? dt)
    {
      if (dt == null) return true;
      return dt.ToDayRangeFrom() <= DateTime.Now;
    }

    public static bool IsInDateRangeTo(this DateTime? dt, TimeSpan? gracePeriod = null)
    {
      if (dt == null) return true;
      var refDt = gracePeriod == null ? dt : dt.Value.Add(gracePeriod.Value);

      return refDt.ToDayRangeTo() >= DateTime.Now;
    }


    public static IEnumerable<T> NotNull<T>(this IEnumerable<T> list)
    {
      return list.Where(l => l != null);
    }

  }

}
