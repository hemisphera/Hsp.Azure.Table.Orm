using System;
using System.Globalization;
using System.Text;

namespace Hsp.Azure.Table.Orm
{

  internal static class FilterHelper
  {


    /// <summary>
    /// Generates a property filter condition string for the string value.
    /// </summary>
    /// <param name="propertyName">A string containing the name of the property to compare.</param>
    /// <param name="operation">A string containing the comparison operator to use.</param>
    /// <param name="givenValue">A string containing the value to compare with the property.</param>
    /// <returns>A string containing the formatted filter condition.</returns>
    public static string GenerateFilterCondition(
      string propertyName,
      string operation,
      string givenValue)
    {
      givenValue = givenValue ?? string.Empty;
      return FilterHelper.GenerateFilterCondition(propertyName, operation, givenValue, EdmType.String);
    }

    /// <summary>
    /// Generates a property filter condition string for the boolean value.
    /// </summary>
    /// <param name="propertyName">A string containing the name of the property to compare.</param>
    /// <param name="operation">A string containing the comparison operator to use.</param>
    /// <param name="givenValue">A <c>bool</c> containing the value to compare with the property.</param>
    /// <returns>A string containing the formatted filter condition.</returns>
    public static string GenerateFilterConditionForBool(
      string propertyName,
      string operation,
      bool givenValue)
    {
      return FilterHelper.GenerateFilterCondition(propertyName, operation, givenValue ? "true" : "false", EdmType.Boolean);
    }

    /// <summary>
    /// Generates a property filter condition string for the binary value.
    /// </summary>
    /// <param name="propertyName">A string containing the name of the property to compare.</param>
    /// <param name="operation">A string containing the comparison operator to use.</param>
    /// <param name="givenValue">A byte array containing the value to compare with the property.</param>
    /// <returns>A string containing the formatted filter condition.</returns>
    public static string GenerateFilterConditionForBinary(
      string propertyName,
      string operation,
      byte[] givenValue)
    {
      StringBuilder stringBuilder = new StringBuilder();
      foreach (byte num in givenValue)
        stringBuilder.AppendFormat("{0:x2}", (object)num);
      return FilterHelper.GenerateFilterCondition(propertyName, operation, stringBuilder.ToString(), EdmType.Binary);
    }

    /// <summary>
    /// Generates a property filter condition string for the <see cref="T:System.DateTimeOffset" /> value.
    /// </summary>
    /// <param name="propertyName">A string containing the name of the property to compare.</param>
    /// <param name="operation">A string containing the comparison operator to use.</param>
    /// <param name="givenValue">A <see cref="T:System.DateTimeOffset" /> containing the value to compare with the property.</param>
    /// <returns>A string containing the formatted filter condition.</returns>
    public static string GenerateFilterConditionForDate(
      string propertyName,
      string operation,
      DateTime givenValue)
    {
      return FilterHelper.GenerateFilterCondition(propertyName, operation, givenValue.ToUniversalTime().ToString("o", (IFormatProvider)CultureInfo.InvariantCulture), EdmType.DateTime);
    }

    /// <summary>
    /// Generates a property filter condition string for the <see cref="T:System.Double" /> value.
    /// </summary>
    /// <param name="propertyName">A string containing the name of the property to compare.</param>
    /// <param name="operation">A string containing the comparison operator to use.</param>
    /// <param name="givenValue">A <see cref="T:System.Double" /> containing the value to compare with the property.</param>
    /// <returns>A string containing the formatted filter condition.</returns>
    public static string GenerateFilterConditionForDouble(
      string propertyName,
      string operation,
      double givenValue)
    {
      return FilterHelper.GenerateFilterCondition(propertyName, operation, Convert.ToString(givenValue, (IFormatProvider)CultureInfo.InvariantCulture), EdmType.Double);
    }

    /// <summary>
    /// Generates a property filter condition string for an <see cref="T:System.Int32" /> value.
    /// </summary>
    /// <param name="propertyName">A string containing the name of the property to compare.</param>
    /// <param name="operation">A string containing the comparison operator to use.</param>
    /// <param name="givenValue">An <see cref="T:System.Int32" /> containing the value to compare with the property.</param>
    /// <returns>A string containing the formatted filter condition.</returns>
    public static string GenerateFilterConditionForInt(
      string propertyName,
      string operation,
      int givenValue)
    {
      return FilterHelper.GenerateFilterCondition(propertyName, operation, Convert.ToString(givenValue, (IFormatProvider)CultureInfo.InvariantCulture), EdmType.Int32);
    }

    /// <summary>
    /// Generates a property filter condition string for an <see cref="T:System.Int64" /> value.
    /// </summary>
    /// <param name="propertyName">A string containing the name of the property to compare.</param>
    /// <param name="operation">A string containing the comparison operator to use.</param>
    /// <param name="givenValue">An <see cref="T:System.Int64" /> containing the value to compare with the property.</param>
    /// <returns>A string containing the formatted filter condition.</returns>
    public static string GenerateFilterConditionForLong(
      string propertyName,
      string operation,
      long givenValue)
    {
      return FilterHelper.GenerateFilterCondition(propertyName, operation, Convert.ToString(givenValue, (IFormatProvider)CultureInfo.InvariantCulture), EdmType.Int64);
    }

    /// <summary>
    /// Generates a property filter condition string for the <see cref="T:System.Guid" /> value.
    /// </summary>
    /// <param name="propertyName">A string containing the name of the property to compare.</param>
    /// <param name="operation">A string containing the comparison operator to use.</param>
    /// <param name="givenValue">A <see cref="T:System.Guid" /> containing the value to compare with the property.</param>
    /// <returns>A string containing the formatted filter condition.</returns>
    public static string GenerateFilterConditionForGuid(
      string propertyName,
      string operation,
      Guid givenValue)
    {
      return FilterHelper.GenerateFilterCondition(propertyName, operation, givenValue.ToString(), EdmType.Guid);
    }

    /// <summary>
    /// Generates a property filter condition string for the <see cref="T:Microsoft.WindowsAzure.Storage.Table.EdmType" /> value, formatted as the specified <see cref="T:Microsoft.WindowsAzure.Storage.Table.EdmType" />.
    /// </summary>
    /// <param name="propertyName">A string containing the name of the property to compare.</param>
    /// <param name="operation">A string containing the comparison operator to use.</param>
    /// <param name="givenValue">A string containing the value to compare with the property.</param>
    /// <param name="edmType">The <see cref="T:Microsoft.WindowsAzure.Storage.Table.EdmType" /> to format the value as.</param>
    /// <returns>A string containing the formatted filter condition.</returns>
    private static string GenerateFilterCondition(
      string propertyName,
      string operation,
      string givenValue,
      EdmType edmType)
    {
      string str1;
      switch (edmType)
      {
        //case EdmType.Binary:
        //  str1 = string.Format((IFormatProvider)CultureInfo.InvariantCulture, "X'{0}'", (object)givenValue);
        //  break;
        case EdmType.Boolean:
        case EdmType.Int32:
          str1 = givenValue;
          break;
        case EdmType.DateTime:
          str1 = string.Format((IFormatProvider)CultureInfo.InvariantCulture, "datetime'{0}'", (object)givenValue);
          break;
        case EdmType.Double:
          string str2;
          if (!int.TryParse(givenValue, out int _))
            str2 = givenValue;
          else
            str2 = string.Format((IFormatProvider)CultureInfo.InvariantCulture, "{0}.0", (object)givenValue);
          str1 = str2;
          break;
        case EdmType.Guid:
          str1 = string.Format((IFormatProvider)CultureInfo.InvariantCulture, "guid'{0}'", (object)givenValue);
          break;
        case EdmType.Int64:
          str1 = string.Format((IFormatProvider)CultureInfo.InvariantCulture, "{0}L", (object)givenValue);
          break;
        default:
          str1 = string.Format((IFormatProvider)CultureInfo.InvariantCulture, "'{0}'", (object)givenValue.Replace("'", "''"));
          break;
      }
      return string.Format((IFormatProvider)CultureInfo.InvariantCulture, "{0} {1} {2}", (object)propertyName, (object)operation, (object)str1);
    }

    /// <summary>
    /// Creates a filter condition using the specified logical operator on two filter conditions.
    /// </summary>
    /// <param name="filterA">A string containing the first formatted filter condition.</param>
    /// <param name="operatorString">A string containing the operator to use (AND, OR).</param>
    /// <param name="filterB">A string containing the second formatted filter condition.</param>
    /// <returns>A string containing the combined filter expression.</returns>
    public static string CombineFilters(string filterA, string operatorString, string filterB) => string.Format((IFormatProvider)CultureInfo.InvariantCulture, "({0}) {1} ({2})", (object)filterA, (object)operatorString, (object)filterB);

  }

}