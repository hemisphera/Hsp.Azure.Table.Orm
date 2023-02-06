using System.Collections.Generic;
using System.Threading.Tasks;

namespace Hsp.Azure.Table.Orm
{

  public interface IStorageTable<T>
  {

    IStorageTable<T> SetRange(string field, object value, string comparison = null);

    Task<T[]> Read();

    Task Store(IEnumerable<T> accountEntity);

    Task<int> Delete();

  }

}
