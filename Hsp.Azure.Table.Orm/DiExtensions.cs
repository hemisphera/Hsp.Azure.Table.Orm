using System;
using Microsoft.Extensions.DependencyInjection;

namespace Hsp.Azure.Table.Orm;

public static class DiExtensions
{
  public static void AddAzureTable(
    this IServiceCollection services,
    Action<RecordFactoryOptions>? configure = null,
    Action<RecordFactory>? setup = null)
  {
    services.AddSingleton<RecordFactory>(_ =>
    {
      var options = new RecordFactoryOptions();
      configure?.Invoke(options);
      var factory = new RecordFactory(options.ConnectionString);
      setup?.Invoke(factory);
      return factory;
    });
  }
}