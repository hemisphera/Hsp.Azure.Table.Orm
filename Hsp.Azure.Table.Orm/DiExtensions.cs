using System;
using Microsoft.Extensions.DependencyInjection;

namespace Hsp.Azure.Table.Orm;

public static class DiExtensions
{
  public static void AddAzureTable(
    this IServiceCollection services,
    Action<IServiceProvider, RecordFactoryOptions>? configure = null,
    Action<IServiceProvider, RecordFactory>? setup = null)
  {
    services.AddSingleton<RecordFactory>(sp =>
    {
      var options = new RecordFactoryOptions();
      configure?.Invoke(sp, options);
      var factory = new RecordFactory(options.ConnectionString);
      setup?.Invoke(sp, factory);
      return factory;
    });
  }
}