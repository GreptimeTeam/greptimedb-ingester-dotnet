using GreptimeDB.Ingester.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace GreptimeDB.Ingester.DependencyInjection;

/// <summary>
/// Extension methods for registering GreptimeClient with dependency injection.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds GreptimeClient to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configureOptions">Action to configure client options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddGreptimeClient(
        this IServiceCollection services,
        Action<GreptimeClientOptions> configureOptions)
    {
        services.Configure(configureOptions);
        services.TryAddSingleton<GreptimeClient>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<GreptimeClientOptions>>().Value;
            var logger = sp.GetService<ILogger<GreptimeClient>>();
            return new GreptimeClient(options, logger);
        });

        return services;
    }

    /// <summary>
    /// Adds GreptimeClient to the service collection with pre-configured options.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="options">The client options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddGreptimeClient(
        this IServiceCollection services,
        GreptimeClientOptions options)
    {
        options.Validate();
        services.TryAddSingleton(Options.Create(options));
        services.TryAddSingleton<GreptimeClient>(sp =>
        {
            var logger = sp.GetService<ILogger<GreptimeClient>>();
            return new GreptimeClient(options, logger);
        });

        return services;
    }
}
