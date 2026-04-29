using Microsoft.Extensions.DependencyInjection;
using StandardMQTTandMCP.Abstractions;

namespace StandardMQTTandMCP.Mqtt;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddMqttLatencyServices(
        this IServiceCollection services,
        MqttOptions mqttOptions,
        LatencyOptions latencyOptions)
    {
        mqttOptions.Validate();
        latencyOptions.Validate();

        services.AddSingleton(mqttOptions);
        services.AddSingleton(latencyOptions);
        services.AddSingleton<LatencyReadingFactory>();
        services.AddSingleton<ILatencyStore, InMemoryLatencyStore>();
        services.AddSingleton<ILatencyReader>(serviceProvider => serviceProvider.GetRequiredService<ILatencyStore>());
        services.AddSingleton<CsvLatencyWriter>();
        services.AddHostedService<EmbeddedMqttBroker>();
        services.AddHostedService<LatencyMqttBridge>();

        return services;
    }
}
