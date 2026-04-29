using Microsoft.Extensions.Hosting;
using MQTTnet;
using StandardMQTTandMCP.Abstractions;

namespace StandardMQTTandMCP.Mqtt;

public sealed class LatencyMqttBridge : BackgroundService
{
    private readonly MqttOptions _mqttOptions;
    private readonly LatencyReadingFactory _readingFactory;
    private readonly ILatencyStore _latencyStore;
    private readonly CsvLatencyWriter _csvWriter;
    private IMqttClient? _mqttClient;
    private bool _isStopping;

    public LatencyMqttBridge(
        MqttOptions mqttOptions,
        LatencyReadingFactory readingFactory,
        ILatencyStore latencyStore,
        CsvLatencyWriter csvWriter)
    {
        _mqttOptions = mqttOptions;
        _readingFactory = readingFactory;
        _latencyStore = latencyStore;
        _csvWriter = csvWriter;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await ConnectAsync(stoppingToken);
        await SubscribeAsync(stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(1000, stoppingToken);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _isStopping = true;

        if (_mqttClient is not null && _mqttClient.IsConnected)
        {
            await _mqttClient.DisconnectAsync();
        }

        await base.StopAsync(cancellationToken);
    }

    private async Task ConnectAsync(CancellationToken stoppingToken)
    {
        var mqttClientFactory = new MqttClientFactory();
        _mqttClient = mqttClientFactory.CreateMqttClient();

        _mqttClient.ConnectedAsync += _ =>
        {
            Console.Error.WriteLine($"[MQTT BRIDGE] Connected to {_mqttOptions.Host}:{_mqttOptions.Port}");
            return Task.CompletedTask;
        };

        _mqttClient.DisconnectedAsync += _ =>
        {
            Console.Error.WriteLine(_isStopping ? "[MQTT BRIDGE] Stopped" : "[MQTT BRIDGE] Disconnected unexpectedly");
            return Task.CompletedTask;
        };

        _mqttClient.ApplicationMessageReceivedAsync += eventArgs =>
        {
            var topic = eventArgs.ApplicationMessage.Topic;
            var payload = eventArgs.ApplicationMessage.ConvertPayloadToString();
            var reading = _readingFactory.Create(topic, payload, DateTimeOffset.UtcNow);

            _latencyStore.Store(reading);
            _csvWriter.Write(reading);

            Console.Error.WriteLine($"[MQTT BRIDGE] {topic} -> {payload}");
            return Task.CompletedTask;
        };

        var clientOptions = new MqttClientOptionsBuilder()
            .WithClientId($"latency-client-{Environment.ProcessId}")
            .WithTcpServer(_mqttOptions.Host, _mqttOptions.Port)
            .Build();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _mqttClient.ConnectAsync(clientOptions, stoppingToken);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[MQTT BRIDGE] Waiting for broker at {_mqttOptions.Host}:{_mqttOptions.Port}. {ex.GetType().Name}: {ex.Message}");
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
        }
    }

    private async Task SubscribeAsync(CancellationToken stoppingToken)
    {
        var mqttClientFactory = new MqttClientFactory();
        var subscribeOptions = mqttClientFactory.CreateSubscribeOptionsBuilder()
            .WithTopicFilter(filter => filter.WithTopic(_mqttOptions.SubscribeTopicFilter))
            .Build();

        await _mqttClient!.SubscribeAsync(subscribeOptions, stoppingToken);
        Console.Error.WriteLine($"[MQTT BRIDGE] Subscribed to '{_mqttOptions.SubscribeTopicFilter}'");
    }
}
