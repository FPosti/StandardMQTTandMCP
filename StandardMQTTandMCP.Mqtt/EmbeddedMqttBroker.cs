using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Hosting;
using MQTTnet.Server;

namespace StandardMQTTandMCP.Mqtt;

public sealed class EmbeddedMqttBroker : BackgroundService
{
    private readonly MqttOptions _options;
    private MqttServer? _mqttServer;

    public EmbeddedMqttBroker(MqttOptions options)
    {
        _options = options;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.StartEmbeddedBroker)
        {
            Console.Error.WriteLine($"[MQTT BROKER] Embedded broker disabled; using broker at {_options.Host}:{_options.Port}.");
            return;
        }

        if (!IsTcpPortAvailable(_options.Port))
        {
            Console.Error.WriteLine($"[MQTT BROKER] Port {_options.Port} is already in use; using the existing broker.");
            return;
        }

        var serverOptions = new MqttServerOptionsBuilder()
            .WithDefaultEndpoint()
            .WithDefaultEndpointPort(_options.Port)
            .Build();

        _mqttServer = new MqttServerFactory().CreateMqttServer(serverOptions);

        _mqttServer.ClientConnectedAsync += eventArgs =>
        {
            Console.Error.WriteLine($"[MQTT BROKER] Client connected: {eventArgs.ClientId}");
            return Task.CompletedTask;
        };

        _mqttServer.ClientDisconnectedAsync += eventArgs =>
        {
            Console.Error.WriteLine($"[MQTT BROKER] Client disconnected: {eventArgs.ClientId}");
            return Task.CompletedTask;
        };

        await _mqttServer.StartAsync();
        Console.Error.WriteLine($"[MQTT BROKER] Listening on port {_options.Port}");

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_mqttServer is not null)
        {
            await _mqttServer.StopAsync();
        }

        await base.StopAsync(cancellationToken);
    }

    private static bool IsTcpPortAvailable(int port)
    {
        TcpListener? tcpListener = null;

        try
        {
            tcpListener = new TcpListener(IPAddress.Any, port);
            tcpListener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ExclusiveAddressUse, true);
            tcpListener.Start();
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
        finally
        {
            tcpListener?.Stop();
        }
    }
}
