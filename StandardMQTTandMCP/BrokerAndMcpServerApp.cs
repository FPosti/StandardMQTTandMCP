using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Hosting;
using MQTTnet;
using MQTTnet.Protocol;
using MQTTnet.Server;

public sealed class BrokerAndMcpServerApp : BackgroundService
{
    private const int DefaultMqttBrokerPort = 1883;
    private const string DefaultMqttBrokerHost = "127.0.0.1";
    private const string MqttBrokerPortEnvironmentVariable = "MQTT_BROKER_PORT";
    private const string MqttBrokerHostEnvironmentVariable = "MQTT_BROKER_HOST";
    private const string MqttBrokerStartEnvironmentVariable = "MQTT_BROKER_START";
    private const int MaxRecentMessages = 200;

    // Singleton instance so static MCP tool methods can access the running app.
    public static BrokerAndMcpServerApp? Instance { get; private set; }

    public BrokerAndMcpServerApp()
    {
        Instance = this;
        MqttBrokerHost = GetConfiguredMqttBrokerHost();
        MqttBrokerPort = GetConfiguredMqttBrokerPort();
        ShouldStartMqttBroker = GetConfiguredMqttBrokerStart();
    }

    private string MqttBrokerHost { get; }

    private int MqttBrokerPort { get; }

    private bool ShouldStartMqttBroker { get; }

    private readonly ConcurrentDictionary<string, EdgeMqttMessage> _latestDeviceMessagesByTopic = new();
    private readonly ConcurrentQueue<EdgeMqttMessage> _recentDeviceMessages = new();
    private readonly Lock _messageWaitersLock = new();
    private readonly List<MessageWaiter> _messageWaiters = [];

    private MqttServer? _mqttBroker;
    private IMqttClient? _mcpServerMqttBridgeClient;
    private bool _isStopping;

    public async Task PublishCommandFromMcpToBrokerAsync(string topic, string payload)
    {
        if (_mcpServerMqttBridgeClient is null || !_mcpServerMqttBridgeClient.IsConnected)
        {
            throw new InvalidOperationException("The MCP to broker bridge client is not connected.");
        }

        var mqttMessage = new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(payload)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce)
            .Build();

        await _mcpServerMqttBridgeClient.PublishAsync(mqttMessage);
    }

    public string ReadLatestDeviceMessage(string topic)
    {
        return _latestDeviceMessagesByTopic.TryGetValue(topic, out var message)
            ? message.Payload
            : "No message has been received for this topic.";
    }

    public EdgeMqttMessage? ReadLatestDeviceMessageDetails(string topic)
    {
        return _latestDeviceMessagesByTopic.TryGetValue(topic, out var message)
            ? message
            : null;
    }

    public IReadOnlyList<EdgeMqttMessage> ReadLatestDeviceMessages(string topicFilter, int maxMessages)
    {
        return _latestDeviceMessagesByTopic.Values
            .Where(message => MqttTopicMatchesFilter(message.Topic, topicFilter))
            .OrderByDescending(message => message.ReceivedAtUtc)
            .Take(Math.Clamp(maxMessages, 1, MaxRecentMessages))
            .ToArray();
    }

    public IReadOnlyList<EdgeMqttMessage> ReadRecentDeviceMessages(string topicFilter, int maxMessages)
    {
        return _recentDeviceMessages
            .Where(message => MqttTopicMatchesFilter(message.Topic, topicFilter))
            .OrderByDescending(message => message.ReceivedAtUtc)
            .Take(Math.Clamp(maxMessages, 1, MaxRecentMessages))
            .ToArray();
    }

    public IReadOnlyList<string> ListObservedTopics(string topicFilter, int maxTopics)
    {
        return _latestDeviceMessagesByTopic.Keys
            .Where(topic => MqttTopicMatchesFilter(topic, topicFilter))
            .Order(StringComparer.Ordinal)
            .Take(Math.Clamp(maxTopics, 1, MaxRecentMessages))
            .ToArray();
    }

    public async Task<EdgeMqttMessage?> WaitForNextDeviceMessageAsync(string topicFilter, TimeSpan timeout)
    {
        var timeoutMilliseconds = Math.Clamp((int)timeout.TotalMilliseconds, 1, 60000);
        var waiter = new MessageWaiter(topicFilter);

        lock (_messageWaitersLock)
        {
            _messageWaiters.Add(waiter);
        }

        var completedTask = await Task.WhenAny(waiter.Task, Task.Delay(timeoutMilliseconds));

        if (completedTask == waiter.Task)
        {
            return await waiter.Task;
        }

        lock (_messageWaitersLock)
        {
            _messageWaiters.Remove(waiter);
        }

        return null;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await StartInternalMqttBrokerAsync();
        await StartInternalMcpToBrokerBridgeClientAsync(stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(1000, stoppingToken);
        }
    }

    private async Task StartInternalMqttBrokerAsync()
    {
        if (!ShouldStartMqttBroker)
        {
            Console.Error.WriteLine($"[MQTT BROKER] Broker startup disabled; connecting to existing broker at {MqttBrokerHost}:{MqttBrokerPort}.");
            return;
        }

        if (!IsTcpPortAvailable(MqttBrokerPort))
        {
            Console.Error.WriteLine($"[MQTT BROKER] Port {MqttBrokerPort} is already in use; using the existing MQTT broker.");
            return;
        }

        var mqttServerFactory = new MqttServerFactory();

        var mqttBrokerOptions = new MqttServerOptionsBuilder()
            .WithDefaultEndpoint()
            .WithDefaultEndpointPort(MqttBrokerPort)
            .Build();

        _mqttBroker = mqttServerFactory.CreateMqttServer(mqttBrokerOptions);

        _mqttBroker.ClientConnectedAsync += eventArgs =>
        {
            Console.Error.WriteLine($"[MQTT BROKER] Client connected: {eventArgs.ClientId}");
            return Task.CompletedTask;
        };

        _mqttBroker.ClientDisconnectedAsync += eventArgs =>
        {
            Console.Error.WriteLine($"[MQTT BROKER] Client disconnected: {eventArgs.ClientId}");
            return Task.CompletedTask;
        };

        _mqttBroker.InterceptingPublishAsync += eventArgs =>
        {
            var topic = eventArgs.ApplicationMessage?.Topic ?? string.Empty;
            var payload = eventArgs.ApplicationMessage?.ConvertPayloadToString() ?? string.Empty;

            StoreDeviceMessage(
                topic,
                payload,
                eventArgs.ApplicationMessage?.QualityOfServiceLevel.ToString() ?? string.Empty,
                eventArgs.ApplicationMessage?.Retain ?? false);

            Console.Error.WriteLine($"[MQTT BROKER] Received publish on topic '{topic}' with payload '{payload}'");
            return Task.CompletedTask;
        };

        try
        {
            await _mqttBroker.StartAsync();
            Console.Error.WriteLine($"[MQTT BROKER] Running on port {MqttBrokerPort}");
        }
        catch (Exception) when (!IsTcpPortAvailable(MqttBrokerPort))
        {
            _mqttBroker = null;
            Console.Error.WriteLine($"[MQTT BROKER] Port {MqttBrokerPort} is already in use; using the existing MQTT broker.");
        }
    }

    private async Task StartInternalMcpToBrokerBridgeClientAsync(CancellationToken stoppingToken)
    {
        var mqttClientFactory = new MqttClientFactory();
        _mcpServerMqttBridgeClient = mqttClientFactory.CreateMqttClient();

        _mcpServerMqttBridgeClient.ConnectedAsync += eventArgs =>
        {
            Console.Error.WriteLine("[MCP SERVER MQTT BRIDGE] Connected to internal MQTT broker");
            return Task.CompletedTask;
        };

        _mcpServerMqttBridgeClient.DisconnectedAsync += eventArgs =>
        {
            if (_isStopping)
            {
                Console.Error.WriteLine("[MCP SERVER MQTT BRIDGE] Stopped");
            }
            else
            {
                Console.Error.WriteLine("[MCP SERVER MQTT BRIDGE] Disconnected unexpectedly from internal MQTT broker");
            }

            return Task.CompletedTask;
        };

        _mcpServerMqttBridgeClient.ApplicationMessageReceivedAsync += eventArgs =>
        {
            var topic = eventArgs.ApplicationMessage.Topic;
            var payload = eventArgs.ApplicationMessage.ConvertPayloadToString();

            StoreDeviceMessage(
                topic,
                payload,
                eventArgs.ApplicationMessage.QualityOfServiceLevel.ToString(),
                eventArgs.ApplicationMessage.Retain);

            Console.Error.WriteLine($"[MCP SERVER MQTT BRIDGE] Observed topic '{topic}' with payload '{payload}'");
            return Task.CompletedTask;
        };

        var mqttBridgeClientOptions = new MqttClientOptionsBuilder()
            .WithClientId($"mcp-server-bridge-client-{Environment.ProcessId}")
            .WithTcpServer(MqttBrokerHost, MqttBrokerPort)
            .Build();

        await ConnectMqttBridgeClientWithRetryAsync(mqttBridgeClientOptions, stoppingToken);

        var subscribeOptions = mqttClientFactory.CreateSubscribeOptionsBuilder()
            .WithTopicFilter(filter => filter.WithTopic("#"))
            .Build();

        await _mcpServerMqttBridgeClient.SubscribeAsync(subscribeOptions, stoppingToken);
        Console.Error.WriteLine("[MCP SERVER MQTT BRIDGE] Subscribed to all topics");
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

    private static int GetConfiguredMqttBrokerPort()
    {
        var configuredPort = Environment.GetEnvironmentVariable(MqttBrokerPortEnvironmentVariable);

        if (int.TryParse(configuredPort, out var port) && port is > 0 and <= 65535)
        {
            return port;
        }

        return DefaultMqttBrokerPort;
    }

    private static string GetConfiguredMqttBrokerHost()
    {
        var configuredHost = Environment.GetEnvironmentVariable(MqttBrokerHostEnvironmentVariable);

        return string.IsNullOrWhiteSpace(configuredHost)
            ? DefaultMqttBrokerHost
            : configuredHost;
    }

    private static bool GetConfiguredMqttBrokerStart()
    {
        var configuredStart = Environment.GetEnvironmentVariable(MqttBrokerStartEnvironmentVariable);

        return !bool.TryParse(configuredStart, out var shouldStartBroker) || shouldStartBroker;
    }

    private async Task ConnectMqttBridgeClientWithRetryAsync(MqttClientOptions mqttBridgeClientOptions, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _mcpServerMqttBridgeClient!.ConnectAsync(mqttBridgeClientOptions, stoppingToken);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[MCP SERVER MQTT BRIDGE] Waiting for MQTT broker at {MqttBrokerHost}:{MqttBrokerPort}. {ex.GetType().Name}: {ex.Message}");
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
        }
    }

    private void StoreDeviceMessage(string topic, string payload, string qualityOfServiceLevel, bool retain)
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            return;
        }

        var message = new EdgeMqttMessage(
            topic,
            payload,
            DateTimeOffset.UtcNow,
            qualityOfServiceLevel,
            retain);

        _latestDeviceMessagesByTopic[topic] = message;
        _recentDeviceMessages.Enqueue(message);

        while (_recentDeviceMessages.Count > MaxRecentMessages && _recentDeviceMessages.TryDequeue(out _))
        {
        }

        CompleteMatchingWaiters(message);
    }

    private void CompleteMatchingWaiters(EdgeMqttMessage message)
    {
        List<MessageWaiter>? matchingWaiters = null;

        lock (_messageWaitersLock)
        {
            for (var index = _messageWaiters.Count - 1; index >= 0; index--)
            {
                var waiter = _messageWaiters[index];

                if (!MqttTopicMatchesFilter(message.Topic, waiter.TopicFilter))
                {
                    continue;
                }

                matchingWaiters ??= [];
                matchingWaiters.Add(waiter);
                _messageWaiters.RemoveAt(index);
            }
        }

        if (matchingWaiters is null)
        {
            return;
        }

        foreach (var waiter in matchingWaiters)
        {
            waiter.TrySetResult(message);
        }
    }

    private static bool MqttTopicMatchesFilter(string topic, string topicFilter)
    {
        if (string.IsNullOrWhiteSpace(topicFilter) || topicFilter == "#")
        {
            return true;
        }

        var topicLevels = topic.Split('/');
        var filterLevels = topicFilter.Split('/');

        for (var filterIndex = 0; filterIndex < filterLevels.Length; filterIndex++)
        {
            var filterLevel = filterLevels[filterIndex];

            if (filterLevel == "#")
            {
                return filterIndex == filterLevels.Length - 1;
            }

            if (filterIndex >= topicLevels.Length)
            {
                return false;
            }

            if (filterLevel == "+")
            {
                continue;
            }

            if (!string.Equals(filterLevel, topicLevels[filterIndex], StringComparison.Ordinal))
            {
                return false;
            }
        }

        return topicLevels.Length == filterLevels.Length;
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _isStopping = true;

        if (_mcpServerMqttBridgeClient is not null && _mcpServerMqttBridgeClient.IsConnected)
        {
            await _mcpServerMqttBridgeClient.DisconnectAsync();
        }

        if (_mqttBroker is not null)
        {
            await _mqttBroker.StopAsync();
        }

        await base.StopAsync(cancellationToken);
    }

    private sealed class MessageWaiter(string topicFilter)
    {
        private readonly TaskCompletionSource<EdgeMqttMessage> _taskCompletionSource = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public string TopicFilter { get; } = topicFilter;

        public Task<EdgeMqttMessage> Task => _taskCompletionSource.Task;

        public void TrySetResult(EdgeMqttMessage message)
        {
            _taskCompletionSource.TrySetResult(message);
        }
    }
}
