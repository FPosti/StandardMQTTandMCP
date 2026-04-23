using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;

[McpServerToolType]
public static class AiAgentFacingMcpTools
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    [McpServerTool, Description("Publish a command or message from the AI agent to the MQTT broker for an edge device to receive.")]
    public static async Task<string> SendMessageToBroker(
        [Description("MQTT topic to publish to, for example edge/device-1/command")] string mqttTopic,
        [Description("Text payload to publish")] string payload)
    {
        var app = GetApp();
        await app.PublishCommandFromMcpToBrokerAsync(mqttTopic, payload);
        return $"Message sent to broker on topic '{mqttTopic}'.";
    }

    [McpServerTool, Description("Read only the latest payload text seen on one exact MQTT topic.")]
    public static string ReadLatestBrokerMessage(
        [Description("Exact MQTT topic to read, for example edge/device-1/temperature")] string mqttTopic)
    {
        return GetApp().ReadLatestDeviceMessage(mqttTopic);
    }

    [McpServerTool, Description("Read the latest structured MQTT message seen on one exact edge-device topic.")]
    public static string ReadLatestMqttMessage(
        [Description("Exact MQTT topic to read, for example edge/device-1/temperature")] string mqttTopic)
    {
        var message = GetApp().ReadLatestDeviceMessageDetails(mqttTopic);

        return message is null
            ? $"No message has been received for topic '{mqttTopic}'."
            : JsonSerializer.Serialize(message, JsonOptions);
    }

    [McpServerTool, Description("List MQTT topics that have been observed from edge devices. Supports MQTT wildcards + and #.")]
    public static string ListObservedMqttTopics(
        [Description("MQTT topic filter, for example #, edge/+/temperature, or edge/device-1/#")] string topicFilter = "#",
        [Description("Maximum number of topics to return.")] int maxTopics = 100)
    {
        var topics = GetApp().ListObservedTopics(topicFilter, maxTopics);

        return topics.Count == 0
            ? $"No MQTT topics have been observed for filter '{topicFilter}'."
            : JsonSerializer.Serialize(topics, JsonOptions);
    }

    [McpServerTool, Description("Read the latest message for each observed MQTT topic matching a filter. Supports MQTT wildcards + and #.")]
    public static string ReadLatestMqttMessages(
        [Description("MQTT topic filter, for example #, edge/+/temperature, or edge/device-1/#")] string topicFilter = "#",
        [Description("Maximum number of messages to return.")] int maxMessages = 25)
    {
        var messages = GetApp().ReadLatestDeviceMessages(topicFilter, maxMessages);

        return messages.Count == 0
            ? $"No MQTT messages have been received for filter '{topicFilter}'."
            : JsonSerializer.Serialize(messages, JsonOptions);
    }

    [McpServerTool, Description("Read recent MQTT messages matching a filter, including multiple messages from the same topic. Supports MQTT wildcards + and #.")]
    public static string ReadRecentMqttMessages(
        [Description("MQTT topic filter, for example #, edge/+/temperature, or edge/device-1/#")] string topicFilter = "#",
        [Description("Maximum number of recent messages to return.")] int maxMessages = 25)
    {
        var messages = GetApp().ReadRecentDeviceMessages(topicFilter, maxMessages);

        return messages.Count == 0
            ? $"No recent MQTT messages have been received for filter '{topicFilter}'."
            : JsonSerializer.Serialize(messages, JsonOptions);
    }

    [McpServerTool, Description("Wait briefly for the next MQTT message matching a topic filter, then return it. Useful when an edge device is expected to publish soon.")]
    public static async Task<string> WaitForMqttMessage(
        [Description("MQTT topic filter, for example #, edge/+/temperature, or edge/device-1/#")] string topicFilter = "#",
        [Description("Maximum seconds to wait, from 1 to 60.")] int timeoutSeconds = 10)
    {
        var timeout = TimeSpan.FromSeconds(Math.Clamp(timeoutSeconds, 1, 60));
        var message = await GetApp().WaitForNextDeviceMessageAsync(topicFilter, timeout);

        return message is null
            ? $"No MQTT message was received for filter '{topicFilter}' within {timeout.TotalSeconds:0} seconds."
            : JsonSerializer.Serialize(message, JsonOptions);
    }

    private static BrokerAndMcpServerApp GetApp()
    {
        return BrokerAndMcpServerApp.Instance
            ?? throw new InvalidOperationException("BrokerAndMcpServerApp is not initialized.");
    }
}
