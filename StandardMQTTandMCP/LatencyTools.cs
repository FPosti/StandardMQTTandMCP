using System.ComponentModel;
using ModelContextProtocol.Server;
using StandardMQTTandMCP.Abstractions;

namespace StandardMQTTandMCP.McpServer;

[McpServerToolType]
public sealed class LatencyTools
{
    private readonly ILatencyReader _latencyReader;

    public LatencyTools(ILatencyReader latencyReader)
    {
        _latencyReader = latencyReader;
    }

    [McpServerTool, Description("Read the latest latency measurement for one exact MQTT topic.")]
    public object ReadLatestLatency(
        [Description("Exact MQTT topic to read.")] string mqttTopic)
    {
        var reading = _latencyReader.ReadLatest(mqttTopic);

        return reading is null
            ? new { Topic = mqttTopic, Error = "No message has been received for this topic yet." }
            : reading;
    }
}
