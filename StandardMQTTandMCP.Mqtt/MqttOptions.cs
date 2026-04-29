namespace StandardMQTTandMCP.Mqtt;

public sealed class MqttOptions
{
    public const string SectionName = "Mqtt";

    public string Host { get; init; } = "127.0.0.1";

    public int Port { get; init; } = 1883;

    public bool StartEmbeddedBroker { get; init; } = true;

    public string SubscribeTopicFilter { get; init; } = "#";

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(Host))
        {
            throw new InvalidOperationException("Mqtt:Host is required.");
        }

        if (Port is <= 0 or > 65535)
        {
            throw new InvalidOperationException("Mqtt:Port must be from 1 to 65535.");
        }

        if (string.IsNullOrWhiteSpace(SubscribeTopicFilter))
        {
            throw new InvalidOperationException("Mqtt:SubscribeTopicFilter is required.");
        }
    }
}
