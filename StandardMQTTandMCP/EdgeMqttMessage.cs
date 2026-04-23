public sealed record EdgeMqttMessage(
    string Topic,
    string Payload,
    DateTimeOffset ReceivedAtUtc,
    string QualityOfServiceLevel,
    bool Retain);
