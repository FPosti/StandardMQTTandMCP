namespace StandardMQTTandMCP.Abstractions;

public sealed record LatencyReading(
    string Topic,
    string Payload,
    DateTimeOffset ReceivedAtUtc,
    DateTimeOffset? SentAtUtc,
    double? LatencyMilliseconds,
    string? ParseError);
