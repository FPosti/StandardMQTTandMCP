using System.Globalization;
using StandardMQTTandMCP.Abstractions;

namespace StandardMQTTandMCP.Mqtt;

public sealed class LatencyReadingFactory
{
    public LatencyReading Create(string topic, string payload, DateTimeOffset receivedAtUtc)
    {
        var trimmedPayload = payload.Trim();

        if (DateTimeOffset.TryParseExact(
                trimmedPayload,
                "O",
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                out var sentAtUtc))
        {
            return new LatencyReading(
                topic,
                trimmedPayload,
                receivedAtUtc,
                sentAtUtc,
                (receivedAtUtc - sentAtUtc).TotalMilliseconds,
                null);
        }

        return new LatencyReading(
            topic,
            trimmedPayload,
            receivedAtUtc,
            null,
            null,
            "Payload must be a UTC timestamp like 2026-04-28T12:43:42.1329812Z.");
    }
}
