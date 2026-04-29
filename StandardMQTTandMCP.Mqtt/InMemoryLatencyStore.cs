using System.Collections.Concurrent;
using StandardMQTTandMCP.Abstractions;

namespace StandardMQTTandMCP.Mqtt;

public sealed class InMemoryLatencyStore : ILatencyStore
{
    private readonly ConcurrentDictionary<string, LatencyReading> _latestReadings = new(StringComparer.Ordinal);

    public LatencyReading? ReadLatest(string topic)
    {
        return _latestReadings.TryGetValue(topic, out var reading)
            ? reading
            : null;
    }

    public void Store(LatencyReading reading)
    {
        _latestReadings[reading.Topic] = reading;
    }
}
