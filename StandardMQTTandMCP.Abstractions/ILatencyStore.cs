namespace StandardMQTTandMCP.Abstractions;

public interface ILatencyStore : ILatencyReader
{
    void Store(LatencyReading reading);
}
