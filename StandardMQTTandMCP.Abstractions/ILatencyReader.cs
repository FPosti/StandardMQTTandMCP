namespace StandardMQTTandMCP.Abstractions;

public interface ILatencyReader
{
    LatencyReading? ReadLatest(string topic);
}
