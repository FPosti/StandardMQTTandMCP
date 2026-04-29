namespace StandardMQTTandMCP.Mqtt;

public sealed class LatencyOptions
{
    public const string SectionName = "Latency";

    public string CsvPath { get; init; } = "latency.csv";

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(CsvPath))
        {
            throw new InvalidOperationException("Latency:CsvPath is required.");
        }
    }
}
