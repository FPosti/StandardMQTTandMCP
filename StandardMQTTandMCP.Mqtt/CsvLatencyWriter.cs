using System.Globalization;
using StandardMQTTandMCP.Abstractions;

namespace StandardMQTTandMCP.Mqtt;

public sealed class CsvLatencyWriter
{
    private readonly string _csvPath;
    private readonly Lock _csvLock = new();

    public CsvLatencyWriter(LatencyOptions options)
    {
        _csvPath = Path.GetFullPath(options.CsvPath);
    }

    public void Write(LatencyReading reading)
    {
        lock (_csvLock)
        {
            EnsureCsvFileExists();

            var row = string.Join(",",
                EscapeCsv(reading.ReceivedAtUtc.ToString("O", CultureInfo.InvariantCulture)),
                EscapeCsv(reading.SentAtUtc?.ToString("O", CultureInfo.InvariantCulture) ?? string.Empty),
                EscapeCsv(reading.LatencyMilliseconds?.ToString(CultureInfo.InvariantCulture) ?? string.Empty),
                EscapeCsv(reading.Topic),
                EscapeCsv(reading.Payload),
                EscapeCsv(reading.ParseError ?? string.Empty));

            File.AppendAllText(_csvPath, row + Environment.NewLine);
        }
    }

    private void EnsureCsvFileExists()
    {
        var directory = Path.GetDirectoryName(_csvPath);

        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        if (File.Exists(_csvPath))
        {
            return;
        }

        File.WriteAllText(_csvPath, "received_at_utc,sent_at_utc,latency_ms,topic,payload,parse_error" + Environment.NewLine);
    }

    private static string EscapeCsv(string value)
    {
        if (!value.Contains('"') && !value.Contains(',') && !value.Contains('\n') && !value.Contains('\r'))
        {
            return value;
        }

        return "\"" + value.Replace("\"", "\"\"") + "\"";
    }
}
