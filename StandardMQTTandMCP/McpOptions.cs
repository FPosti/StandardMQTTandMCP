namespace StandardMQTTandMCP.McpServer;

public sealed class McpOptions
{
    public const string SectionName = "Mcp";

    public string Url { get; init; } = "http://127.0.0.1:5000";

    public string Path { get; init; } = "/mcp";

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(Url))
        {
            throw new InvalidOperationException("Mcp:Url is required.");
        }

        if (string.IsNullOrWhiteSpace(Path) || !Path.StartsWith("/", StringComparison.Ordinal))
        {
            throw new InvalidOperationException("Mcp:Path must start with '/'.");
        }
    }
}
