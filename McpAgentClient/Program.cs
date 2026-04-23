using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;

internal static class Program
{
    private const string McpHttpEndpoint = "http://127.0.0.1:5000/mcp";
    private const string DefaultTopicFilter = "#";

    private static async Task<int> Main(string[] args)
    {
        Console.WriteLine("MCP Agent client (HTTP demo) starting...");

        try
        {
            await using var client = await CreateHttpMcpClientWithRetryAsync();

            Console.WriteLine($"Connected to MCP server at {McpHttpEndpoint}.");

            var tools = await client.ListToolsAsync();
            Console.WriteLine($"Available tools: {string.Join(", ", tools.Select(tool => tool.Name))}");

            var topicFilter = args.Length > 0 ? args[0] : DefaultTopicFilter;
            using var monitorCancellation = new CancellationTokenSource();
            var monitorTask = RunIncomingMessageMonitorAsync(client, topicFilter, monitorCancellation.Token);

            await ReadLatestMessagesAsync(client, topicFilter);
            await RunCommandLoopAsync(client, monitorCancellation);

            await monitorCancellation.CancelAsync();
            await WaitForMonitorShutdownAsync(monitorTask);

            Console.WriteLine("Disconnected.");
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error: {ex}");
            return 1;
        }
    }

    private static async Task<McpClient> CreateHttpMcpClientWithRetryAsync()
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await McpClient.CreateAsync(
                    new HttpClientTransport(new HttpClientTransportOptions
                    {
                        Name = "standard-mqtt-mcp",
                        Endpoint = new Uri(McpHttpEndpoint)
                    }));
            }
            catch (Exception ex) when (attempt < 30)
            {
                Console.WriteLine($"Waiting for MCP HTTP server at {McpHttpEndpoint} ({ex.GetType().Name}: {ex.Message})");
                await Task.Delay(TimeSpan.FromSeconds(1));
            }
        }
    }

    private static async Task RunDemoAsync(McpClient client)
    {
        const string topic = "test/from-agent";
        var payload = $"hello at {DateTime.UtcNow:O}";

        var sendParams = new Dictionary<string, object?>
        {
            ["mqttTopic"] = topic,
            ["payload"] = payload
        };

        Console.WriteLine("Calling send_message_to_broker...");
        var sendResult = await client.CallToolAsync("send_message_to_broker", sendParams);
        Console.WriteLine($"send_message_to_broker result: {FormatToolResult(sendResult)}");

        await Task.Delay(500);

        var readParams = new Dictionary<string, object?>
        {
            ["mqttTopic"] = topic
        };

        Console.WriteLine("Calling read_latest_mqtt_message...");
        var readResult = await client.CallToolAsync("read_latest_mqtt_message", readParams);
        Console.WriteLine($"read_latest_mqtt_message result: {FormatToolResult(readResult)}");
    }

    private static async Task RunIncomingMessageMonitorAsync(McpClient client, string topicFilter, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Monitoring incoming MQTT messages matching '{topicFilter}'.");

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var waitParams = new Dictionary<string, object?>
                {
                    ["topicFilter"] = topicFilter,
                    ["timeoutSeconds"] = 30
                };

                var result = await client.CallToolAsync(
                    "wait_for_mqtt_message",
                    waitParams,
                    cancellationToken: cancellationToken);

                var formattedResult = FormatToolResult(result);

                if (!formattedResult.StartsWith("No MQTT message was received", StringComparison.Ordinal))
                {
                    Console.WriteLine();
                    Console.WriteLine("[incoming MQTT]");
                    Console.WriteLine(formattedResult);
                    Console.Write("> ");
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.Error.WriteLine($"[monitor] {ex.GetType().Name}: {ex.Message}");
                Console.Write("> ");
                await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
            }
        }
    }

    private static async Task ReadLatestMessagesAsync(McpClient client, string topicFilter)
    {
        var readParams = new Dictionary<string, object?>
        {
            ["topicFilter"] = topicFilter,
            ["maxMessages"] = 10
        };

        var result = await client.CallToolAsync("read_latest_mqtt_messages", readParams);
        Console.WriteLine();
        Console.WriteLine($"Latest observed messages for '{topicFilter}':");
        Console.WriteLine(FormatToolResult(result));
    }

    private static async Task RunCommandLoopAsync(McpClient client, CancellationTokenSource monitorCancellation)
    {
        Console.WriteLine();
        Console.WriteLine("Client is running. Commands: demo, send <topic> <payload>, read <topic>, latest <filter>, exit");

        while (true)
        {
            Console.Write("> ");
            var line = Console.ReadLine();

            if (line is null)
            {
                Console.WriteLine("Console input is closed. Keeping the client running; stop debugging to exit.");
                await Task.Delay(Timeout.InfiniteTimeSpan);
                return;
            }

            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            var commandParts = SplitOnce(line.Trim());
            var command = commandParts.First.ToLowerInvariant();
            var rest = commandParts.Second;

            switch (command)
            {
                case "demo":
                    await RunDemoAsync(client);
                    break;

                case "send":
                    await SendMessageAsync(client, rest);
                    break;

                case "read":
                    await ReadMessageAsync(client, rest);
                    break;

                case "latest":
                    await ReadLatestMessagesAsync(client, string.IsNullOrWhiteSpace(rest) ? DefaultTopicFilter : rest);
                    break;

                case "exit":
                case "quit":
                case "q":
                    await monitorCancellation.CancelAsync();
                    return;

                case "help":
                case "?":
                    Console.WriteLine("Commands: demo, send <topic> <payload>, read <topic>, latest <filter>, exit");
                    break;

                default:
                    Console.WriteLine($"Unknown command '{command}'. Type help for commands.");
                    break;
            }
        }
    }

    private static async Task WaitForMonitorShutdownAsync(Task monitorTask)
    {
        try
        {
            await monitorTask;
        }
        catch (OperationCanceledException)
        {
        }
    }

    private static async Task SendMessageAsync(McpClient client, string input)
    {
        var parts = SplitOnce(input.Trim());

        if (string.IsNullOrWhiteSpace(parts.First) || string.IsNullOrWhiteSpace(parts.Second))
        {
            Console.WriteLine("Usage: send <topic> <payload>");
            return;
        }

        var sendParams = new Dictionary<string, object?>
        {
            ["mqttTopic"] = parts.First,
            ["payload"] = parts.Second
        };

        var result = await client.CallToolAsync("send_message_to_broker", sendParams);
        Console.WriteLine(FormatToolResult(result));
    }

    private static async Task ReadMessageAsync(McpClient client, string topic)
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            Console.WriteLine("Usage: read <topic>");
            return;
        }

        var readParams = new Dictionary<string, object?>
        {
            ["mqttTopic"] = topic.Trim()
        };

        var result = await client.CallToolAsync("read_latest_mqtt_message", readParams);
        Console.WriteLine(FormatToolResult(result));
    }

    private static (string First, string Second) SplitOnce(string value)
    {
        var separatorIndex = value.IndexOf(' ');

        return separatorIndex < 0
            ? (value, string.Empty)
            : (value[..separatorIndex], value[(separatorIndex + 1)..].TrimStart());
    }

    private static string FormatToolResult(CallToolResult result)
    {
        var content = result.Content
            .Select(block => block is TextContentBlock textBlock ? textBlock.Text : block.ToString())
            .Where(text => !string.IsNullOrWhiteSpace(text));

        var formattedContent = string.Join(Environment.NewLine, content);

        if (!string.IsNullOrWhiteSpace(formattedContent))
        {
            return formattedContent;
        }

        return result.IsError == true ? "Tool returned an error without content." : "Tool returned no content.";
    }
}
