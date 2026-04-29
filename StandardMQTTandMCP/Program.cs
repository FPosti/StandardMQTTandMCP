using ModelContextProtocol;
using StandardMQTTandMCP.McpServer;
using StandardMQTTandMCP.Mqtt;

var builder = WebApplication.CreateBuilder(args);

var mcpOptions = builder.Configuration
    .GetSection(McpOptions.SectionName)
    .Get<McpOptions>() ?? new McpOptions();

mcpOptions.Validate();

var mqttOptions = builder.Configuration
    .GetSection(MqttOptions.SectionName)
    .Get<MqttOptions>() ?? new MqttOptions();

var latencyOptions = builder.Configuration
    .GetSection(LatencyOptions.SectionName)
    .Get<LatencyOptions>() ?? new LatencyOptions();

builder.WebHost.UseUrls(mcpOptions.Url);

builder.Services.AddSingleton(mcpOptions);
builder.Services.AddMqttLatencyServices(mqttOptions, latencyOptions);

builder.Services.AddMcpServer()
    .WithHttpTransport()
    .WithTools<LatencyTools>();

var app = builder.Build();

app.MapMcp(mcpOptions.Path);

Console.Error.WriteLine($"[MCP HTTP] Listening at {mcpOptions.Url}{mcpOptions.Path}");

await app.RunAsync();
