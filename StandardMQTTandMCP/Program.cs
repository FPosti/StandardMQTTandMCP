using ModelContextProtocol;

const string DefaultMcpHttpUrl = "http://127.0.0.1:5000";
const string McpHttpUrlEnvironmentVariable = "MCP_HTTP_URL";

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls(GetMcpHttpUrl());

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy
            .AllowAnyOrigin()
            .AllowAnyHeader()
            .AllowAnyMethod();
    });
});

builder.Services.AddSingleton<BrokerAndMcpServerApp>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<BrokerAndMcpServerApp>());

builder.Services.AddMcpServer()
    .WithHttpTransport()
    .WithToolsFromAssembly(typeof(AiAgentFacingMcpTools).Assembly);

var app = builder.Build();

app.UseCors();
app.MapGet("/health", () => Results.Ok("healthy"));
app.MapMcp("/mcp");

Console.Error.WriteLine($"[MCP HTTP] Listening at {GetMcpHttpUrl()}/mcp");

await app.RunAsync();

static string GetMcpHttpUrl()
{
    var configuredUrl = Environment.GetEnvironmentVariable(McpHttpUrlEnvironmentVariable);

    return string.IsNullOrWhiteSpace(configuredUrl)
        ? DefaultMcpHttpUrl
        : configuredUrl;
}
