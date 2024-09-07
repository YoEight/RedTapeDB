using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using RedTape.Engine;
using Serilog;
using RedTape.Node.Services;

Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .CreateLogger();

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<IMultiplexer, Multiplexer>();

// Add services to the container.
builder.Services.AddGrpc();
builder.Services.AddSerilog();

var app = builder.Build();

// Configure the HTTP request pipeline.
app.MapGrpcService<ProtocolService>();
app.MapGet("/",
    () =>
        "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");

app.Run();