using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;

/// <summary>
/// Tiny HTTP streaming server.
/// Every connected HTTP client receives the same raw MPEG-TS byte stream
/// (content-type audio/mpeg or video/mp2t).  The stream is relayed without
/// any re-encoding.
/// </summary>
public sealed class HttpStreamServer : IDisposable
{
    private readonly int _port;
    private readonly string _contentType;
    private readonly ConcurrentDictionary<Guid, ConnectedClient> _clients = new();
    private TcpListener? _listener;

    public int ClientCount => _clients.Count;

    public HttpStreamServer(int port, string contentType = "audio/mpeg")
    {
        _port = port;
        _contentType = contentType;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    public async Task RunAsync(CancellationToken ct)
    {
        _listener = new TcpListener(IPAddress.Any, _port);
        _listener.Start();
        Console.WriteLine($"[HTTP] Listening on http://localhost:{_port}/");

        try
        {
            while (!ct.IsCancellationRequested)
            {
                TcpClient client = await _listener.AcceptTcpClientAsync(ct);
                _ = Task.Run(() => HandleClientAsync(client, ct), ct);
            }
        }
        catch (OperationCanceledException) { /* normal shutdown */ }
        finally
        {
            _listener.Stop();
        }
    }

    // -------------------------------------------------------------------------
    // Push data to all connected clients
    // -------------------------------------------------------------------------

    public void BroadcastData(byte[] data)
    {
        if (_clients.IsEmpty) return;

        foreach (var (id, client) in _clients)
        {
            if (!client.Enqueue(data))
            {
                // buffer overflow or closed – drop client
                if (_clients.TryRemove(id, out var removed))
                    removed.Dispose();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Per-client HTTP handling
    // -------------------------------------------------------------------------

    private async Task HandleClientAsync(TcpClient tcpClient, CancellationToken ct)
    {
        tcpClient.NoDelay = true;
        string remoteEndPoint = tcpClient.Client.RemoteEndPoint?.ToString() ?? "unknown";
        Console.WriteLine($"[HTTP] Client connected: {remoteEndPoint}");

        using (tcpClient)
        {
            NetworkStream ns = tcpClient.GetStream();

            // -- Parse HTTP request (we accept everything on /) --
            try
            {
                using var reader = new StreamReader(ns, Encoding.ASCII, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
                string? requestLine = await reader.ReadLineAsync(ct);
                if (requestLine is null) return;

                // Drain remaining request headers
                string? headerLine;
                while (!string.IsNullOrEmpty(headerLine = await reader.ReadLineAsync(ct))) { }

                // -- Send HTTP/1.0 streaming response headers --
                var headers = new StringBuilder();
                headers.Append("HTTP/1.0 200 OK\r\n");
                headers.Append($"Content-Type: {_contentType}\r\n");
                headers.Append("Connection: close\r\n");
                headers.Append("Cache-Control: no-cache\r\n");
                headers.Append("icy-name: SAT-IP Radio Stream\r\n");
                headers.Append("\r\n");

                byte[] headerBytes = Encoding.ASCII.GetBytes(headers.ToString());
                await ns.WriteAsync(headerBytes, ct);
                await ns.FlushAsync(ct);

                // -- Register client and stream data --
                var connectedClient = new ConnectedClient(ns);
                var id = Guid.NewGuid();
                _clients[id] = connectedClient;

                try
                {
                    await connectedClient.StreamAsync(ct);
                }
                catch (Exception ex) when (ex is IOException or SocketException)
                {
                    // Client closed the connection – not an error worth logging.
                }
                finally
                {
                    if (_clients.TryRemove(id, out var removed))
                        removed.Dispose();
                    Console.WriteLine($"[HTTP] Client disconnected: {remoteEndPoint}");
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                Console.WriteLine($"[HTTP] Client error ({remoteEndPoint}): {ex.Message}");
            }
        }
    }

    public void Dispose() => _listener?.Stop();

    // -------------------------------------------------------------------------
    // Inner class – one per connected client
    // -------------------------------------------------------------------------

    private sealed class ConnectedClient : IDisposable
    {
        private readonly NetworkStream _stream;
        private readonly Channel<byte[]> _channel;

        public ConnectedClient(NetworkStream stream)
        {
            _stream = stream;
            // Bounded channel: if client is too slow, old data is dropped
            _channel = Channel.CreateBounded<byte[]>(
                new BoundedChannelOptions(512)
                {
                    FullMode = BoundedChannelFullMode.DropOldest,
                    SingleReader = true,
                    SingleWriter = false
                });
        }

        /// <summary>Returns false when the channel is closed (client gone).</summary>
        public bool Enqueue(byte[] data)
        {
            if (_channel.Writer.TryWrite(data)) return true;
            // Channel closed
            return false;
        }

        public async Task StreamAsync(CancellationToken ct)
        {
            await foreach (var chunk in _channel.Reader.ReadAllAsync(ct))
            {
                await _stream.WriteAsync(chunk, ct);
            }
        }

        public void Dispose()
        {
            _channel.Writer.TryComplete();
            _stream.Close();
        }
    }
}
