using System.Net.Sockets;
using System.Text;

/// <summary>
/// Connects to a SAT-IP server via RTSP, receives the RTP/MPEG-TS stream over UDP
/// and fires <see cref="DataReceived"/> for every RTP payload (MPEG-TS data).
/// The audio is never re-encoded; raw MPEG-TS packets are forwarded as-is.
/// </summary>
public sealed class SatIpRtspClient
{
    /// <summary>Fired for every RTP packet payload (MPEG-TS bytes, RTP header already stripped).</summary>
    public event Action<byte[]>? DataReceived;

    private int _cseq = 1;

    // -------------------------------------------------------------------------
    // Public entry point
    // -------------------------------------------------------------------------

    public async Task RunAsync(string rtspUrl, int localRtpPort, CancellationToken ct)
    {
        var uri = new Uri(rtspUrl);
        string host = uri.Host;
        int port = uri.Port > 0 ? uri.Port : 554;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                _cseq = 1;
                await ConnectAndStreamAsync(rtspUrl, host, port, localRtpPort, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[SAT-IP] Error: {ex.Message}");
                Console.WriteLine("[SAT-IP] Reconnecting in 5 seconds...");
                try { await Task.Delay(5_000, ct); } catch (OperationCanceledException) { break; }
            }
        }
    }

    // -------------------------------------------------------------------------
    // RTSP session lifecycle
    // -------------------------------------------------------------------------

    private async Task ConnectAndStreamAsync(
        string rtspUrl, string host, int port, int localRtpPort, CancellationToken ct)
    {
        using var tcp = new TcpClient();
        await tcp.ConnectAsync(host, port, ct);
        Console.WriteLine($"[SAT-IP] Connected to {host}:{port}");

        var ns = tcp.GetStream();
        using var reader = new StreamReader(ns, Encoding.ASCII, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
        using var writer = new StreamWriter(ns, Encoding.ASCII, leaveOpen: true) { AutoFlush = true, NewLine = "\r\n" };

        // -- OPTIONS (probe server capabilities) --
        await SendAsync(writer, $"OPTIONS {rtspUrl} RTSP/1.0", null, null);
        await ReadResponseAsync(reader);

        // -- SETUP (request unicast RTP on localRtpPort) --
        await SendAsync(writer, $"SETUP {rtspUrl} RTSP/1.0",
            null,
            $"Transport: RTP/AVP;unicast;client_port={localRtpPort}-{localRtpPort + 1}");
        var setupHeaders = await ReadResponseAsync(reader);

        string rawSession = GetHeader(setupHeaders, "Session");
        if (string.IsNullOrWhiteSpace(rawSession))
            throw new InvalidOperationException("SETUP response contained no Session header.");

        // Session may look like "01234567;timeout=60"
        string sessionId = rawSession.Split(';')[0].Trim();
        int timeoutSeconds = 60;
        foreach (var part in rawSession.Split(';'))
        {
            if (part.Trim().StartsWith("timeout=", StringComparison.OrdinalIgnoreCase) &&
                int.TryParse(part.Trim()[8..], out int t))
            {
                timeoutSeconds = t;
                break;
            }
        }
        Console.WriteLine($"[SAT-IP] Session: {sessionId}  (timeout={timeoutSeconds}s)");

        // -- PLAY --
        await SendAsync(writer, $"PLAY {rtspUrl} RTSP/1.0",
            sessionId,
            "Range: npt=0.000-");
        await ReadResponseAsync(reader);
        Console.WriteLine("[SAT-IP] Stream started – receiving RTP packets.");

        // -- Parallel: RTP receive + RTSP keep-alive --
        using var udp = new UdpClient(localRtpPort);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var rtpTask = ReceiveRtpLoopAsync(udp, linkedCts.Token);
        var keepAliveTask = KeepAliveLoopAsync(writer, reader, rtspUrl, sessionId,
            TimeSpan.FromSeconds(timeoutSeconds / 2), linkedCts.Token);

        // If either task ends (error or cancellation) cancel the other one
        var completed = await Task.WhenAny(rtpTask, keepAliveTask);
        linkedCts.Cancel();

        try { await Task.WhenAll(rtpTask, keepAliveTask); }
        catch (OperationCanceledException) { /* expected on shutdown */ }

        // Re-throw original error (if not just a cancellation)
        if (completed.IsFaulted)
            await completed; // propagates exception ? triggers reconnect

        // -- TEARDOWN --
        try
        {
            await SendAsync(writer, $"TEARDOWN {rtspUrl} RTSP/1.0", sessionId, null);
        }
        catch { /* best-effort */ }
    }

    // -------------------------------------------------------------------------
    // RTP receive loop
    // -------------------------------------------------------------------------

    private async Task ReceiveRtpLoopAsync(UdpClient udp, CancellationToken ct)
    {
        long packetCount = 0;
        long byteCount   = 0;

        while (!ct.IsCancellationRequested)
        {
            var result = await udp.ReceiveAsync(ct);
            var payload = StripRtpHeader(result.Buffer);
            if (payload is { Length: > 0 })
            {
                DataReceived?.Invoke(payload);
                packetCount++;
                byteCount += payload.Length;

                // Log first packet and then every 1000 packets as a heartbeat.
                if (packetCount == 1 || packetCount % 1000 == 0)
                    Console.WriteLine($"[SAT-IP] RTP packets: {packetCount}, bytes forwarded: {byteCount:N0}");
            }
        }
    }

    /// <summary>
    /// Strips the fixed + variable RTP header (RFC 3550) and returns the payload.
    /// MPEG-TS over RTP uses no padding and rarely extensions, but we handle both.
    /// </summary>
    private static byte[]? StripRtpHeader(byte[] data)
    {
        if (data.Length < 12) return null;

        int version = (data[0] >> 6) & 0x3;
        if (version != 2) return null;                    // not a valid RTP packet

        bool hasPadding   = (data[0] & 0x20) != 0;
        bool hasExtension = (data[0] & 0x10) != 0;
        int  csrcCount    = data[0] & 0x0F;

        int headerLen = 12 + csrcCount * 4;

        if (hasExtension)
        {
            if (data.Length < headerLen + 4) return null;
            int extWordCount = (data[headerLen + 2] << 8) | data[headerLen + 3];
            headerLen += 4 + extWordCount * 4;
        }

        if (data.Length <= headerLen) return null;

        int payloadLen = data.Length - headerLen;

        if (hasPadding && payloadLen > 0)
            payloadLen -= data[^1]; // last byte is padding count

        if (payloadLen <= 0) return null;

        return data[headerLen..(headerLen + payloadLen)];
    }

    // -------------------------------------------------------------------------
    // RTSP keep-alive
    // -------------------------------------------------------------------------

    private async Task KeepAliveLoopAsync(
        StreamWriter writer, StreamReader reader,
        string rtspUrl, string sessionId,
        TimeSpan interval, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await Task.Delay(interval, ct);
            await SendAsync(writer, $"OPTIONS {rtspUrl} RTSP/1.0", sessionId, null);
            await ReadResponseAsync(reader);
        }
    }

    // -------------------------------------------------------------------------
    // RTSP helpers
    // -------------------------------------------------------------------------

    private async Task SendAsync(StreamWriter writer, string requestLine, string? session, string? extraHeader)
    {
        var sb = new StringBuilder();
        sb.AppendLine(requestLine);
        sb.AppendLine($"CSeq: {_cseq++}");
        sb.AppendLine("User-Agent: SatIPRadioRepeater/1.0");
        if (session is not null)
            sb.AppendLine($"Session: {session}");
        if (extraHeader is not null)
            sb.AppendLine(extraHeader);
        sb.AppendLine(); // blank line = end of request

        await writer.WriteAsync(sb.ToString());
    }

    /// <summary>Reads a complete RTSP response and returns its headers as a dictionary.</summary>
    private static async Task<Dictionary<string, string>> ReadResponseAsync(StreamReader reader)
    {
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        // Status line (e.g. "RTSP/1.0 200 OK")
        string? statusLine = await reader.ReadLineAsync();
        if (statusLine is null)
            throw new IOException("RTSP connection closed by server.");

        // Header lines until blank line
        string? line;
        while (!string.IsNullOrEmpty(line = await reader.ReadLineAsync()))
        {
            int colon = line.IndexOf(':');
            if (colon > 0)
                headers[line[..colon].Trim()] = line[(colon + 1)..].Trim();
        }

        // Consume optional body
        if (headers.TryGetValue("Content-Length", out var clStr) &&
            int.TryParse(clStr, out int cl) && cl > 0)
        {
            var body = new char[cl];
            await reader.ReadBlockAsync(body, 0, cl);
        }

        return headers;
    }

    private static string GetHeader(Dictionary<string, string> headers, string name)
        => headers.TryGetValue(name, out var v) ? v : string.Empty;
}
