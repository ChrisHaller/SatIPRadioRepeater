// SAT-IP Radio Repeater

const string RtspUrl =
    "rtsp://192.168.1.200/?src=4&fe=2&freq=10971&pol=h&ro=0.35&msys=dvbs2" +
    "&mtype=8psk&plts=off&sr=29700&fec=23&pids=213";

const int HttpPort    = 8080;   // http://localhost:8080/
const int LocalRtpPort = 5004;  // local UDP port for incoming RTP packets
// ---------------------------------------------------------------------------

Console.WriteLine("SAT-IP Radio Repeater starting…");
Console.WriteLine($"  RTSP source : {RtspUrl}");
Console.WriteLine($"  HTTP output : http://localhost:{HttpPort}/");
Console.WriteLine($"  Local RTP   : UDP {LocalRtpPort}");
Console.WriteLine();
Console.WriteLine("Press Ctrl+C to stop.");
Console.WriteLine();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

// The HTTP server relays raw MPEG-TS to all connected clients.
using var httpServer = new HttpStreamServer(HttpPort, contentType: "video/mp2t");

// The SAT-IP RTSP client pulls the stream and fires DataReceived.
var rtspClient = new SatIpRtspClient();
rtspClient.DataReceived += httpServer.BroadcastData;

var httpTask  = httpServer.RunAsync(cts.Token);
var rtspTask  = rtspClient.RunAsync(RtspUrl, LocalRtpPort, cts.Token);

await Task.WhenAll(httpTask, rtspTask);

Console.WriteLine("Stopped.");

