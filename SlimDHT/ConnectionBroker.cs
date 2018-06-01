using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using CoCoL;

namespace SlimDHT
{
    /// <summary>
    /// A peer registration request
    /// </summary>
    public struct ConnectionRegistrationRequest
    {
        /// <summary>
        /// The peer has terminated the connection
        /// </summary>
        public bool IsTerminate;

        /// <summary>
        /// Updates the routing table with the peer info
        /// </summary>
        public bool UpdateRouting;

        /// <summary>
        /// The peer to change registration for
        /// </summary>
        public PeerInfo Peer;

        /// <summary>
        /// The channel used by the peer
        /// </summary>
        public IWriteChannel<ConnectionRequest> Channel;
    }

    /// <summary>
    /// Class with stats from the connection broker
    /// </summary>
    public struct ConnectionStatsResponse
    {
        /// <summary>
        /// The number of registered end points with keys
        /// </summary>
        public long Keys;
        /// <summary>
        /// The number of endpoints registered
        /// </summary>
        public long EndPoints;
        /// <summary>
        /// The request channel stats, if the channel is profiled
        /// </summary>
        public string Stats;
    }

    /// <summary>
    /// Requests connection to a peer
    /// </summary>
    public struct ConnectionRequest : IRequestResponse<ConnectionResponse>
    {
        /// <summary>
        /// The ID for the request, if any
        /// </summary>
        public long RequestID;

        /// <summary>
        /// The peer key, if known
        /// </summary>
        public Key Key;
        /// <summary>
        /// The peer endpoint
        /// </summary>
        public EndPoint EndPoint;

        /// <summary>
        /// The request to send
        /// </summary>
        public Protocol.Request Request;

        /// <summary>
        /// The channel where the response is written back
        /// </summary>
        public IWriteChannel<ConnectionResponse> Response { get; set; }
    }

    public struct ConnectionResponse : IResponse
    {
        /// <summary>
        /// The ID from the request, if any
        /// </summary>
        public long RequestID;

        /// <summary>
        /// The remote peer key
        /// </summary>
        public Key Key;

        /// <summary>
        /// The exception if the call failed
        /// </summary>
        public Exception Exception { get; set; }

        /// <summary>
        /// The response from the remote destination
        /// </summary>
        public Protocol.Response Response;
    }

    /// <summary>
    /// A broker that keeps track of all connections and keeps each connection alive
    /// </summary>
    public static class ConnectionBroker
    {
        /// <summary>
        /// The log module
        /// </summary>
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(typeof(ConnectionBroker));

        /// <summary>
        /// The size of the request buffer.
        /// </summary>
        private const int REQ_BUFFER_SIZE = 10;

        /// <summary>
        /// Runs the broker process.
        /// </summary>
        /// <param name="node">This nodes information</param>
        /// <param name="maxconnections">The maximum number of connections to allow</param>
        /// <returns>An awaitable task.</returns>
        public static Task RunAsync(PeerInfo node, int maxconnections = 50)
        {
            // The primary table for finding peers
            var peers = new Dictionary<EndPoint, Tuple<Task, IWriteChannel<ConnectionRequest>>>();

            // The peers listed by key
            var peersbykey = new Dictionary<Key, EndPoint>();

            // The MRU cache of peers
            var mrucache = new MRUCache<EndPoint, Key>(maxconnections, TimeSpan.FromDays(10));

            return AutomationExtensions.RunTask(
                new
                {
                    Request = Channels.ConnectionBrokerRequests.ForRead,
                    Registrations = Channels.ConnectionBrokerRegistrations.ForRead,
                    Stats = Channels.ConnectionBrokerStats.ForRead,
                    SelfHandler = Channels.RemoteRequests.ForWrite,
                    Routing = Channels.RoutingTableRequests.ForWrite
                },

                async self =>
                {

                    log.Debug($"Broker is now running");
                    while (true)
                    {
                        log.Debug($"Broker is waiting for requests ...");
                        var mreq = await MultiChannelAccess.ReadFromAnyAsync(
                            self.Stats.RequestRead(),
                            self.Registrations.RequestRead(),
                            self.Request.RequestRead()
                        );

                        if (mreq.Channel == self.Stats)
                        {
                            log.Debug($"Broker got stat request");
                            var req = (IWriteChannel<ConnectionStatsResponse>)mreq.Value;
                            await req.WriteAsync(new ConnectionStatsResponse()
                            {
                                EndPoints = peers.Count,
                                Keys = peersbykey.Count,
                                Stats = (Channels.ConnectionBrokerRequests.Get() as ProfilingChannel<ConnectionRequest>)?.ReportStats()
                            });
                        }
                        else if (mreq.Channel == self.Registrations)
                        {
                            var req = (ConnectionRegistrationRequest)mreq.Value;
                            log.Debug($"Broker got {(req.IsTerminate ? "termination" : "registration")} request");
                            if (req.IsTerminate)
                            {
                                // Make sure we do not have stale stuff in the MRU cache
                                if (req.Peer != null && req.Peer.Address != null)
                                    mrucache.Remove(req.Peer.Address);
                            
                                if (req.Peer.Address != null && peers.TryGetValue(req.Peer.Address, out var c) && c.Item2 == req.Channel)
                                {
                                    peers.Remove(req.Peer.Address);
                                    if (req.Peer.Key != null)
                                        peersbykey.Remove(req.Peer.Key);
                                }

                                if (req.UpdateRouting)
                                {
                                    log.Debug($"Removing peer in routing table due to termination of connection {req.Peer.Key} - {req.Peer.Address}");
                                    await self.Routing.RemovePeerAsync(req.Peer.Key);
                                }
                            }
                            else
                            {
                                if (req.Peer.Address != null && peers.TryGetValue(req.Peer.Address, out var c) && (c.Item2 == req.Channel || c == null))
                                {
                                    if (c == null)
                                        peers[req.Peer.Address] = new Tuple<Task, IWriteChannel<ConnectionRequest>>(null, req.Channel);

                                    if (!peersbykey.ContainsKey(req.Peer.Key))
                                        peersbykey[req.Peer.Key] = req.Peer.Address;
                                }

                                if (req.UpdateRouting)
                                {
                                    log.Debug($"Adding new peer to routing table {req.Peer.Key} - {req.Peer.Address}");
                                    await self.Routing.AddPeerAsync(req.Peer.Key, req.Peer);
                                }
                            }
                        }
                        else
                        {
                            var req = (ConnectionRequest)mreq.Value;
                            log.Debug($"Broker got connection request for {req.EndPoint}");

                            // Check if we request ourselves
                            if (node.Key.Equals(req.Key) || node.Address.Equals(req.EndPoint))
                            {
                                log.Debug($"Broker got self-request, forwarding to owner");
                                await self.SelfHandler.WriteAsync(req);
                                continue;
                            }

                            Tuple<Task, IWriteChannel<ConnectionRequest>> peer = null;
                            try
                            {
                                // Existing connection, update MRU
                                var overflow = mrucache.Add(req.EndPoint, req.Key);

                                // If we have too many connections, kill one now
                                if (overflow != null)
                                {
                                    // We could make this also take the closest k peers into account
                                    log.Debug($"Broker has too many connections, closing {req.EndPoint}");
                                    await peers[overflow].Item2.RetireAsync();
                                }

                                if (!peers.TryGetValue(req.EndPoint, out peer))
                                {
                                    log.Debug($"Broker is starting a connection to {req.EndPoint}");
                                    mrucache.Add(req.EndPoint, req.Key);
                                    peer = peers[req.EndPoint] =
                                        PeerConnection.CreatePeer(
                                            node,
                                            new PeerInfo(req.Key, req.EndPoint),
                                            () => ConnectToPeerAsync(req.EndPoint),
                                            REQ_BUFFER_SIZE
                                        );

                                    if (req.Key != null)
                                        peersbykey[req.Key] = req.EndPoint;
                                }

                                await peer.Item2.WriteAsync(req);
                            }
                            catch (Exception ex)
                            {
                                log.Warn("Failed to send request to peer", ex);
                                try { await req.Response.WriteAsync(new ConnectionResponse() { Exception = ex }); }
                                catch (Exception ex2) { log.Warn("Failed to write failure response", ex2); }

                                if (peer != null)
                                {
                                    try { peer.Item2.AsWriteOnly().Dispose(); }
                                    catch (Exception ex2) { log.Warn("Failed to terminate write channel", ex2); }

                                    try { await peer.Item1; }
                                    catch (Exception ex2) { log.Warn("Peer connection stopped with error", ex2); }

                                }

                                peers.Remove(req.EndPoint);
                            }
                        }
                    }
                }
            );
        }                                         

        /// <summary>
        /// Sets up a peer connection
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="endPoint">The end point to connect to.</param>
        private static async Task<Stream> ConnectToPeerAsync(EndPoint endPoint)
        {
            log.Debug($"Broker is opening a socket to {endPoint}");

            if (endPoint == null)
                throw new ArgumentNullException(nameof(endPoint));
            if (!(endPoint is IPEndPoint))
                throw new ArgumentException($"The {nameof(endPoint)} must be an {nameof(IPEndPoint)}", nameof(endPoint));

            var ip = (IPEndPoint)endPoint;
            var sock = new TcpClient();

            await sock.ConnectAsync(ip.Address, ip.Port);

            log.Debug($"Broker got socket to {endPoint}");

            return sock.GetStream();
        }
    }
}
