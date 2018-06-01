using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using CoCoL;

namespace SlimDHT
{
    /// <summary>
    /// The operations supported by a peer from a non-peer
    /// </summary>
    public enum PeerOperation
    {
        /// <summary>
        /// Finds the data for the given key
        /// </summary>
        Find,
        /// <summary>
        /// Adds a key/value pair to the DHT
        /// </summary>
        Add,
        /// <summary>
        /// Returns local node stats
        /// </summary>
        Stats,
        /// <summary>
        /// Refreshes the routing table
        /// </summary>
        Refresh,
    }

    /// <summary>
    /// Represents a request to the local node
    /// </summary>
    public class PeerRequest
    {
        /// <summary>
        /// The operation requested
        /// </summary>
        public PeerOperation Operation;

        /// <summary>
        /// The key to find or store data with
        /// </summary>
        public Key Key;

        /// <summary>
        /// The data if the operation is a <see cref="PeerOperation.Add"/>
        /// </summary>
        public byte[] Data;

        /// <summary>
        /// The channel where the response is returned to
        /// </summary>
        public IWriteChannel<PeerResponse> Response;
    }

    /// <summary>
    /// Represents a response from the local node
    /// </summary>
    public class PeerResponse
    {
        /// <summary>
        /// The data from the lookup, if any
        /// </summary>
        public byte[] Data;

        /// <summary>
        /// The number of peers that reported success for the operation
        /// </summary>
        public int SuccessCount;
    }

    /// <summary>
    /// A peer in the DHT
    /// </summary>
    public static class Peer
    {
        /// <summary>
        /// The log module
        /// </summary>
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(typeof(Peer));

        /// <summary>
        /// Runs the peer process
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="selfinfo">The information describing the peer.</param>
        /// <param name="k">The redundancy parameter.</param>
        /// <param name="storesize">The maximum size of the local store</param>
        /// <param name="maxage">The maximum age of items in the cache</param>
        /// <param name="initialContactlist">Initial list of peers to contact.</param>
        /// <param name="requests">The request channel for local management</param>
        public static async Task RunPeer(PeerInfo selfinfo, int k, int storesize, TimeSpan maxage, EndPoint[] initialContactlist, IReadChannel<PeerRequest> requests)
        {
            try
            {
                if (selfinfo == null)
                    throw new ArgumentNullException(nameof(selfinfo));
                if (initialContactlist == null)
                    throw new ArgumentNullException(nameof(initialContactlist));

                var ip = selfinfo.Address as IPEndPoint;
                if (ip == null)
                    throw new ArgumentException($"Unable to convert {nameof(selfinfo.Address)} to a {nameof(IPEndPoint)}", nameof(selfinfo));

                log.Debug($"Starting a peer with key {selfinfo.Key} and address {selfinfo.Address}");

                using (var scope = new IsolatedChannelScope())
                {
                    var sock = new TcpListener(ip);
                    sock.Start();

                    // Set up the helper processes
                    var router = RoutingProcess.RunAsync(selfinfo, k);
                    var broker = ConnectionBroker.RunAsync(selfinfo);
                    var values = MRUProcess.RunAsync(selfinfo, storesize, maxage);
                    var remoter = RemoteProcess.RunAsync(selfinfo);

                    log.Debug("Started router, broker, and value store");

                    // Handle new connections
                    var listener = ListenAsync(selfinfo, sock);

                    log.Debug("Started listener");

                    // Start discovery of peers
                    var discovery = DiscoveryProcess.RunAsync(selfinfo, initialContactlist);

                    log.Debug("Started discovery");

                    // The process handling requests to the local node
                    var proc = AutomationExtensions.RunTask(
                        new
                        {
                            Requests = requests,
                            Refresh = Channels.PeerRequests.ForRead,

                            // Add these, so this process terminates the others as well
                            BrokerReg = Channels.ConnectionBrokerRegistrations.ForWrite,
                            BrokerReq = Channels.ConnectionBrokerRequests.ForWrite,
                            BrokerStat = Channels.ConnectionBrokerStats.ForWrite,
                            MRUReq = Channels.MRURequests.ForWrite,
                            MRUStat = Channels.MRUStats.ForWrite,
                            RouteReq = Channels.RoutingTableRequests.ForWrite,
                            RouteStat = Channels.RoutingTableStats.ForWrite,
                        },
                        async self =>
                        {
                            log.Debug("Running peer main loop");

                            try
                            {
                                while (true)
                                {
                                    var req = (await MultiChannelAccess.ReadFromAnyAsync(self.Requests, self.Refresh)).Value;
                                    log.Debug($"Peer {selfinfo.Key} got message: {req.Operation}");
                                    switch (req.Operation)
                                    {
                                        case PeerOperation.Add:
                                            await HandleAddOperation(selfinfo, req, k);
                                            break;
                                        case PeerOperation.Find:
                                            await HandleFindOperation(selfinfo, req, k);
                                            break;
                                        case PeerOperation.Stats:
                                            await HandleStatsOperation(req);
                                            break;
                                        case PeerOperation.Refresh:
                                            await HandleRefreshOperation(selfinfo, req, k);
                                            break;
                                        default:
                                            await req.Response.WriteAsync(new PeerResponse()
                                            {
                                                SuccessCount = -1
                                            });
                                            break;
                                    }
                                    log.Debug($"Peer {selfinfo.Key} handled message: {req.Operation}");
                                }
                            }
                            catch (Exception ex)
                            {
                                if (!ex.IsRetiredException())
                                    log.Warn($"Terminating peer {selfinfo.Key} due to error", ex);
                                throw;
                            }
                            finally
                            {
                                log.Debug($"Terminating peer {selfinfo.Key}");
                            }
                        }
                    );

                    log.Debug("Started main handler");

                    // Set up a process that periodically emits refresh operations
                    var refresher = AutomationExtensions.RunTask(
                        new { Control = Channels.PeerRequests.ForWrite },
                        async self =>
                        {
                            var respchan = Channel.Create<PeerResponse>();
                            while (true)
                            {
                                // Sleep, but exit if the parent does
                                if (await Task.WhenAny(Task.Delay(TimeSpan.FromMinutes(10)), proc) == proc)
                                    return;
                            
                                await self.Control.WriteAsync(new PeerRequest()
                                {
                                    Operation = PeerOperation.Refresh,
                                    Response = respchan
                                });
                                await respchan.ReadAsync();
                            }
                        }
                    );

                    log.Debug("Started refresh process, peer is now live");
                    await proc;
                    await router;
                    await broker;
                    await values;
                    await remoter;
                    await discovery;
                    await refresher;

                    await Task.WhenAll(router, broker, values, remoter, discovery, refresher);
                }
            }
            catch (Exception ex)
            {
                log.Warn("Failed to start peer", ex);

                try { await requests.RetireAsync(); }
                catch(Exception ex2) { log.Warn("Failed to stop the input channel", ex2); }

                log.Debug($"Peer with key {selfinfo.Key} and address {selfinfo.Address} stopped...");

                throw;
            }
        }

        /// <summary>
        /// Listens to the socket and registers all new requests
        /// </summary>
        /// <returns>The async.</returns>
        /// <param name="selfinfo">Selfinfo.</param>
        /// <param name="sock">Sock.</param>
        private static Task ListenAsync(PeerInfo selfinfo, TcpListener sock)
        {
            // Set up a channel for sending sockets
            var sockchan = Channel.Create<TcpClient>();
            var listener = Task.Run(async () =>
            {
                while (true)
                    await sockchan.WriteAsync(await sock.AcceptTcpClientAsync());
            });

            // Handle each request
            var handlers = Skeletons.CollectAsync(sockchan, async client => {
                var peer = PeerConnection.CreatePeer(selfinfo, selfinfo, client.GetStream());

                // Ping the peer
                var res = await peer.Item2.SendConnectionRequestAsync(null, null, 
                    new Protocol.Request()
                    {
                        Operation = Protocol.Operation.Ping,
                        Self = selfinfo,

                    });

                // Register this peer with the broker
                await Channels.ConnectionBrokerRegistrations.Get().WriteAsync(
                    new ConnectionRegistrationRequest()
                    {
                        IsTerminate = false,
                        UpdateRouting = true,
                        Peer = res.Response.Self,
                        Channel = peer.Item2
                    }
                );

                // If we get anything back, register with the routing table
                if (res.Response.Peers != null)
                {
                    var router = Channels.RoutingTableRequests.Get();

                    log.Debug($"Ping response had {res.Response.Peers.Count} peers, registering with routing table");
                    foreach (var np in res.Response.Peers)
                        await router.AddPeerAsync(np.Key, np);
                }

                log.Debug("Completed initial ping sequence");
                    
            }, 10);

            return Task.WhenAll(listener, handlers);
        }

        /// <summary>
        /// Handles the stats operation
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="request">The request to handle.</param>
        public static async Task HandleStatsOperation(PeerRequest request)
        {
            var brokerchan = Channel.Create<ConnectionStatsResponse>();
            var mruchan = Channel.Create<MRUStatResponse>();
            var routechan = Channel.Create<RoutingStatsResponse>();

            var brokertask = Task.Run(brokerchan.ReadAsync);
            var mrutask = Task.Run(mruchan.ReadAsync);
            var routetask = Task.Run(routechan.ReadAsync);

            // Send all requests
            await Task.WhenAll(
                Task.Run(() => Channels.ConnectionBrokerStats.Get().WriteAsync(brokerchan)),
                Task.Run(() => Channels.MRUStats.Get().WriteAsync(mruchan)),
                Task.Run(() => Channels.RoutingTableStats.Get().WriteAsync(routechan))
            );

            // Then wait for all results
            await Task.WhenAll(
                brokertask,
                mrutask,
                routetask
            );

            var sb = new StringBuilder();
            foreach(var n in new object[] { await brokertask, await mrutask, await routetask })
            {
                sb.AppendLine(string.Format("{0}:", n.GetType().Name));
                foreach (var p in n.GetType().GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.DeclaredOnly | System.Reflection.BindingFlags.Instance))
                    sb.AppendLine(string.Format("{0}: {1}", p.Name, p.GetValue(n)));
            }

            await request.Response.WriteAsync(new PeerResponse() { Data = Encoding.UTF8.GetBytes(sb.ToString()) });
        }

        /// <summary>
        /// Gets the <paramref name="count"/> closest live nodes to the <paramref name="key"/>.
        /// </summary>
        /// <returns>The closest live peers.</returns>
        /// <param name="selfinfo">Selfinfo.</param>
        /// <param name="key">The key to search for.</param>
        public static Task<List<PeerInfo>> GetClosestPeers(PeerInfo selfinfo, Key key, int count)
        {
            return Channels.RoutingTableRequests.Get().LookupPeerAsync(key);
        }

        /// <summary>
        /// Visits the <paramref name="k"/> closest peers and sends the <paramref name="request"/> message to them
        /// </summary>
        /// <returns>The number of nodes visited.</returns>
        /// <param name="selfinfo">Selfinfo.</param>
        /// <param name="key">The key to use for findng the closest nodes.</param>
        /// <param name="k">The redundancy parameter.</param>
        /// <param name="succes_count">The number of success responses to find</param>
        /// <param name="request">The request to send.</param>
        private static async Task<List<Protocol.Response>> VisitClosestPeers(PeerInfo selfinfo, Key key, int k, int succes_count, Protocol.Request request)
        {
            KeyDistance closesttried = null;

            var peers = await GetClosestPeers(selfinfo, key, k);
            log.Debug($"Initial query gave {peers.Count} peers");

            var used = new HashSet<Key>();

            var lck = new object();
            var closest = new List<PeerInfo>();

            var cb = Channels.ConnectionBrokerRequests.Get();

            var success = new List<Protocol.Response>();

            using (var tp = new TaskPool<PeerInfo>(2, (p, ex) => log.Warn($"Request to peer {p.Key} - {p.Address} failed", ex)))
            while (success.Count < succes_count && (peers.Count + closest.Count > 0))
            {
                peers = closest
                    .Union(peers)
                    // Remove dead items
                    .Where(x => !used.Contains(x.Key))
                    // Always move closer to the target for find
                    .Where(x => closesttried == null || (new KeyDistance(key, x.Key).CompareTo(closesttried)) <= 0)
                    // Sort by distance 
                    .OrderBy(x => new KeyDistance(key, x.Key))
                    .ToList();

                // Clean the list
                closest.Clear();

                log.Debug($"Sending request to {peers.Count} peers");

                // Get the peers
                var alloperations =
                    peers.Select(x => tp.Run(x, async () =>
                    {
                        log.Debug($"Success count {success.Count}, target: {succes_count}");
                        if (success.Count >= succes_count)
                            return;

                        log.Debug("Sending request via broker");
                        var r = cb.SendConnectionRequestAsync(x.Address, x.Key, request);

                        // Don't try this again
                        lock (lck)
                            used.Add(x.Key);

                        log.Debug($"Waiting for broker response {x.Key}...");
                        var res = await r;
                        log.Debug($"Peer response for {x.Key} obtained ({(res.Exception != null ? "exception": (res.Response.Success ? "success" : "no hit"))})");

                        // Skip error nodes
                        if (res.Exception != null)
                        {
                            log.Warn("Node request failed", res.Exception);
                            //await Channels.ConnectionBrokerRegistrations.Get().WriteAsync(new ConnectionRegistrationRequest() {
                            //    IsTerminate = true,
                            //    UpdateRouting = true,
                            //    Peer = x
                            //});
                                            
                            return;
                        }
                        // Record success
                        if (res.Response.Success)
                        {
                            lock (lck)
                                success.Add(res.Response);
                        }
                        
                        // Stock up on peers, if any
                        if (res.Response.Peers != null)
                        {
                            lock (lck)
                                closest.AddRange(res.Response.Peers);
                        }

                        // If we are doing a find, narrow the scope
                        if (request.Operation == Protocol.Operation.FindValue)
                        {
                            if (closesttried == null || new KeyDistance(key, x.Key).CompareTo(closesttried) < 0)
                                closesttried = new KeyDistance(key, x.Key);                        
                        }
                    }));

                await Task.WhenAll(alloperations);
                await tp.FinishedAsync();

                log.Debug($"Got {closest.Count} potential new peers");

                if (closest.Count == 0)
                    break;
            }

            return success;
        }

        /// <summary>
        /// Handles a request to add a value to the DHT
        /// </summary>
        /// <returns>The add operation.</returns>
        /// <param name="selfinfo">Selfinfo.</param>
        /// <param name="request">The request to handle.</param>
        /// <param name="k">The number of copies to store</param>
        public static async Task HandleAddOperation(PeerInfo selfinfo, PeerRequest request, int k)
        {
            var key = Key.ComputeKey(request.Data);
            log.Debug($"Handling the add request");
            var stored = await VisitClosestPeers(selfinfo, key, k, k, new Protocol.Request()
            {
                Self = selfinfo,
                Operation = Protocol.Operation.Store,
                Data = request.Data,
                Target = key
            });

            await request.Response.WriteAsync(new PeerResponse() { 
                SuccessCount = stored.Count
            });
        }

        /// <summary>
        /// Handles the find operation
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="selfinfo">The nodes own information.</param>
        /// <param name="request">The request to handle.</param>
        /// <param name="k">The redundancy count</param>
        public static async Task HandleFindOperation(PeerInfo selfinfo, PeerRequest request, int k)
        {
            // Query the local value store first
            var data = await Channels.MRURequests.Get().SendGetAsync(request.Key);
            if (data != null)
            {
                log.Debug($"Local lookup for key succeeded");
                await request.Response.WriteAsync(new PeerResponse()
                {
                    Data = data,
                    SuccessCount = 1
                });
                return;
            }

            var res = await VisitClosestPeers(selfinfo, request.Key, k, 1, new Protocol.Request()
            {
                Self = selfinfo,
                Operation = Protocol.Operation.FindValue,
                Target = request.Key
            });

            var result = res.Where(x => x.Success).FirstOrDefault();
            if (result.Success)
            {
                log.Debug($"Lookup succeeded for key, (re-)adding to local table");
                await Channels.MRURequests.Get().SendAddAsync(request.Key, result.Data);
            }

            await request.Response.WriteAsync(new PeerResponse() { 
                Data = result.Data,
                SuccessCount = res.Count
            });
        }

        /// <summary>
        /// Handles a refresh operation by requesting routing data from peers
        /// </summary>
        /// <returns>The refresh operation.</returns>
        /// <param name="selfinfo">The nodes own information.</param>
        /// <param name="request">Request.</param>
        /// <param name="k">The number of peers to request</param>
        public static async Task HandleRefreshOperation(PeerInfo selfinfo, PeerRequest request, int k)
        {
            var res = await VisitClosestPeers(selfinfo, request.Key ?? selfinfo.Key, request.Key == null ? k : 1, 1, new Protocol.Request()
            {
                Self = selfinfo,
                Operation = Protocol.Operation.FindPeer,
                Target = selfinfo.Key
            });

            if (request.Response != null)
                await request.Response.WriteAsync(new PeerResponse()
                {
                    SuccessCount = res.Count
                });
        }
    }
}
