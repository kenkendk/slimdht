using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CoCoL;
namespace SlimDHT
{
    /// <summary>
    /// The operations supported by the MRU cache
    /// </summary>
    public enum MRUOperation
    {
        /// <summary>
        /// Adds a value to the cache
        /// </summary>
        Add,
        /// <summary>
        /// Attempts to get a value from the cache
        /// </summary>
        Get,
        /// <summary>
        /// Expires items from the cache
        /// </summary>
        Expire,
    }

    /// <summary>
    /// The stats from the MRU
    /// </summary>
    public class MRUStatResponse
    {
        /// <summary>
        /// The number of items in the cache
        /// </summary>
        public long Items;

        /// <summary>
        /// The oldest item in the cache
        /// </summary>
        public DateTime Oldest;

        /// <summary>
        /// The size of all items in the cache
        /// </summary>
        public long Size;

        /// <summary>
        /// The stats from the request channel if it supports profiling
        /// </summary>
        public string Stats;
    }

    /// <summary>
    /// Represents a request to the MRU cache
    /// </summary>
    public struct MRURequest : IRequestResponse<MRUResponse>
    {
        /// <summary>
        /// The operation to perform
        /// </summary>
        public MRUOperation Operation;

        /// <summary>
        /// The key of the item to get or add
        /// </summary>
        public Key Key;

        /// <summary>
        /// The data to add, if this is an add request
        /// </summary>
        public byte[] Data;

        /// <summary>
        /// The response channel
        /// </summary>
        public IWriteChannel<MRUResponse> Response { get; set; }


    }

    /// <summary>
    /// Represents a response from the MRU
    /// </summary>
    public struct MRUResponse : IResponse
    {
        /// <summary>
        /// The key from the request
        /// </summary>
        public Key Key;
        /// <summary>
        /// A flag indicating if the operation succeeded
        /// </summary>
        public bool Success;
        /// <summary>
        /// The data, if this was a get request and the data was found
        /// </summary>
        public byte[] Data;
        /// <summary>
        /// The exception if the operation failed
        /// </summary>
        public Exception Exception { get; set; }
    }

    /// <summary>
    /// Runs a process that exposes the MRU cache
    /// </summary>
    public static class MRUProcess
    {
        /// <summary>
        /// An internal request used to update the long-term storage table
        /// </summary>
        private struct MRUInternalStore
        {
            /// <summary>
            /// The peers to broadcast to, if this is a new item
            /// </summary>
            public List<PeerInfo> Peers;

            /// <summary>
            /// The key of the item to get or add
            /// </summary>
            public Key Key;

            /// <summary>
            /// The data to add, if this is an add request
            /// </summary>
            public byte[] Data;

        }

        /// <summary>
        /// The log module
        /// </summary>
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(typeof(MRUProcess));

        /// <summary>
        /// Runs the MRU cache
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="selfinfo">This peer's information</param>
        /// <param name="storesize">The size of the MRU store</param>
        /// <param name="maxage">The maximum amount of time items are stored</param>
        /// <param name="buffersize">The size of the forwarding buffer.</param>
        public static Task RunAsync(PeerInfo selfinfo, int storesize, TimeSpan maxage, int buffersize = 10)
        {
            var parent = RunMRUAsync(selfinfo, storesize, maxage, buffersize);
            return Task.WhenAll(
                parent,

                AutomationExtensions.RunTask(
                    new { Request = Channels.MRURequests.ForWrite },
                    async self =>
                    {
                        while (true)
                        {
                            // Sleep, but quit if the MRU stops
                            if (await Task.WhenAny(parent, Task.Delay(new TimeSpan(maxage.Ticks / 3))) == parent)
                                return;
                            log.Debug("Invoking store expiration");
                            await self.Request.SendExpireAsync();
                            log.Debug("Store expiration completed, waiting ...");
                        }
                    }
                )
            );
        }

        /// <summary>
        /// Runs the MRU cache
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="selfinfo">This peer's information</param>
        /// <param name="storesize">The size of the MRU store</param>
        /// <param name="maxage">The maximum amount of time items are stored</param>
        /// <param name="buffersize">The size of the parallel processing buffer</param>
        private static Task RunMRUAsync(PeerInfo selfinfo, int storesize, TimeSpan maxage, int buffersize)
        {
            var storechan = Channel.Create<MRUInternalStore>();
            return AutomationExtensions.RunTask(new
            {
                Request = Channels.MRURequests.ForRead,
                Routing = Channels.RoutingTableRequests.ForWrite,
                Stats = Channels.MRUStats.ForRead,
                Store = storechan.AsRead()
            },
            async self =>
            {
                var cache = new MRUCache<Key, byte[]>(storesize, maxage);
                var store = new MRUCache<Key, byte[]>(int.MaxValue, maxage);

                log.Debug($"Store is now running");

                // Set up a shared error handler for logging and reporting errors
                Func<MRURequest, Exception, Task> errorHandler = async (req, ex) =>
                {
                    log.Warn("Failed to process request, sending error", ex);
                    try { await req.SendResponseAsync(ex); }
                    catch (Exception ex2) { log.Warn("Failed to forward error report", ex2); }
                };

                using (var tp = new TaskPool<MRURequest>(buffersize, errorHandler))
                while (true)
                {
                    log.Debug($"Store is waiting for requests ...");
                    var mreq = await MultiChannelAccess.ReadFromAnyAsync(
                        self.Stats.RequestRead(), 
                        self.Store.RequestRead(),
                        self.Request.RequestRead()
                    );

                    if (mreq.Channel == self.Stats)
                    {
                        log.Debug($"Store got stat request");
                        var r = (IWriteChannel<MRUStatResponse>)mreq.Value;

                        await r.WriteAsync(new MRUStatResponse()
                        {
                            Items = cache.Count + store.Count,
                            Oldest = new DateTime(Math.Min(cache.OldestItem.Ticks, store.OldestItem.Ticks)),
                            Size = cache.Select(x => x.Value.Length).Sum() + store.Select(x => x.Value.Length).Sum(),
                            Stats = (Channels.MRURequests.Get() as ProfilingChannel<MRURequest>)?.ReportStats()
                        });
                        continue;
                    }

                    if (mreq.Channel == self.Store)
                    {
                        var sreq = (MRUInternalStore)mreq.Value;

                        log.Debug($"Store got internal store request");
                        var shouldBroadCast = sreq.Peers != null && !store.TryGetValue(sreq.Key, out _);
                        store.Add(sreq.Key, sreq.Data);

                        // We currently rely on the injector to broadcast,
                        // If we enable this, we need some logic to figure out
                        // the source of the Add, to both allow re-insertion
                        // and avoid repeated broadcasts if two peers determine
                        // they are *the* handling peer

                        //if (shouldBroadCast)
                            //await tp.Run(new MRURequest() { }, () => BroadcastValueAsync(selfinfo, sreq));
                            
                        continue;
                    }

                    var req = (MRURequest)mreq.Value;
                    log.Debug($"Store got request {req.Operation}");
                    try
                    {
                        switch (req.Operation)
                        {
                            case MRUOperation.Add:
                            {
                                // Always store it in our cache    
                                cache.Add(req.Key, req.Data);
                                
                                // Process long-term if needed
                                await tp.Run(req, () => StoreLongTermAsync(selfinfo, self.Routing, storechan.AsWrite(), req.Key, req.Data));

                                // Respond that we completed
                                await tp.Run(req, () => req.SendResponseAsync(req.Key, null));
                                break;
                            }

                            case MRUOperation.Get:
                            {
                                var res = cache.TryGetValue(req.Key, out var data);
                                if (!res)
                                    res = store.TryGetValue(req.Key, out data);
                                    
                                await tp.Run(req, () => req.SendResponseAsync(req.Key, data, res));
                                break;
                            }
                            case MRUOperation.Expire:
                                cache.ExpireOldItems();
                                store.ExpireOldItems();
                                await tp.Run(req, () => req.SendResponseAsync(null, null));
                                break;
                            default:
                                throw new Exception($"Unable to handle request with type {req.Operation}");
                        }
                        log.Debug($"Store completed request {req.Operation}");
                    }
                    catch (Exception ex)
                    {
                        await errorHandler(req, ex);
                    }
                }
            });
        }

        /// <summary>
        /// Stores the value in long-term storage if required, and broadcasts the value, 
        /// if we are the closest peer
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="self">The node's own information</param>
        /// <param name="router">The router request channel</param>
        /// <param name="key">The key for the data.</param>
        /// <param name="data">The data to store.</param>
        /// <param name="storechan">The channel pointing back to the store process</param>
        private static async Task StoreLongTermAsync(PeerInfo self, IWriteChannel<RoutingRequest> router, IWriteChannel<MRUInternalStore> storechan, Key key, byte[] data)
        {
            // We could use a small cache of peers here
            // to avoid hammering the routing process
            // for each insert

            log.Debug($"Checking if {key} should be inserted into long-term storage for {self.Key}");
            var list = await router.LookupPeerAsync(key, true);

            // If we are in the k-bucket closest peers, store it
            if (list.Any(x => self.Key.Equals(x.Key)))
            {
                // Check if we are the closest node
                var closest = self.Key.Equals(list.OrderBy(x => new KeyDistance(key, x.Key)).First().Key);

                log.Debug($"We are {(closest ? "*the* peer": "one of the peers")} for {key}");
                await storechan.WriteAsync(new MRUInternalStore()
                {
                    Key = key,
                    Data = data,
                    Peers = closest ? list : null
                });
            }
            else
            {
                log.Debug($"We are not one of the closest peers for {key}");
            }

        }

        /// <summary>
        /// Broadcasts the store operation to the peers.
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="request">The internal request to handle.</param>
        private static async Task BroadcastValueAsync(PeerInfo self, MRUInternalStore request)
        {
            log.Debug($"Broadcasting store to {request.Peers.Count - 1} peers");

            var tp = new TaskPool<PeerInfo>(5, (x, ex) => log.Warn($"Failed to broadcast to peer {x.Key} - {x.Address}", ex));
            var cb = Channels.ConnectionBrokerRequests.Get();

            foreach (var n in request.Peers.Where(x => !self.Key.Equals(x.Key)))
            {
                // Send a store request to all the k nearest peers
                await tp.Run(n, peer => cb.SendConnectionRequestAsync(peer.Address, peer.Key, 
                    new Protocol.Request()
                    {
                        Operation = Protocol.Operation.Store,
                        Data = request.Data,
                        Target = request.Key,
                        Self = self
                    }
                ));
            }
        }
    }
}
