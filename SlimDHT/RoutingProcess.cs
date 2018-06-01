using System;
using CoCoL;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace SlimDHT
{
    /// <summary>
    /// The operations supported by the routing table
    /// </summary>
    public enum RoutingOperation
    {
        /// <summary>
        /// Look for keys in the table
        /// </summary>
        Lookup,
        /// <summary>
        /// Add a new peer to the table
        /// </summary>
        Add,
        /// <summary>
        /// Remove a peer from the table
        /// </summary>
        Remove
    }

    /// <summary>
    /// The routing table stats
    /// </summary>
    public struct RoutingStatsResponse
    {
        /// <summary>
        /// The number of peers in the cache
        /// </summary>
        public long Count;

        /// <summary>
        /// The stats if the request channel is profiled
        /// </summary>
        public string Stats;
    }

    /// <summary>
    /// Response from a routing request
    /// </summary>
    public struct RoutingResponse : IResponse
    {
        /// <summary>
        /// A flag indicating if the request succeeded
        /// </summary>
        public bool Succes;
        /// <summary>
        /// A flag indicating if the peer is new
        /// </summary>
        public bool IsNew;
        /// <summary>
        /// The peers found, or null
        /// </summary>
        public List<PeerInfo> Peers;
        /// <summary>
        /// The exception if the operation failed
        /// </summary>
        public Exception Exception { get; set; }
    }

    /// <summary>
    /// Request to the routing table
    /// </summary>
    public struct RoutingRequest : IRequestResponse<RoutingResponse>
    {
        /// <summary>
        /// The channel where responses are written to
        /// </summary>
        public IWriteChannel<RoutingResponse> Response { get; set; }
        /// <summary>
        /// The operation to perform
        /// </summary>
        public RoutingOperation Operation;
        /// <summary>
        /// The routing request key
        /// </summary>
        public Key Key;
        /// <summary>
        /// The data to add, if any
        /// </summary>
        public PeerInfo Data;
        /// <summary>
        /// Only returns results from the k-bucket
        /// </summary>
        public bool OnlyKBucket;
    }

    /// <summary>
    /// Implemenets a routing process that accepts inputs
    /// </summary>
    public static class RoutingProcess
    {
        /// <summary>
        /// The log module
        /// </summary>
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(typeof(RoutingProcess));

        /// <summary>
        /// Runs the router and forwarder
        /// </summary>
        /// <returns>An awaitable result.</returns>
        /// <param name="owner">The owner of the routing table.</param>
        /// <param name="k">The redundancy parameter.</param>
        /// <param name="buffersize">The size of the forwarding buffer.</param>
        public static Task RunAsync(PeerInfo owner, int k, int buffersize = 10)
        {
            return AutomationExtensions.RunTask(
                new
                {
                    Requests = Channels.RoutingTableRequests.ForRead,
                    Stats = Channels.RoutingTableStats.ForRead,
                    PeerReq = Channels.PeerRequests.ForWrite
                },

                async self =>
                {
                    // Setup the routing table, and add us to it
                    var table = new RoutingTable(owner.Key, k);
                    table.Add(owner);
                    log.Debug($"Router is now running");

                    // Set up the error handler
                    Func<RoutingRequest, Exception, Task> errorHandler = async (t, ex) =>
                    {
                        log.Warn("Routing operation failed, sending failure response to requester", ex);
                        try 
                        { 
                            await t.Response.WriteAsync(new RoutingResponse() { Exception = ex, Succes = false }); 
                        }
                        catch (Exception ex2) 
                        { 
                            log.Warn("Failed to forward error message", ex2); 
                        }
                    };

                    using(var tp = new TaskPool<RoutingRequest>(buffersize, errorHandler))
                    while (true)
                    {
                        // Wait for requests
                        log.Debug($"Router is waiting for requests ...");
                        var r = await MultiChannelAccess.ReadFromAnyAsync(self.Stats.RequestRead(), self.Requests.RequestRead());
                        if (r.Channel == self.Stats)
                        {
                            log.Debug($"Router got stat request");

                            var m = (IWriteChannel<RoutingStatsResponse>)r.Value;
                            await m.WriteAsync(new RoutingStatsResponse() {
                                Count = table.Count,
                                Stats = (Channels.RoutingTableRequests.Get() as ProfilingChannel<RoutingRequest>)?.ReportStats()
                            });
                            continue;
                        }

                        var data = (RoutingRequest)r.Value;
                        try
                        {
                            log.Debug($"Router got request {data.Operation}");
                            // Multiplex the operation
                            switch (data.Operation)
                            {
                                case RoutingOperation.Add:
                                {
                                    var success = table.Add(data.Data, out var isNew);
                                    if (data.Response != null)
                                        await tp.Run(data, () => data.Response.WriteAsync(new RoutingResponse() { Succes = true, IsNew = isNew  }));

                                    // If the peer is new, discover what peers it knows
                                    if (isNew)
                                    {
                                        log.Debug($"New peer, requesting refresh");
                                        await tp.Run(data, () => self.PeerReq.WriteAsync(new PeerRequest()
                                        {
                                            Operation = PeerOperation.Refresh,
                                            Key = data.Data.Key
                                        }));
                                        log.Debug($"Peer refresh requested");
                                    }

                                    break;
                                }
                                case RoutingOperation.Remove:
                                {
                                    var sucess = table.RemoveKey(data.Key);
                                    if (data.Response != null)
                                        await tp.Run(data, () => data.Response.WriteAsync(new RoutingResponse() { Succes = sucess }));
                                    break;
                                }
                                case RoutingOperation.Lookup:
                                {
                                    var peers = table.Nearest(data.Key, k, data.OnlyKBucket);
                                    await tp.Run(data, () => data.Response.WriteAsync(new RoutingResponse() { Succes = true, Peers = peers }));
                                    break;
                                }
                                default:
                                    throw new Exception($"Operation not supported: {data.Operation}");
                            }
                        }
                        catch (Exception ex)
                        {
                            await errorHandler(data, ex);
                        }
                    }
                });                
        }
    }
}
