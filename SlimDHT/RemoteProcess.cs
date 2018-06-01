using System;
using System.Threading.Tasks;
using CoCoL;

namespace SlimDHT
{
    /// <summary>
    /// A class that handles remote operations
    /// </summary>
    public static class RemoteProcess
    {
        /// <summary>
        /// The log module
        /// </summary>
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(typeof(RemoteProcess));

        /// <summary>
        /// Runs a process that handles remote requests
        /// </summary>
        /// <param name="selfinfo">Description of the owning node</param>
        /// <param name="maxparallel">The maximum number of requests to handle in parallel</param>
        /// <returns>The async.</returns>
        public static Task RunAsync(PeerInfo selfinfo, int maxparallel = 10)
        {
            return AutomationExtensions.RunTask(new
            {
                Requests = Channels.RemoteRequests.ForRead,
                Routing = Channels.RoutingTableRequests.ForWrite
            },
            async self => 
            {
                log.Debug("Running the remote handler");

                // Set up an error handler
                Func<ConnectionRequest, Exception, Task> errorHandler = async (t, ex) =>
                {
                    try
                    {
                        await t.Response.WriteAsync(new ConnectionResponse()
                        {
                            Exception = ex,
                            RequestID = t.RequestID                            
                        });
                    }
                    catch (Exception ex2)
                    {
                        log.Warn("Failed to send error response", ex2);
                    }
                };

                using(var tp = new TaskPool<ConnectionRequest>(maxparallel, errorHandler))
                while (true)
                {
                    log.Debug("Remote handler is waiting for requests ...");
                    var req = await self.Requests.ReadAsync();
                    log.Debug($"Remote handler got a {req.Request.Operation} request ({req.RequestID})");

                    await tp.Run(req, async () => {
                        log.Debug($"Remote handler is processing {req.Request.Operation} request ({req.RequestID})");
                        if (req.Key != null && req.EndPoint != null)
                        {
                            log.Debug($"Updating route table with remote request data {req.Key} - {req.EndPoint}");
                            await self.Routing.AddPeerAsync(req.Key, new PeerInfo(req.Key, req.EndPoint));
                        }

                        switch (req.Request.Operation)
                        {
                            case Protocol.Operation.Ping:
                                await HandlePingRequestAsync(selfinfo, req);
                                break;

                            case Protocol.Operation.Store:
                                await HandleStoreRequestAsync(selfinfo, req);
                                break;

                            case Protocol.Operation.FindValue:
                                await HandleFindRequestAsync(selfinfo, req);
                                break;

                            case Protocol.Operation.FindPeer:
                                await HandleNodeLookupRequestAsync(selfinfo, req);
                                break;

                            default:
                                await req.Response.WriteAsync(new ConnectionResponse()
                                {
                                    RequestID = req.RequestID,
                                    Exception = new Exception($"Invalid operation: {req.Request.Operation}")
                                });
                                break;
                        }

                        log.Debug($"Remote handler finished processing {req.Request.Operation} request ({req.RequestID})");

                    });
                }
            });
        }


        /// <summary>
        /// Handles a remote ping request
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="self">The node information.</param>
        /// <param name="req">The request to handle.</param>
        private static Task HandlePingRequestAsync(PeerInfo self, ConnectionRequest req)
        {
            log.Debug($"Converting ping request to node lookup ({req.RequestID})");

            return HandleNodeLookupRequestAsync(self, new ConnectionRequest()
            {
                Key = self.Key,
                EndPoint = self.Address,
                RequestID = req.RequestID,
                Response = req.Response,
                Request = new Protocol.Request()
                {
                    Target = self.Key,
                    Operation = Protocol.Operation.Ping
                }
            });
        }

        /// <summary>
        /// Handles a remote store request
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="self">The node information.</param>
        /// <param name="request">The request to handle.</param>
        private static async Task HandleStoreRequestAsync(PeerInfo self, ConnectionRequest request)
        {
            log.Debug($"Query MRU ({request.RequestID})");
            var res = await Channels.MRURequests.Get()
                .SendAddAsync(request.Request.Target, request.Request.Data);

            log.Debug($"Got response, forwarding to requester ({request.RequestID}) ...");
            await request.Response.WriteAsync(new ConnectionResponse()
            {
                RequestID = request.RequestID,
                Response = new Protocol.Response()
                {
                    Self = self,
                    Success = res,
                }
            });

            log.Debug($"Completed store query ({request.RequestID})");

        }

        /// <summary>
        /// Handles a remote node-lookup request
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="self">The node information.</param>
        /// <param name="request">The request to handle.</param>
        /// <param name="success">The success value reported</param>
        private static async Task HandleNodeLookupRequestAsync(PeerInfo self, ConnectionRequest request, bool success = true)
        {
            log.Debug($"Starting node lookup ({request.RequestID}) ...");

            var c = Channel.Create<RoutingResponse>();
            var res = await Channels
                .RoutingTableRequests
                .Get()
                .LookupPeerAsync(request.Request.Target);

            log.Debug($"Sending router response to requester ({request.RequestID}) ...");

            await request.Response.WriteAsync(new ConnectionResponse()
                {
                    RequestID = request.RequestID,
                    Response = new Protocol.Response()
                    {
                        Self = self,
                        Success = res.Count != 0 && success,
                        Peers = res
                    }
                }
            );

            log.Debug($"Finished find-node ({request.RequestID})");
        }

        /// <summary>
        /// Handles a remote find request
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="self">The node information.</param>
        /// <param name="request">The request to handle.</param>
        private static async Task HandleFindRequestAsync(PeerInfo self, ConnectionRequest request)
        {
            log.Debug($"Query MRU ({request.RequestID}) ...");
            var res = await Channels.MRURequests.Get()
                          .SendGetAsync(request.Request.Target);
            log.Debug($"Waiting for MRU response ({request.RequestID}) ...");

            // If we have the value, return it
            if (res != null)
            {
                log.Debug($"Forwarding MRU response ({request.RequestID}) ...");
                await request.Response.WriteAsync(new ConnectionResponse()
                {
                    RequestID = request.RequestID,
                    Response = new Protocol.Response()
                    {
                        Self = self,
                        Data = res,
                        Success = true,
                    }
                });
            }

            // Otherwise return the peers that are closest
            else
            {
                log.Debug($"Performing node lookup ({request.RequestID}) ...");
                await HandleNodeLookupRequestAsync(self, request, false);
            }

            log.Debug($"Completed value find ({request.RequestID})");

        }
    }
}
