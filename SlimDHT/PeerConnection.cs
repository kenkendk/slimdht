using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using CoCoL;

namespace SlimDHT
{
	/// <summary>
    /// Handler class for running a single connection
    /// </summary>
    public static class PeerConnection
    {
        /// <summary>
        /// The log module
        /// </summary>
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(typeof(PeerConnection));

        /// <summary>
        /// The size of the request buffer.
        /// </summary>
        private const int REQ_BUFFER_SIZE = 10;

        /// <summary>
        /// Creates a new peer for the given endpoint
        /// </summary>
        /// <returns>A task for the peer, and a communication channel.</returns>
        /// <param name="self">This peer's information</param>
        /// <param name="remote">The remote peer's information</param>
        /// <param name="stream">The stream to use.</param>
        /// <param name="maxparallel">The maximum number of requests to handle in parallel</param>
        public static Tuple<Task, IWriteChannel<ConnectionRequest>> CreatePeer(PeerInfo self, PeerInfo remote, Stream stream, int maxparallel = REQ_BUFFER_SIZE)
        {
            return CreatePeer(self, remote, () => Task.FromResult(stream), maxparallel);
        }

        /// <summary>
        /// Creates a new peer for the given endpoint
        /// </summary>
        /// <returns>A task for the peer, and a communication channel.</returns>
        /// <param name="self">This peer's information</param>
        /// <param name="remote">The remote peer's information</param>
        /// <param name="connecthandler">The method used to obtain the stream</param>
        /// <param name="maxparallel">The maximum number of requests to handle in parallel</param>
        public static Tuple<Task, IWriteChannel<ConnectionRequest>> CreatePeer(PeerInfo self, PeerInfo remote, Func<Task<Stream>> connecthandler, int maxparallel = REQ_BUFFER_SIZE)
        {
            if (self == null)
                throw new ArgumentNullException(nameof(self));
            if (connecthandler == null)
                throw new ArgumentNullException(nameof(connecthandler));
            
            var source = Channel.Create<ConnectionRequest>();
            var sink = Channel.Create<ConnectionRequest>();

            return new Tuple<Task, IWriteChannel<ConnectionRequest>>(
                Task.WhenAll(
                    Task.Run(() => RunSingleConnection(self, remote, connecthandler, sink.AsRead(), maxparallel)),
                    TaskPool.RunParallelAsync(
                        source.AsRead(),
                        x => sink.WriteAsync(x),
                        REQ_BUFFER_SIZE,
                        async (c, x) =>
                        {
                            try { await c.Response.WriteAsync(new ConnectionResponse() { Exception = x }); }
                            catch (Exception ex) { log.Warn("Failed to send error response", ex); }
                        }
                    )
                ),
                source.AsWrite()
            );
        }
        /// <summary>
        /// Runs a single peer connection, using the IPC link
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="self">This peer's information</param>
        /// <param name="remote">The remote peer's information</param>
        /// <param name="connecthandler">The method used to obtain the connection.</param>
        /// <param name="input">The channel for reading requests.</param>
        /// <param name="maxparallel">The maximum number of parallel handlers</param>
        private static async Task RunSingleConnection(PeerInfo self, PeerInfo remote, Func<Task<Stream>> connecthandler, IReadChannel<ConnectionRequest> input, int maxparallel)
        {
            // Get the local handler for remote requests
            var remotehandler = Channels.RemoteRequests.Get();

            LeanIPC.IPCPeer connection = null;

            try
            {
                if (connecthandler == null)
                    throw new ArgumentNullException(nameof(connecthandler));
                if (input == null)
                    throw new ArgumentNullException(nameof(input));

                log.Debug($"Setting up connection to {remote?.Key}");

                // Connect to the remote peer
                connection = new LeanIPC.IPCPeer(await connecthandler());

                // Setup a handler for remote requests, that forwards responses from the remote handler
                connection.AddUserTypeHandler<Protocol.Request>(
                    async (id, req) =>
                    {
                        await connection.SendResponseAsync(id, (await remotehandler.SendConnectionRequestAsync(null, null, id, req)).Response);
                        return true;
                    }
                );

                var mainTask = connection.RunMainLoopAsync(self != remote);
                Key targetKey = null;

                // Grab a connection to update the routing table automatically
                var routingrequests = Channels.RoutingTableRequests.Get();

                log.Debug($"Peer connection running {self.Key}, {self.Address}");

                using (var tp = new TaskPool<ConnectionRequest>(maxparallel, (t, ex) => log.Warn("Unexpected error handling request", ex)))
                while (true)
                {
                    log.Debug($"Peer connection is waiting for request ...");

                    // Get either a local or a remote request
                    var req = await input.ReadAsync();

                    log.Debug($"Peer connection got request, handling on taskpool ...");

                    await tp.Run(req, () =>
                    {
                        log.Debug($"Peer connection is forwarding a local {req.Request.Operation} request to the remote");
                        return Task.Run(async () =>
                        {
                            ConnectionResponse res;

                            try
                            {
                                var p = await connection.SendAndWaitAsync<Protocol.Request, Protocol.Response>(req.Request);
                                if (targetKey == null)
                                {
                                    // Record the target key
                                    targetKey = p.Self.Key;
                                    if (remote == null || remote.Key == null)
                                        remote = new PeerInfo(p.Self.Key, remote.Address);

                                    // Write a registration request to the broker
                                    await Channels.ConnectionBrokerRegistrations.Get().WriteAsync(
                                        new ConnectionRegistrationRequest() 
                                        { 
                                            IsTerminate = false, 
                                            UpdateRouting = true, 
                                            Peer = remote 
                                        }
                                    );

                                    log.Debug($"Registering peer in routing table: {remote.Key} {remote.Address} ...");
                                    await routingrequests.AddPeerAsync(remote.Key, remote);
                                }

                                if (p.Peers != null)
                                {
                                    log.Debug($"Registering {p.Peers.Count} peers with the routing table ...");
                                    foreach (var peer in p.Peers)
                                        await routingrequests.AddPeerAsync(peer.Key, peer);
                                    
                                    log.Debug($"Registered {p.Peers.Count} peers with the routing table");
                                }

                                res = new ConnectionResponse()
                                {
                                    Key = p.Self.Key,
                                    Response = p
                                };
                            }
                            catch (Exception ex)
                            {
                                log.Warn($"Failed to get result, sending error response", ex);
                                res = new ConnectionResponse()
                                {
                                    Key = targetKey,
                                    Exception = ex
                                };

                                log.Warn($"Killing peer due to the previous exception");
                                await input.RetireAsync();
                            }

                            if (req.Response != null)
                            {
                                try { await req.Response.WriteAsync(res); }
                                catch (Exception ex) { log.Warn("Failed to send response", ex); }
                            }
                        });
                    });
                }
            }
            finally
            {
                await remotehandler.RetireAsync();

                if (connection != null)
                    try { await connection.ShutdownAsync(); }
                    catch (Exception ex) { log.Warn("Failed to shut down IPC Peer", ex); }

                // Write a registration request to the broker
                await Channels.ConnectionBrokerRegistrations.Get().WriteAsync(
                    new ConnectionRegistrationRequest() 
                    { 
                        IsTerminate = false, 
                        UpdateRouting = false,
                        Peer = remote 
                    }
                );
            }
        }


    }
}
