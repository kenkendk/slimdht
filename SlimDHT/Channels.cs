using System;
using System.Collections.Generic;
using CoCoL;

namespace SlimDHT
{
    /// <summary>
    /// Shared channel definitions
    /// </summary>
    public static class Channels
    {
        /// <summary>
        /// The channel used to handle remote requests
        /// </summary>
        public static readonly ChannelMarkerWrapper<ConnectionRequest> RemoteRequests = new ChannelMarkerWrapper<ConnectionRequest>(nameof(RemoteRequests));

        /// <summary>
        /// The channel used to send console output
        /// </summary>
        public static readonly ChannelMarkerWrapper<string> ConsoleOutput = new ChannelMarkerWrapper<string>(nameof(ConsoleOutput), buffersize: 10, targetScope: ChannelNameScope.Global);

        /// <summary>
        /// Requests to the routing table manager
        /// </summary>
        public static readonly ChannelMarkerWrapper<RoutingRequest> RoutingTableRequests = new ChannelMarkerWrapper<RoutingRequest>(nameof(RoutingTableRequests));

        /// <summary>
        /// Requests the routing table stats
        /// </summary>
        public static readonly ChannelMarkerWrapper<IWriteChannel<RoutingStatsResponse>> RoutingTableStats = new ChannelMarkerWrapper<IWriteChannel<RoutingStatsResponse>>(nameof(RoutingTableStats));

        /// <summary>
        /// Sends requests to the connection broker
        /// </summary>
        public static readonly ChannelMarkerWrapper<ConnectionRequest> ConnectionBrokerRequests = new ChannelMarkerWrapper<ConnectionRequest>(nameof(ConnectionBrokerRequests));

        /// <summary>
        /// Sends registration requests to the broker
        /// </summary>
        public static readonly ChannelMarkerWrapper<ConnectionRegistrationRequest> ConnectionBrokerRegistrations = new ChannelMarkerWrapper<ConnectionRegistrationRequest>(nameof(ConnectionBrokerRegistrations));

        /// <summary>
        /// The channel used to request connection broker stats
        /// </summary>
        public static readonly ChannelMarkerWrapper<IWriteChannel<ConnectionStatsResponse>> ConnectionBrokerStats = new ChannelMarkerWrapper<IWriteChannel<ConnectionStatsResponse>>(nameof(ConnectionBrokerStats));

        /// <summary>
        /// Requests to the local MRU table
        /// </summary>
        public static readonly ChannelMarkerWrapper<MRURequest> MRURequests = new ChannelMarkerWrapper<MRURequest>(nameof(MRURequests));

        /// <summary>
        /// Channel used to obtain MRU stats
        /// </summary>
        public static readonly ChannelMarkerWrapper<IWriteChannel<MRUStatResponse>> MRUStats = new ChannelMarkerWrapper<IWriteChannel<MRUStatResponse>>(nameof(MRUStats));

        /// <summary>
        /// The channel used to request actions from the peer
        /// </summary>
        public static readonly ChannelMarkerWrapper<PeerRequest> PeerRequests = new ChannelMarkerWrapper<PeerRequest>(nameof(PeerRequest));
    }
}
