using System;
using System.Collections.Generic;

namespace SlimDHT.Protocol
{
    /// <summary>
    /// The operations that can be performed
    /// </summary>
    public enum Operation
    {
        /// <summary>
        /// Pings the peer
        /// </summary>
        Ping,
        /// <summary>
        /// Stores a value in the DHT
        /// </summary>
        Store,
        /// <summary>
        /// Finds a peer
        /// </summary>
        FindPeer,
        /// <summary>
        /// Retrieves a value in the DHT
        /// </summary>
        FindValue
    }

    /// <summary>
    /// Represents a remote request
    /// </summary>
    public struct Request
    {
        /// <summary>
        /// The operation to perform
        /// </summary>
        public Operation Operation;
        /// <summary>
        /// The node making the request
        /// </summary>
        public PeerInfo Self;
        /// <summary>
        /// The item we attempt to locate
        /// </summary>
        public Key Target;
        /// <summary>
        /// The data, if this is a store request
        /// </summary>
        public byte[] Data;
    }

    /// <summary>
    /// Represents a remote response
    /// </summary>
    public struct Response
    {
        /// <summary>
        /// The node making the response
        /// </summary>
        public PeerInfo Self;
        /// <summary>
        /// A value indicating if the operation succeeded
        /// </summary>
        public bool Success;
        /// <summary>
        /// The data returned, if any
        /// </summary>
        public byte[] Data;
        /// <summary>
        /// The list of peers
        /// </summary>
        public List<PeerInfo> Peers;
    }
}
