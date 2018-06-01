using System;
using CoCoL;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace SlimDHT
{
    /// <summary>
    /// Class that performs discovery or refresh for a number of peers
    /// </summary>
    public static class DiscoveryProcess
    {
        /// <summary>
        /// The log module
        /// </summary>
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(typeof(DiscoveryProcess));

        /// <summary>
        /// Runs the discovery process
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="self">The peer making the query.</param>
        /// <param name="endPoints">The end points to query.</param>
        public static Task RunAsync(PeerInfo self, EndPoint[] endPoints)
        {
            if (self == null)
                throw new ArgumentNullException(nameof(self));
            if (endPoints == null)
                throw new ArgumentNullException(nameof(endPoints));

            var targets = endPoints.Where(x => !self.Address.Equals(x));

            log.Debug($"Performing discovery on {targets.Count()} peer");
            return TaskPool.RunParallelAsync(
                targets, 
                x => QueryPeerAsync(self, x), 
                (x, ex) => log.Warn($"Discovery failed for peer {x}", ex)
            ).ContinueWith(x => log.Debug("Discovery process completed"));
        }

        /// <summary>
        /// Queries a single peer, trying to obtain the peer information and updates the routing table
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="self">The peer making the query.</param>
        /// <param name="endPoint">The end point to query.</param>
        private static async Task QueryPeerAsync(PeerInfo self, EndPoint endPoint)
        {
            if (self == null)
                throw new ArgumentNullException(nameof(self));
            if (endPoint == null)
                throw new ArgumentNullException(nameof(endPoint));

            if (!(endPoint is IPEndPoint))
                throw new ArgumentException($"Can only connect to an {nameof(IPEndPoint)}", nameof(endPoint));

            log.Debug($"Performing discovery on {endPoint} ...");

            var response = await Channels.ConnectionBrokerRequests.Get().SendConnectionRequestAsync(endPoint, null, 
                new Protocol.Request()
                {
                    Self = self,
                    Operation = Protocol.Operation.FindPeer,
                    Target = self.Key
                });

            log.Debug($"Discovery response arrived from {endPoint} ...");

            if (response.Exception != null)
            {
                log.DebugFormat("Failed to contact peer {0}: {1}", endPoint, response.Exception);
                return;
            }

            log.Debug($"Discovery complete for {endPoint}");
        }
    }
}
