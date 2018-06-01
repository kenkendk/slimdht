using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using CoCoL;

namespace SlimDHT
{
    /// <summary>
    /// An interface for the request part of a request-response message
    /// </summary>
    public interface IRequestResponse<T>
        where T : IResponse
    {
        /// <summary>
        /// Gets or sets the response channel.
        /// </summary>
        IWriteChannel<T> Response { get; set; }
    }

    /// <summary>
    /// An interface for the response part of a request-response message
    /// </summary>
    public interface IResponse
    {
        /// <summary>
        /// Gets or sets the exception on this response.
        /// </summary>
        /// <value>The exception.</value>
        Exception Exception { get; set; }
    }

    /// <summary>
    /// A list of helpers that hide the channel communication part
    /// </summary>
    public static class SendHelpers
    {
        /// <summary>
        /// Sends a request message, and waits for a response
        /// </summary>
        /// <returns>The message result.</returns>
        /// <param name="channel">The channel to send the request over.</param>
        /// <param name="request">The request to send.</param>
        /// <typeparam name="TIn">The request type parameter.</typeparam>
        /// <typeparam name="TOut">The response type parameter.</typeparam>
        private static async Task<TOut> SendMessageAsync<TIn, TOut>(IWriteChannel<TIn> channel, TIn request)
            where TIn : IRequestResponse<TOut> 
            where TOut : IResponse
        {
            var resp = Channel.Create<TOut>();
            var tres = resp.ReadAsync();
            request.Response = resp;

            await channel.WriteAsync(request);

            var res = await tres;
            if (res.Exception != null)
                throw res.Exception;

            return res;
        }

        /// <summary>
        /// Sends a response to a request.
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="request">The request to respond to.</param>
        /// <param name="response">The response to send.</param>
        /// <typeparam name="TIn">The request type parameter.</typeparam>
        /// <typeparam name="TOut">The response type parameter.</typeparam>
        private static Task SendResponseAsync<TIn, TOut>(TIn request, TOut response)
            where TIn : IRequestResponse<TOut>
            where TOut : IResponse
        {
            return request.Response == null
                ? Task.FromResult(true)
                : request.Response.WriteAsync(response);
        }

        /// <summary>
        /// Adds a value to the MRU cache and long-term storage
        /// </summary>
        /// <returns>The result of the add operation.</returns>
        /// <param name="channel">The channel to send the request over.</param>
        /// <param name="key">The key of the data to add.</param>
        /// <param name="data">The data to add.</param>
        public static async Task<bool> SendAddAsync(this IWriteChannel<MRURequest> channel, Key key, byte[] data)
        {
            return (await SendMessageAsync<MRURequest, MRUResponse>(channel, new MRURequest()
            {
                Operation = MRUOperation.Add,
                Key = key,
                Data = data

            })).Success;
        }

        /// <summary>
        /// Sends an expire request
        /// </summary>
        /// <returns>The result of the expire operation.</returns>
        /// <param name="channel">The request channel.</param>
        public static async Task<bool> SendExpireAsync(this IWriteChannel<MRURequest> channel)
        {
            return (await SendMessageAsync<MRURequest, MRUResponse>(channel, new MRURequest() { Operation = MRUOperation.Expire })).Success;
        }

        /// <summary>
        /// Requests a value from the MRU cache or value store
        /// </summary>
        /// <returns>The result or null.</returns>
        /// <param name="channel">The channel to place the request on.</param>
        /// <param name="key">The key to get the data for.</param>
        public static async Task<byte[]> SendGetAsync(this IWriteChannel<MRURequest> channel, Key key)
        {
            var res = await SendMessageAsync<MRURequest, MRUResponse>(channel, new MRURequest() { 
                Operation = MRUOperation.Get,
                Key = key
            });
            return res.Success ? res.Data : null;
        }

        /// <summary>
        /// Sends a response to a request
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="request">The request to respond to.</param>
        /// <param name="key">The key for the response.</param>
        /// <param name="data">The data for the response.</param>
        /// <param name="success">The success flag</param>
        public static Task SendResponseAsync(this MRURequest request, Key key, byte[] data, bool success = true)
        {
            return SendResponseAsync(request, new MRUResponse()
            {
                Success = success,
                Data = data,
                Key = key
            });
        }

        /// <summary>
        /// Sends an error response
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="request">The request to respond to.</param>
        /// <param name="exception">The exception to send.</param>
        public static Task SendResponseAsync(this MRURequest request, Exception exception)
        {
            return SendResponseAsync(request, new MRUResponse()
            {
                Success = false,
                Exception = exception
            });
        }

        /// <summary>
        /// Sends a request and waits for the response
        /// </summary>
        /// <returns>The connection request async.</returns>
        /// <param name="channel">The channel to send the request over</param>
        /// <param name="endPoint">End point.</param>
        /// <param name="key">Key.</param>
        /// <param name="request">Request.</param>
        public static Task<ConnectionResponse> SendConnectionRequestAsync(this IWriteChannel<ConnectionRequest> channel, EndPoint endPoint, Key key, Protocol.Request request)
        {
            return SendConnectionRequestAsync(channel, endPoint, key, 0, request);
        }
                
        /// <summary>
        /// Sends a request and waits for the response
        /// </summary>
        /// <returns>The connection request async.</returns>
        /// <param name="channel">The channel to send the request over</param>
        /// <param name="endPoint">End point.</param>
        /// <param name="key">Key.</param>
        /// <param name="requestId">The request ID</param>
        /// <param name="request">Request.</param>
        public static Task<ConnectionResponse> SendConnectionRequestAsync(this IWriteChannel<ConnectionRequest> channel, EndPoint endPoint, Key key, long requestId, Protocol.Request request)
        {
            return SendMessageAsync<ConnectionRequest, ConnectionResponse>(channel, new ConnectionRequest()
            {
                EndPoint = endPoint,
                Key = key,
                RequestID = requestId,
                Request = request
            });
        }

        /// <summary>
        /// Adds a peer to the routing table
        /// </summary>
        /// <returns><c>true</c> if the peer is new, <c>false</c> otherwise.</returns>
        /// <param name="channel">The channel to send the request on.</param>
        /// <param name="key">The key for the peer.</param>
        /// <param name="peer">The peer.</param>
        public static async Task<bool> AddPeerAsync(this IWriteChannel<RoutingRequest> channel, Key key, PeerInfo peer)
        {
            return (await SendMessageAsync<RoutingRequest, RoutingResponse>(channel, new RoutingRequest()
            {
                Operation = RoutingOperation.Add,
                Key = key,
                Data = peer
            })).IsNew;
        }

        /// <summary>
        /// Removes the peer with the given key from the routing table.
        /// </summary>
        /// <returns>The peer to remove.</returns>
        /// <param name="channel">The channel to send the request on.</param>
        /// <param name="key">The key for the peer to remove.</param>
        public static Task RemovePeerAsync(this IWriteChannel<RoutingRequest> channel, Key key)
        {
            return SendMessageAsync<RoutingRequest, RoutingResponse>(channel, new RoutingRequest()
            {
                Operation = RoutingOperation.Remove,
                Key = key
            });
        }

        /// <summary>
        /// Looks up peers in the routing table
        /// </summary>
        /// <returns>The peers.</returns>
        /// <param name="channel">The channel to send the request on</param>
        /// <param name="onlyKbucket"><c>true</c> if the response must originate from the closest bucket, <c>false</c> otherwise.</param>
        /// <param name="key">The key to return the results for.</param>
        public static async Task<List<PeerInfo>> LookupPeerAsync(this IWriteChannel<RoutingRequest> channel, Key key, bool onlyKbucket = false)
        {
            return (await SendMessageAsync<RoutingRequest, RoutingResponse>(channel, new RoutingRequest()
            {
                Operation = RoutingOperation.Lookup,
                Key = key,
                OnlyKBucket = onlyKbucket
            })).Peers;
        }
    }
}
