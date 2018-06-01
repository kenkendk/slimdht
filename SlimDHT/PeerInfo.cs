using System;
using System.Net;

namespace SlimDHT
{
	/// <summary>
    /// Describes a peer contact
    /// </summary>
    public class PeerInfo : IEquatable<PeerInfo>
    {
		/// <summary>
        /// The peer key
        /// </summary>
		public readonly Key Key;
        /// <summary>
        /// The address for the peer
        /// </summary>
		public readonly EndPoint Address;
        /// <summary>
        /// The last activity from the peer
        /// </summary>
		public DateTime LastHeartBeat;

        /// <summary>
        /// Initializes a new instance of the <see cref="T:SlimDHT.PeerInfo"/> class.
        /// </summary>
        /// <param name="key">The peer key.</param>
        /// <param name="endPoint">The IP address for the peer.</param>
		public PeerInfo(Key key, EndPoint endPoint)
        {
			Key = key;
			Address = endPoint ?? throw new ArgumentNullException(nameof(endPoint));
			LastHeartBeat = DateTime.Now;
        }

        /// <summary>
        /// Determines whether the specified <see cref="object"/> is equal to the current <see cref="T:SlimDHT.PeerInfo"/>.
        /// </summary>
        /// <param name="obj">The <see cref="object"/> to compare with the current <see cref="T:SlimDHT.PeerInfo"/>.</param>
        /// <returns><c>true</c> if the specified <see cref="object"/> is equal to the current <see cref="T:SlimDHT.PeerInfo"/>;
        /// otherwise, <c>false</c>.</returns>
        public override bool Equals(object obj)
        {
            return Equals(obj as PeerInfo);
        }

        /// <summary>
        /// Serves as a hash function for a <see cref="T:SlimDHT.PeerInfo"/> object.
        /// </summary>
        /// <returns>A hash code for this instance that is suitable for use in hashing algorithms and data structures such as a
        /// hash table.</returns>
        public override int GetHashCode()
        {
            return (Key == null ? 0 : Key.GetHashCode()) ^ (Address == null ? 0 : Address.GetHashCode());
        }

        /// <summary>
        /// Determines whether the specified <see cref="SlimDHT.PeerInfo"/> is equal to the current <see cref="T:SlimDHT.PeerInfo"/>.
        /// </summary>
        /// <param name="other">The <see cref="SlimDHT.PeerInfo"/> to compare with the current <see cref="T:SlimDHT.PeerInfo"/>.</param>
        /// <returns><c>true</c> if the specified <see cref="SlimDHT.PeerInfo"/> is equal to the current
        /// <see cref="T:SlimDHT.PeerInfo"/>; otherwise, <c>false</c>.</returns>
        public bool Equals(PeerInfo other)
        {
            if (other == null)
                return false;

            return
                (Key == other.Key || (Key != null && Key.Equals(other.Key)) || (other.Key != null && other.Key.Equals(Key)))
                &&
                (Address == other.Address || (Address != null && Address.Equals(other.Address)) || (other.Address != null && other.Address.Equals(Address)))
            ;
        }
    }
}
