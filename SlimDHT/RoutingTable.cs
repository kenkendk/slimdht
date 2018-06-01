using System;
using System.Collections.Generic;
using System.Linq;

namespace SlimDHT
{
	/// <summary>
    /// Class for representing a routing table
    /// </summary>
    public class RoutingTable
    {
		/// <summary>
        /// A node in the tree
        /// </summary>
		private class Node
		{
			/// <summary>
            /// The bit to use for choosing left or right
            /// </summary>
			public int SplitBit;
                        
			/// <summary>
            /// The items in this tree, if the node is a leaf
            /// </summary>
			public List<PeerInfo> Items;

            /// <summary>
            /// The parent node, or null if this is the root
            /// </summary>
			public Node Parent;

            /// <summary>
            /// The left side, if this node is not a leaf
            /// </summary>
			public Node Left;
			/// <summary>
            /// The right side, if this node is not a leaf
            /// </summary>
            public Node Right;
                        
			/// <summary>
            /// Constructs a new node with the given elements as content
            /// </summary>
            /// <param name="splitBit">The first bit in the key prefix</param>
			public Node(Node parent, int splitBit)
			{
				Parent = parent;
				SplitBit = splitBit;
				Items = new List<PeerInfo>();
			}
		}

		/// <summary>
		/// The root node
		/// </summary>
		private Node m_root = new Node(null, 0);
        
		/// <summary>
        /// The redundancy parameter
        /// </summary>
		private readonly int m_k;

        /// <summary>
        /// The node ID
        /// </summary>
		private readonly Key m_nodeid;

        /// <summary>
        /// The number of elements in the routing table
        /// </summary>
		private int m_count;

        /// <summary>
        /// Gets the number of peers in the table
        /// </summary>
        /// <value>The peers in the table.</value>
		public int Count => m_count;

		/// <summary>
        /// Initializes a new instance of the <see cref="T:SlimDHT.RoutingTable"/> class.
        /// </summary>
		/// <param name="nodeid">The key of the node</param>
        /// <param name="k">The redundancy parameter.</param>
        public RoutingTable(Key nodeid, int k)
        {
			m_k = k;
			m_nodeid = nodeid;
        }

        /// <summary>
        /// Gets the leaf node closest to the target
        /// </summary>
        /// <returns>The closest leaf node.</returns>
        /// <param name="target">The key to look for.</param>
		private Node GetClosestLeaf(Key target)
		{
			var node = m_root;
            while (node.Items == null)
                node = PrefixMatches(node, target) ? node.Right : node.Left;

			return node;
		}

        /// <summary>
        /// Adds a peer to the routing table
        /// </summary>
        /// <returns><c>true</c> if the item was added; <c>false</c> otherwise.</returns>
        /// <param name="peer">Peer.</param>
        public bool Add(PeerInfo peer)
        {
            return Add(peer, out var isNew);
        }

        /// <summary>
        /// Adds a peer to the routing table
        /// </summary>
		/// <returns><c>true</c> if the item was added; <c>false</c> otherwise.</returns>
        /// <param name="peer">Peer.</param>
        /// <param name="isNew">A flag indicating if the item was new</param>
		public bool Add(PeerInfo peer, out bool isNew)
		{
			if (peer == null)
				throw new ArgumentNullException(nameof(peer));

            isNew = false;

			// Get the closest leaf node
			var node = GetClosestLeaf(peer.Key);

			// Check if we are re-inserting a known key
			for (var i = 0; i < node.Items.Count; i++)
                if (node.Items[i].Key.Equals(peer.Key))
				{
				    // Are we simply re-freshing the peer?
                    if (node.Items[i].Address.Equals(peer.Address))
					{
                        // Refreshing, move the item to the top
						node.Items.RemoveAt(i);
						node.Items.Add(peer);
						return true;
					}

                    // We are not replacing keys
					return false;
				}

            // Keep splitting until we have space
			while (true)
            {
				// Do we have space?
				if (node.Items.Count == m_k)
				{
					// No space, but should we make space?
					if (PrefixMatches(node, peer.Key) || node == m_root)
					{
						SplitNode(node);
						node = PrefixMatches(node, peer.Key) ? node.Right : node.Left;
						continue;
					}

                    // No more space
					return false;
				}


                // We have space, so just add it
				node.Items.Add(peer);
				m_count++;
                isNew = true;
				return true;
			}
		}

		/// <summary>
        /// Checks if the key matches the prefix
        /// </summary>
        /// <returns><c>true</c>, if the prefix matches, <c>false</c> otherwise.</returns>
        /// <param name="key">The key to test agains the prefix.</param>
        private bool PrefixMatches(Node node, Key key)
        {
			var el = (node.SplitBit) / Key.ELBITS;
			var bit = (node.SplitBit) % Key.ELBITS;
			return (m_nodeid[el] & (1uL << bit)) != (key[el] & (1uL << bit));
        }

        /// <summary>
        /// Splits the node, moving elements into sub-nodes.
        /// </summary>
        /// <param name="item">Item.</param>
		private void SplitNode(Node item)
		{
			item.Left = new Node(
				item,
				item.SplitBit + 1
			);

			item.Right = new Node(
				item,
				item.SplitBit + 1
			);

			foreach (var n in item.Items)
				(PrefixMatches(item, n.Key) ? item.Right : item.Left).Items.Add(n);

			item.Items = null;
		}

        /// <summary>
		/// Recursively finding the <paramref name="k"/> peers nearest to <paramref name="target"/>
        /// </summary>
		/// <returns>The k nearest peers.</returns>
        /// <param name="startNode">The node to start the search from.</param>
        /// <param name="target">The target key.</param>
        /// <param name="k">The number of results to return.</param>
        /// <param name="onlyfrombucket">Only return results from the closest bucket</param>
		private List<PeerInfo> Nearest(Node startNode, Key target, int k, bool onlyfrombucket)
		{
			if (startNode.Items == null)
			{
                var left = Nearest(startNode.Left, target, k, onlyfrombucket);
                var right = Nearest(startNode.Right, target, k, onlyfrombucket);

                // If the item is not in the bucket, return an empty bucket
                if (onlyfrombucket)
                {
                    // If both have results, pick the bucket that is closest
                    if (left.Count != 0 & right.Count != 0)
                    {
                        var leftdist = left.Select(x => new KeyDistance(target, x.Key)).OrderBy(x => x).First();
                        var rightdist = right.Select(x => new KeyDistance(target, x.Key)).OrderBy(x => x).First();
                        return leftdist.CompareTo(rightdist) < 0 ? left : right;
                    }
                    else if (left.Count != 0)
                        return left;
                    else if (right.Count != 0)
                        return right;

                    // Empty on both sides, return empty
                    return left;
                }
                else
                {
                    if (left.Count + right.Count > k)
                        return new List<PeerInfo>(left.Concat(right).OrderBy(x => new KeyDistance(target, x.Key)).Take(k));
                    else
                        return new List<PeerInfo>(left.Concat(right));
                }
			}
			else
			{
				// If we have too many, pick the closest ones
				if (k < startNode.Items.Count)
					return new List<PeerInfo>(startNode.Items.OrderBy(x => new KeyDistance(target, x.Key)).Take(k));
				else
					return new List<PeerInfo>(startNode.Items);
			}
		}

		/// <summary>
        /// Gets the <paramref name="k"/> peers nearest to the key
        /// </summary>
        /// <returns>The k nearest peers.</returns>
        /// <param name="target">The target key.</param>
        /// <param name="k">The number of results to return.</param>
        /// <param name="onlyfrombucket">Only returns results from the first bucket we hit</param>
		public List<PeerInfo> Nearest(Key target, int k, bool onlyfrombucket = false)
		{
            return Nearest(m_root, target, k, onlyfrombucket);
		}

        /// <summary>
        /// Removes the entry with the target key
        /// </summary>
        /// <returns><c>true</c>, if key was removed, <c>false</c> otherwise.</returns>
        /// <param name="target">The key to remove.</param>
		public bool RemoveKey(Key target)
		{
			var node = GetClosestLeaf(target);
			for (var i = 0; i < node.Items.Count; i++)
			{
                if (node.Items[i].Key.Equals(target))
				{
					node.Items.RemoveAt(i);
					m_count--;
					// TODO: Compact tree?
					return true;
				}
			}

			return false;
		}
    }
}
