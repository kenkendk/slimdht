using System;
using System.Collections.Generic;
using System.Linq;

namespace SlimDHT
{
	/// <summary>
    /// Represents the distance between two keys
    /// </summary>
	public class KeyDistance : IComparable<KeyDistance>, IEquatable<KeyDistance>
    {
		/// <summary>
        /// The number of elements used to hold the key
        /// </summary>
		private const int KEYLEN = Key.KEYLEN;

		/// <summary>
        /// The zero distance
        /// </summary>
		public static readonly KeyDistance ZERO = new KeyDistance(Key.ZERO, Key.ZERO);

        /// <summary>
        /// The distance element
        /// </summary>
		private readonly Key m_distance;

        /// <summary>
        /// Initializes a new instance of the <see cref="T:SlimDHT.Key.KeyDistance"/> class.
        /// </summary>
        /// <param name="a">One key.</param>
        /// <param name="b">Another key.</param>
        public KeyDistance(Key a, Key b)
        {
            if (a == null)
                throw new ArgumentNullException(nameof(a));
            if (b == null)
                throw new ArgumentNullException(nameof(b));

			var zero = true;
			var res = new ulong[KEYLEN];

			for (var i = 0; i < KEYLEN; i++)
			{
				res[i] = a[i] ^ b[i];
				zero &= res[i] == 0;
			}

			m_distance = new Key(res);
			SignBit = m_distance.SignBit;
            IsZero = m_distance.IsZero;
        }

        /// <summary>
        /// Constructs a KeyDistance from the string representation
        /// </summary>
        /// <param name="value">The string to parse.</param>
		public KeyDistance(string value)
		{
			m_distance = new Key(value);
			SignBit = m_distance.SignBit;
			IsZero = m_distance.IsZero;
		}

		/// <summary>
		/// Gets the most significant bit of this key
		/// </summary>
		/// <value><c>true</c> if signed; otherwise, <c>false</c>.</value>
		public readonly bool SignBit;

        /// <summary>
        /// Gets a value indicating if the value is zero
        /// </summary>
		public readonly bool IsZero;

        /// <summary>
        /// Returns a value indicating the relation between two distances
        /// </summary>
        /// <returns>The distance.</returns>
        /// <param name="other">The distance to compare with.</param>
        public int CompareTo(KeyDistance other)
        {
			for (var i = 0; i < KEYLEN; i++)
			{
				if (this.m_distance[i] == other.m_distance[i])
					continue;

				if (this.m_distance[i] < other.m_distance[i])
					return -1;
				else
					return 1;
			}

			return 0;
        }

		/// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="T:SlimDHT.KeyDistance"/>.
        /// </summary>
		/// <returns>A <see cref="T:System.String"/> that represents the current <see cref="T:SlimDHT.KeyDistance"/>.</returns>
        public override string ToString()
        {
			return m_distance.ToString();
        }

        /// <summary>
		/// Determines whether the specified <see cref="object"/> is equal to the current <see cref="T:SlimDHT.KeyDistance"/>.
        /// </summary>
		/// <param name="obj">The <see cref="object"/> to compare with the current <see cref="T:SlimDHT.KeyDistance"/>.</param>
		/// <returns><c>true</c> if the specified <see cref="object"/> is equal to the current <see cref="T:SlimDHT.KeyDistance"/>;
        /// otherwise, <c>false</c>.</returns>
        public override bool Equals(object obj)
        {
			return Equals(obj as KeyDistance);
        }

        /// <summary>
		/// Serves as a hash function for a <see cref="T:SlimDHT.KeyDistance"/> object.
        /// </summary>
        /// <returns>A hash code for this instance that is suitable for use in hashing algorithms and data structures such as a
        /// hash table.</returns>
        public override int GetHashCode()
        {
			return m_distance.GetHashCode();
        }

        /// <summary>
        /// Determines whether the specified <see cref="SlimDHT.KeyDistance"/> is equal to the current <see cref="T:SlimDHT.KeyDistance"/>.
        /// </summary>
        /// <param name="other">The <see cref="SlimDHT.KeyDistance"/> to compare with the current <see cref="T:SlimDHT.KeyDistance"/>.</param>
        /// <returns><c>true</c> if the specified <see cref="SlimDHT.KeyDistance"/> is equal to the current
        /// <see cref="T:SlimDHT.KeyDistance"/>; otherwise, <c>false</c>.</returns>
		public bool Equals(KeyDistance other)
		{
			return this.m_distance.Equals(other.m_distance);
		}
	}
}
