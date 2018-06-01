using System;
using System.Collections.Generic;
using System.Linq;

namespace SlimDHT
{
	/// <summary>
    /// Implementation of a DHT key
    /// </summary>
	public class Key : IEquatable<Key>
    {
		/// <summary>
        /// The number of elements used to hold the key
        /// </summary>
		public const int KEYLEN = HASHSIZE / ELBITS;

        /// <summary>
        /// The number of bits in each element
        /// </summary>
		public const int ELBITS = 64;

        /// <summary>
        /// The hash size in bits
        /// </summary>
		public const int HASHSIZE = 256;

		/// <summary>
        /// The key data
        /// </summary>
		private readonly ulong[] m_key = new ulong[KEYLEN];

        /// <summary>
        /// The zero key
        /// </summary>
		public static readonly Key ZERO = new Key(new ulong[KEYLEN]);

		/// <summary>
        /// Constructs a Key from the string representation
        /// </summary>
        /// <param name="value">The string to parse.</param>
        public Key(string value)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));
			if (value.Length != HASHSIZE / 4)
				throw new ArgumentException($"The {nameof(Key)} needs a string of length {HASHSIZE / 4} characters", nameof(value));

			var zero = true;
			for (var i = 0; i < m_key.Length; i++)
			{
				m_key[i] = ulong.Parse(value.Substring(i * (ELBITS / 4), ELBITS / 4), System.Globalization.NumberStyles.HexNumber);
				zero &= m_key[i] == 0;
			}
			
			SignBit = ((m_key[0] >> (ELBITS - 1)) & 0x1) == 1;
			IsZero = zero;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:SlimDHT.Key"/> class.
        /// </summary>
        /// <param name="data">The data to initialize with.</param>
		public Key(ulong[] data)
		{
			if (data == null)
                throw new ArgumentNullException(nameof(data));
			if (data.Length != KEYLEN)
				throw new ArgumentOutOfRangeException($"The {nameof(data)} element must have exactly {KEYLEN} items");

			data.CopyTo(m_key, 0);

			SignBit = ((m_key[0] >> (ELBITS - 1)) & 0x1) == 1;
			IsZero = m_key.All(x => x == 0);
		}

		/// <summary>
        /// Initializes a new instance of the <see cref="T:SlimDHT.Key"/> class.
        /// </summary>
        /// <param name="data">The data to initialize with.</param>
		public Key(byte[] data)
		{
			if (data == null)
                throw new ArgumentNullException(nameof(data));
			if (data.Length != HASHSIZE / 8)
				throw new ArgumentOutOfRangeException($"The {nameof(data)} element must have exactly {HASHSIZE / 8} items");

			var zero = true;
			for (var i = 0; i < KEYLEN; i++)
			{
				m_key[i] =
					(ulong)data[(i * 8) + 0] << 0 |
					(ulong)data[(i * 8) + 1] << 8 |
					(ulong)data[(i * 8) + 2] << 16 |
					(ulong)data[(i * 8) + 3] << 24 |
					(ulong)data[(i * 8) + 4] << 32 |
					(ulong)data[(i * 8) + 5] << 40 |
					(ulong)data[(i * 8) + 6] << 48 |
					(ulong)data[(i * 8) + 7] << 56;

				zero &= m_key[i] == 0;
			}
			
			SignBit = ((m_key[0] >> (ELBITS - 1)) & 0x1) == 1;
            IsZero = zero;
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
        /// Gets the parts of this key
        /// </summary>
        /// <param name="index">The index of the element to get.</param>
		public ulong this[int index]
		{
			get
			{
				return m_key[index];
			}
		}

		/// <summary>
        /// Computes a key from the given input data
        /// </summary>
        /// <returns>The key for the data.</returns>
        /// <param name="data">The data to compute the key for.</param>
        public static Key ComputeKey(string data)
        {
            if (data == null)
                throw new ArgumentNullException(nameof(data));

			return ComputeKey(System.Text.Encoding.UTF8.GetBytes(data));
        }

        /// <summary>
        /// Computes a key from the given input data
        /// </summary>
        /// <returns>The key for the data.</returns>
		/// <param name="data">The data to compute the key for.</param>
		public static Key ComputeKey(byte[] data)
		{
			if (data == null)
				throw new ArgumentNullException(nameof(data));

			using (var h = System.Security.Cryptography.SHA256.Create())
				return new Key(h.ComputeHash(data));
		}

		/// <summary>
        /// Computes a key from the given input data
        /// </summary>
        /// <returns>The key for the data.</returns>
        /// <param name="stream">The data to compute the key for.</param>
		public static Key ComputeKey(System.IO.Stream stream)
        {
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));

            using (var h = System.Security.Cryptography.SHA256.Create())
                return new Key(h.ComputeHash(stream));
        }

        /// <summary>
        /// Creates a random key
        /// </summary>
        /// <returns>The random key.</returns>
		public static Key CreateRandomKey()
		{
			var buf = new byte[HASHSIZE / 8];
			new Random().NextBytes(buf);
			return new Key(buf);
		}

        /// <summary>
        /// Computes the distance between two keys
        /// </summary>
        /// <returns>The distance between the keys.</returns>
        /// <param name="a">One key.</param>
        /// <param name="b">Another key.</param>
		public static KeyDistance Distance(Key a, Key b)
		{
			return new KeyDistance(a, b);
		}

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="T:SlimDHT.Key"/>.
        /// </summary>
        /// <returns>A <see cref="T:System.String"/> that represents the current <see cref="T:SlimDHT.Key"/>.</returns>
		public override string ToString()
		{
			return string.Join("", m_key.Select(x => x.ToString("x16")));
		}

        /// <summary>
        /// Determines whether the specified <see cref="object"/> is equal to the current <see cref="T:SlimDHT.Key"/>.
        /// </summary>
        /// <param name="obj">The <see cref="object"/> to compare with the current <see cref="T:SlimDHT.Key"/>.</param>
        /// <returns><c>true</c> if the specified <see cref="object"/> is equal to the current <see cref="T:SlimDHT.Key"/>;
        /// otherwise, <c>false</c>.</returns>
		public override bool Equals(object obj)
		{
			return Equals(obj as Key);
		}

        /// <summary>
        /// Serves as a hash function for a <see cref="T:SlimDHT.Key"/> object.
        /// </summary>
        /// <returns>A hash code for this instance that is suitable for use in hashing algorithms and data structures such as a
        /// hash table.</returns>
		public override int GetHashCode()
		{
			var b = m_key[0].GetHashCode();
			for (var i = 1; i < KEYLEN; i++)
				b ^= m_key[i].GetHashCode();

			return b;
		}

        /// <summary>
        /// Determines whether the specified <see cref="SlimDHT.Key"/> is equal to the current <see cref="T:SlimDHT.Key"/>.
        /// </summary>
        /// <param name="other">The <see cref="SlimDHT.Key"/> to compare with the current <see cref="T:SlimDHT.Key"/>.</param>
        /// <returns><c>true</c> if the specified <see cref="SlimDHT.Key"/> is equal to the current <see cref="T:SlimDHT.Key"/>;
        /// otherwise, <c>false</c>.</returns>
		public bool Equals(Key other)
		{
			if (other == null)
				return false;
			
			for (var i = 0; i < KEYLEN; i++)
                if (other.m_key[i] != this.m_key[i])
                    return false;

            return true;
		}

        /// <summary>
        /// Returns a copy of the key data
        /// </summary>
        /// <value>The key data.</value>
        public ulong[] KeyData => new List<ulong>(m_key).ToArray();
	}
}
