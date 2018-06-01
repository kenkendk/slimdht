using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace SlimDHT
{
    /// <summary>
    /// Implements a simple cache list
    /// </summary>
    public class MRUCache<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
    {
        /// <summary>
        /// The list of data items in the cache
        /// </summary>
        private readonly List<KeyValuePair<TKey, long>> m_position = new List<KeyValuePair<TKey, long>>();

        /// <summary>
        /// A lookup table to keep track of what keys are present
        /// </summary>
        private readonly Dictionary<TKey, TValue> m_data = new Dictionary<TKey, TValue>();

        /// <summary>
        /// The maximum number of items to keep in the cache
        /// </summary>
        private readonly int m_max_size;

        /// <summary>
        /// The maximum age for items in the cache
        /// </summary>
        private readonly long m_max_age;

        /// <summary>
        /// Initializes a new instance of the <see cref="T:SlimDHT.MRUCache`1"/> class.
        /// </summary>
        /// <param name="size">The size of the MRU cache</param>
        /// <param name="age">The maximum age of items</param>
        public MRUCache(int size, TimeSpan age)
        {
            if (size <= 0)
                throw new ArgumentOutOfRangeException(nameof(size), size, "The size must be greater than zero");
            if (age.Ticks <= TimeSpan.TicksPerSecond)
                throw new ArgumentOutOfRangeException(nameof(size), size, "The age must be greater than a second");
            m_max_size = size;
            m_max_age = age.Ticks;
        }

        /// <summary>
        /// Add the specified key and data.
        /// </summary>
        /// <param name="key">The key of the data to add.</param>
        /// <param name="data">The data to add.</param>
        /// <returns>The key being removed, or null</returns>
        public TKey Add(TKey key, TValue data)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            
            // If we already know the key, pull it out of the list
            if (m_data.ContainsKey(key))
            {
                var ix = m_position.FindIndex(x => x.Key.Equals(key));
                m_position.RemoveAt(ix);
            }

            // Prepare the return value
            TKey result = default(TKey);

            // If we are filled, remove the oldest item
            if (m_position.Count >= m_max_size)
            {
                result = m_position[0].Key;
                m_position.RemoveAt(0);
                m_data.Remove(result);
            }

            // Add the new item at the end
            m_data[key] = data;
            m_position.Add(new KeyValuePair<TKey, long>(key, DateTime.Now.Ticks));

            return result;
        }

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        /// <returns>The enumerator.</returns>
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return m_data.GetEnumerator();
        }

        /// <summary>
        /// Removes the data at the given key
        /// </summary>
        /// <param name="key">The key to remove the data for.</param>
        /// <returns><c>true</c> if the data was found and removed; false otherwise</returns>
        public bool Remove(TKey key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            
            if (!m_data.ContainsKey(key))
                return false;

            m_data.Remove(key);
            for (var i = 0; i < m_position.Count; i++)
                if (m_position[i].Key.Equals(key))
                {
                    m_position.RemoveAt(i);
                    return true;
                }

            throw new Exception($"Unexpected lookup failure");
        }

        /// <summary>
        /// Attempts to read the value for the given key
        /// </summary>
        /// <returns><c>true</c>, if the value was read, <c>false</c> otherwise.</returns>
        /// <param name="key">The key to look up.</param>
        /// <param name="data">The resulting data.</param>
        public bool TryGetValue(TKey key, out TValue data)
        {
            return m_data.TryGetValue(key, out data);
        }

        /// <summary>
        /// Returns an enumerator for the contents
        /// </summary>
        /// <returns>The enumerator for the contents.</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        /// <summary>
        /// Removes all items older than the age limit
        /// </summary>
        public void ExpireOldItems()
        {
            var maxage = DateTime.Now.Ticks - m_max_age;
            int i;
            for (i = 0; i < m_position.Count; i++)
            {
                if (m_position[i].Value < maxage)
                    m_data.Remove(m_position[0].Key);
                else
                    break;
            }

            m_position.RemoveRange(0, i - 1);
        }

        /// <summary>
        /// The number of items in the cache
        /// </summary>
        /// <value>The count.</value>
        public int Count => m_position.Count;

        /// <summary>
        /// Gets the timestamp on the oldest item.
        /// </summary>
        /// <value>The oldest item.</value>
        public DateTime OldestItem => new DateTime(m_position.FirstOrDefault().Value);
    }
}
