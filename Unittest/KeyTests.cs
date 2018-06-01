using System;
using SlimDHT;
using Xunit;

namespace Unittest
{
    public class KeyTests
    {
        [Fact]
        public void CreateKey()
        {
			var key = SlimDHT.Key.CreateRandomKey();
        }

		[Fact]
        public void ComputeDistances()
        {
			var key1 = Key.ComputeKey("key1");
			var key2 = Key.ComputeKey("key2");
			var dist = new KeyDistance(key1, key2);
			Assert.Equal(dist, new KeyDistance("1e4529cbe05a76306e7402f8358f974740603a1740993e9ead8c3f56ad5c9fae"));
        }
    }
}
