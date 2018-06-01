using System;
using System.Collections.Generic;
using System.Linq;
using SlimDHT;
using Xunit;

namespace Unittest
{
    public class RoutingTests
    {
		private const int KEY_COUNT = 1000;
		private const int REDUNDANCY = 4;
		private const int SAMPLE = 10;

		[Fact]
		public void TestTable()
		{
			var nodekey = Key.ComputeKey("node0");
			var table = new RoutingTable(nodekey, REDUNDANCY);
			var keys = Enumerable.Range(0, KEY_COUNT).Select(x => Key.ComputeKey($"item{x}")).ToList();				
		    var orderedKeys = keys.OrderBy(x => new KeyDistance(nodekey, x)).ToList();
			var tmp = new List<Key>();
			foreach (var k in keys)
				if (table.Add(new PeerInfo(k, new System.Net.IPEndPoint(1, 1))))
					tmp.Add(k);

			var expected = tmp.OrderBy(x => new KeyDistance(nodekey, x)).Take(SAMPLE).ToList();
			var found = table.Nearest(nodekey, SAMPLE);
			Assert.Equal(SAMPLE, found.Count);

			var sort = found.Select(x => x.Key).OrderBy(x => new KeyDistance(nodekey, x)).ToList();
			for (var i = 0; i < SAMPLE; i++)
				if (expected[i] != sort[i])
					throw new Exception($"Wrong key at position {i}");
		}
    }
}
