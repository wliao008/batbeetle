using System;
using Batbeetle;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitTest
{
    [TestClass]
    public class ClusterTests
    {
        [TestMethod]
        public void GetCrc16_ShouldReturnUshort()
        {
            var result = Crc16.GetCrc16("foo1".ToByte());
            Assert.AreEqual(62583, result);
        }

        [TestMethod]
        public void KeySlot_GivenAKey_ReturnReturnSlot()
        {
            var cluster = new Cluster();
            var result = cluster.KeySlot("foo".ToByte());
            Assert.AreEqual(12182, result);
            var result2 = cluster.KeySlot("hello".ToByte());
            Assert.AreEqual(866, result2);
        }

        [TestMethod]
        public void Dummy()
        {
            using (var client = new RedisClient("192.168.1.43", 7000))
            {
                var result = client.Strings.Set("name", "Wei Liao");
                Assert.IsTrue(result);
            }
        }

        [TestMethod]
        public void ClusterNodes()
        {
            var cluster = new Cluster();
            var result = cluster.RefreshNodes();
            Assert.IsNotNull(result);
        }
    }
}
