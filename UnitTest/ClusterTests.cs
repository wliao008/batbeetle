using System;
using Batbeetle;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitTest
{
    [TestClass]
    public class ClusterTests
    {
        [TestMethod]
        [TestCategory("Cluster")]
        public void GetCrc16_ShouldReturnUshort()
        {
            var result = Crc16.GetCrc16("foo1".ToByte());
            Assert.AreEqual(62583, result);
        }

        [TestMethod]
        [TestCategory("Cluster")]
        public void KeySlot_GivenAKey_ReturnReturnSlot()
        {
            var cluster = new Cluster();
            var result = cluster.KeySlot("foo".ToByte());
            Assert.AreEqual(12182, result);
            var result2 = cluster.KeySlot("hello".ToByte());
            Assert.AreEqual(866, result2);
        }

        [TestMethod]
        [TestCategory("Cluster")]
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
            Cluster.NodeChanged += (s, e) =>
            {
                System.Diagnostics.Debug.WriteLine("FAILED NODE");
                System.Diagnostics.Debug.WriteLine(e.Node.ToString());
            };
            var client = cluster.GetClientFromKey("myname".ToByte());
            client.Strings.Set("myname", "test");
            var result = client.Strings.Get("myname");
            //Assert.IsTrue(result);
            Assert.IsNotNull(result);
            Assert.AreEqual("test", result);
        }
    }
}
