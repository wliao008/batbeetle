using System;
using Batbeetle;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections;

namespace UnitTest
{
    [TestClass]
    public class HashTests : TestBase
    {
        [TestMethod]
        public void HMSet_ValidParams_ShouldStoreHashtable()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.HMSet("mykey", tbl);

                var result = client.HMGetAll("mykey");
                Assert.IsNotNull(result);
                Assert.AreEqual("test", result["name"]);
                Assert.AreEqual("20", result["age"]);
                Assert.AreEqual("test@ttest.com", result["email"]);
            }
        }
    }
}
