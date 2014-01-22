﻿using Batbeetle;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitTest
{
    [TestClass]
    public class ConnectionTests : TestBase
    {
        [TestMethod]
        public void Echo_ValidMsg_ShouldEchoBackWhateverMessageSent()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.Echo("Hello World".ToByte());
                Assert.AreEqual("Hello World", result.BytesToString());
            }
        }

        [TestMethod]
        public void Quit_CloseConnection_ShouldAlwaysReturnOK()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.Quit();
                Assert.AreEqual("OK", result);
            }
        }

        [TestMethod]
        public void Select_ValidIndex_SelectTheCorrectDb()
        {
            using (var client = new RedisClient(this.Host))
            {
                var selectResult = client.Select("1".ToByte());
                Assert.IsNotNull(selectResult);
                Assert.AreEqual("OK", selectResult);
                client.Set("mykey", "value");
                selectResult = client.Select("0".ToByte());
                Assert.IsNotNull(selectResult);
                Assert.AreEqual("OK", selectResult);
                var result = client.Get("mykey");
                Assert.IsNull(result); //mykey should be in db 1
                selectResult = client.Select("1".ToByte());
                Assert.IsNotNull(selectResult);
                Assert.AreEqual("OK", selectResult);
                result = client.Get("mykey");
                Assert.IsNotNull(result);
                Assert.AreEqual("value", result);
            }
        }
    }
}