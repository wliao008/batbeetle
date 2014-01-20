using System;
using Batbeetle;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitTest
{
    [TestClass]
    public class RedisClientTests
    {
        public readonly string Host = "127.0.0.1";

        [TestInitialize]
        public void Init()
        {
            using (var client = new RedisClient(Host))
            {
                client.FlushAll();
            }
        }

        [TestMethod]
        public void Append_IfKeyExistAndIsString_AppendValueAtEnd()
        {
            using (var client = new RedisClient(Host))
            {
                client.Set("existingKey", "Hello");
                client.Append("existingKey".ToByte(), " World".ToByte());
                var result = client.Get("existingKey");
                Assert.AreEqual("Hello World\r\n", result);
            }
        }

        [TestMethod]
        public void Append_IfKeyNotExist_CreateWithValue()
        {
            using (var client = new RedisClient(Host))
            {
                client.Append("nonExistingKey".ToByte(), "Hello".ToByte());
                client.Append("nonExistingKey".ToByte(), " World".ToByte());
                var result = client.Get("nonExistingKey");
                Assert.AreEqual("Hello World\r\n", result);
            }
        }

        [TestMethod]
        public void Bitcount_IfString_ReturnNumOfBitsSetTo1()
        {
            using (var client = new RedisClient(Host))
            {
                client.Set("mykey", "foobar");
                var result = client.Bitcount("mykey".ToByte(), null, null);
                Assert.AreEqual(26, result);
                client.Set("mykey", "1"); //-> note, 1 is treated as a char value of 49.
                result = client.Bitcount("mykey".ToByte(), null, null);
                Assert.AreEqual(3, result);
            }
        }
    }
}
