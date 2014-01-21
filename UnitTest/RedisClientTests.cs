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
        public void BitCount_IfString_ReturnNumOfBitsSetTo1()
        {
            using (var client = new RedisClient(Host))
            {
                client.Set("mykey", "foobar");
                var result = client.BitCount("mykey".ToByte(), null, null);
                Assert.AreEqual(26, result);
                client.Set("mykey", "1"); //-> note, 1 is treated as a char value of 49.
                result = client.BitCount("mykey".ToByte(), null, null);
                Assert.AreEqual(3, result);
            }
        }

        [TestMethod]
        public void BitCount_StartEnd_ReturnNumOfBitsSetTo1()
        {
            using (var client = new RedisClient(Host))
            {
                client.Set("mykey", "foobar");
                var result = client.BitCount("mykey".ToByte(), "0".ToByte(), "0".ToByte());
                Assert.AreEqual(4, result);
                result = client.BitCount("mykey".ToByte(), "1".ToByte(), "1".ToByte());
                Assert.AreEqual(6, result);
            }
        }

        [TestMethod]
        public void BitCount_NonExistingKey_ReturnZero()
        {
            using (var client = new RedisClient(Host))
            {
                var result = client.BitCount("nonExistingKey".ToByte(), null, null);
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void Decr_ExistingKey_DecremenNumbertBy1()
        {
            using (var client = new RedisClient(Host))
            {
                client.Set("mykey", "10");
                var result = client.Decr("mykey".ToByte());
                Assert.AreEqual(9, result);
            }
        }

        [TestMethod]
        public void Decr_NonExistingKey_ReturnMinusOne()
        {
            using (var client = new RedisClient(Host))
            {
                //set nonExistingKey to 0, then perform the operation
                var result = client.Decr("nonExistingKey".ToByte());
                Assert.AreEqual(-1, result);
            }
        }

        [TestMethod]
        public void Decr_NonIntParsableKey_ReturnError()
        {
            using (var client = new RedisClient(Host))
            {
                //limits to 64bit signed int.
                client.Set("mykey", "234293482390480948029348230948");
                var result = client.Decr("mykey".ToByte());
                Assert.IsNull(result);
            }
        }
    }
}
