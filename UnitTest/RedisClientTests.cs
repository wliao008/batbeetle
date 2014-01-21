using System;
using Batbeetle;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections;

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
        public void Decr_Non64bitIntParsableKey_ReturnError()
        {
            using (var client = new RedisClient(Host))
            {
                //limits to 64bit signed int.
                client.Set("mykey", "234293482390480948029348230948");
                var result = client.Decr("mykey".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void DecrBy_ExistingKey_DecremenNumbertBySpecified()
        {
            using (var client = new RedisClient(Host))
            {
                client.Set("mykey", "10");
                var result = client.DecrBy("mykey".ToByte(), "5".ToByte());
                Assert.AreEqual(5, result);
            }
        }

        [TestMethod]
        public void DecrBy_NonExistingKey_ReturnMinusSpecified()
        {
            using (var client = new RedisClient(Host))
            {
                //set nonExistingKey to 0, then perform the operation
                var result = client.DecrBy("nonExistingKey".ToByte(), "5".ToByte());
                Assert.AreEqual(-5, result);
            }
        }

        [TestMethod]
        public void DecrBy_Non64bitIntParsableKey_ReturnError()
        {
            using (var client = new RedisClient(Host))
            {
                //limits to 64bit signed int.
                client.Set("mykey", "234293482390480948029348230948");
                var result = client.DecrBy("mykey".ToByte(), "5".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void Get_ExistingStringKey_ReturnValue()
        {
            using (var client = new RedisClient(Host))
            {
                client.Set("mykey", "value");
                var result = client.Get("mykey");
                Assert.AreEqual("value\r\n", result);
            }
        }

        [TestMethod]
        public void Get_NonExistingKey_ReturnNull()
        {
            using (var client = new RedisClient(Host))
            {
                var result = client.Get("nonExistingKey");
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void Get_NonStringKey_ReturnNull()
        {
            using (var client = new RedisClient(Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 1;
                client.HMSet("mykey", tbl);
                var result = client.Get("mykey");
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void GetBit_ExistingStringKey_ReturnBitAtSpecifiedOffset()
        {
            using (var client = new RedisClient(Host))
            {
                client.SetBit("mykey".ToByte(), "7".ToByte(), "1".ToByte());
                var result = client.GetBit("mykey".ToByte(), "0".ToByte());
                Assert.AreEqual(0, result);
                result = client.GetBit("mykey".ToByte(), "7".ToByte());
                Assert.AreEqual(1, result);
                result = client.GetBit("mykey".ToByte(), "100".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void GetBit_ExistingStringKeyWrongOffset_ReturnZero()
        {
            using (var client = new RedisClient(Host))
            {
                client.SetBit("mykey".ToByte(), "7".ToByte(), "1".ToByte());
                var result = client.GetBit("mykey".ToByte(), "100".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void GetBit_NonExistingStringKey_ReturnZero()
        {
            using (var client = new RedisClient(Host))
            {
                var result = client.GetBit("nonExistingKey".ToByte(), "1".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void GetRange_ValidStringKey_ReturnSubstring()
        {
            using (var client = new RedisClient(Host))
            {
                client.Set("mykey", "This is a string");
                var result = client.GetRange("mykey".ToByte(), "0".ToByte(), "3".ToByte());
                Assert.AreEqual("This\r\n", result.BytesToString());
                result = client.GetRange("mykey".ToByte(), "-3".ToByte(), "-1".ToByte());
                Assert.AreEqual("ing\r\n", result.BytesToString());
                result = client.GetRange("mykey".ToByte(), "0".ToByte(), "-1".ToByte());
                Assert.AreEqual("This is a string\r\n", result.BytesToString());
                result = client.GetRange("mykey".ToByte(), "10".ToByte(), "100".ToByte());
                Assert.AreEqual("string\r\n", result.BytesToString());
            }
        }
    }
}
