﻿using System.Collections;
using Batbeetle;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitTest
{
    [TestClass]
    public class StringTests : TestBase
    {
        [TestMethod]
        public void Append_IfKeyExistAndIsString_AppendValueAtEnd()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("existingKey", "Hello");
                client.Append("existingKey".ToByte(), " World".ToByte());
                var result = client.Get("existingKey");
                Assert.AreEqual("Hello World", result);
            }
        }

        [TestMethod]
        public void Append_IfKeyNotExist_CreateWithValue()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Append("nonExistingKey".ToByte(), "Hello".ToByte());
                client.Append("nonExistingKey".ToByte(), " World".ToByte());
                var result = client.Get("nonExistingKey");
                Assert.AreEqual("Hello World", result);
            }
        }

        [TestMethod]
        public void BitCount_IfString_ReturnNumOfBitsSetTo1()
        {
            using (var client = new RedisClient(this.Host))
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
            using (var client = new RedisClient(this.Host))
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
            using (var client = new RedisClient(this.Host))
            {
                var result = client.BitCount("nonExistingKey".ToByte(), null, null);
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void Decr_ExistingKey_DecremenNumbertBy1()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "10");
                var result = client.Decr("mykey".ToByte());
                Assert.AreEqual(9, result);
            }
        }

        [TestMethod]
        public void Decr_NonExistingKey_ReturnMinusOne()
        {
            using (var client = new RedisClient(this.Host))
            {
                //set nonExistingKey to 0, then perform the operation
                var result = client.Decr("nonExistingKey".ToByte());
                Assert.AreEqual(-1, result);
            }
        }

        [TestMethod]
        public void Decr_Non64bitIntParsableKey_ReturnError()
        {
            using (var client = new RedisClient(this.Host))
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
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "10");
                var result = client.DecrBy("mykey".ToByte(), "5".ToByte());
                Assert.AreEqual(5, result);
            }
        }

        [TestMethod]
        public void DecrBy_NonExistingKey_ReturnMinusSpecified()
        {
            using (var client = new RedisClient(this.Host))
            {
                //set nonExistingKey to 0, then perform the operation
                var result = client.DecrBy("nonExistingKey".ToByte(), "5".ToByte());
                Assert.AreEqual(-5, result);
            }
        }

        [TestMethod]
        public void DecrBy_Non64bitIntParsableKey_ReturnError()
        {
            using (var client = new RedisClient(this.Host))
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
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "value");
                var result = client.Get("mykey");
                Assert.AreEqual("value", result);
            }
        }

        [TestMethod]
        public void Get_NonExistingKey_ReturnNull()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.Get("nonExistingKey");
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void Get_NonStringKey_ReturnNull()
        {
            using (var client = new RedisClient(this.Host))
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
            using (var client = new RedisClient(this.Host))
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
            using (var client = new RedisClient(this.Host))
            {
                client.SetBit("mykey".ToByte(), "7".ToByte(), "1".ToByte());
                var result = client.GetBit("mykey".ToByte(), "100".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void GetBit_NonExistingStringKey_ReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.GetBit("nonExistingKey".ToByte(), "1".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void GetRange_ValidStringKey_ReturnSubstring()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "This is a string");
                var result = client.GetRange("mykey".ToByte(), "0".ToByte(), "3".ToByte());
                Assert.AreEqual("This", result.BytesToString());
                result = client.GetRange("mykey".ToByte(), "-3".ToByte(), "-1".ToByte());
                Assert.AreEqual("ing", result.BytesToString());
                result = client.GetRange("mykey".ToByte(), "0".ToByte(), "-1".ToByte());
                Assert.AreEqual("This is a string", result.BytesToString());
                result = client.GetRange("mykey".ToByte(), "10".ToByte(), "100".ToByte());
                Assert.AreEqual("string", result.BytesToString());
            }
        }

        [TestMethod]
        public void GetRange_NonExistingStringKey_ReturnEmptyString()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.GetRange("nonExistingKey".ToByte(), "0".ToByte(), "-1".ToByte());
                Assert.AreEqual("", result.BytesToString());
            }
        }

        [TestMethod]
        public void GetRange_OnWrongData_ReturnError()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 1;
                client.HMSet("mykey", tbl);
                var result = client.GetRange("mykey".ToByte(), "0".ToByte(), "-1".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void GetSet_ValidParams_SetThenGetTheValueStoredAtKey()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Incr("mycounter".ToByte()); //1
                var result = client.GetSet("mycounter".ToByte(), "0".ToByte());
                var result2 = client.Get("mycounter");
                Assert.AreEqual("1", result.BytesToString());
                Assert.AreEqual("0", result2);
            }
        }

        [TestMethod]
        public void GetSet_InValidParams_ReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.GetSet("nonExistingKey".ToByte(), "0".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void GetSet_NonStringDataType_ReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 1;
                client.HMSet("mykey", tbl);
                var result = client.GetSet("mykey".ToByte(), "0".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void Incr_ExistingKey_IncremenNumbertBy1()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "10");
                var result = client.Incr("mykey".ToByte());
                Assert.AreEqual(11, result);
            }
        }

        [TestMethod]
        public void Incr_NonExistingKey_ReturnOne()
        {
            using (var client = new RedisClient(this.Host))
            {
                //set nonExistingKey to 0, then perform the operation
                var result = client.Incr("nonExistingKey".ToByte());
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void Incr_Non64bitIntParsableKey_ReturnError()
        {
            using (var client = new RedisClient(this.Host))
            {
                //limits to 64bit signed int.
                client.Set("mykey", "234293482390480948029348230948");
                var result = client.Incr("mykey".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void IncrBy_ExistingKey_IncremenNumbertBySpecified()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "10");
                var result = client.IncrBy("mykey".ToByte(), "5".ToByte());
                Assert.AreEqual(15, result);
            }
        }

        [TestMethod]
        public void IncrBy_NonExistingKey_ReturnSpecified()
        {
            using (var client = new RedisClient(this.Host))
            {
                //set nonExistingKey to 0, then perform the operation
                var result = client.IncrBy("nonExistingKey".ToByte(), "5".ToByte());
                Assert.AreEqual(5, result);
            }
        }

        [TestMethod]
        public void IncrBy_Non64bitIntParsableKey_ReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                //limits to 64bit signed int.
                client.Set("mykey", "234293482390480948029348230948");
                var result = client.IncrBy("mykey".ToByte(), "5".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void IncrByFloat_ExistingKey_IncremenNumbertBySpecified()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "10.50");
                var result = client.IncrByFloat("mykey".ToByte(), "0.1".ToByte());
                Assert.AreEqual("10.6", result.BytesToString());
            }
        }

        [TestMethod]
        public void IncrByFloat_ExistingKey_IncremenNumbertBySpecified2()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "5.0e3");
                var result = client.IncrByFloat("mykey".ToByte(), "2.0e2".ToByte());
                Assert.AreEqual("5200", result.BytesToString());
            }
        }

        [TestMethod]
        public void IncrByFloat_NonExistingKey_ReturnSpecified()
        {
            using (var client = new RedisClient(this.Host))
            {
                //set nonExistingKey to 0, then perform the operation
                var result = client.IncrByFloat("nonExistingKey".ToByte(), "0.25".ToByte());
                Assert.AreEqual("0.25", result.BytesToString());
            }
        }

        [TestMethod]
        public void IncrByFloat_WrongDataType_ReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 1;
                client.HMSet("mykey", tbl);
                var result = client.IncrByFloat("mykey".ToByte(), "0.1".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void MGet_ValidParams_ReturnValues()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("key1", "value1");
                client.Set("key2", "value2");
                client.Set("key3", "value3");
                var result = client.MGet("key1".ToByte(), "key2".ToByte(), "key3".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("value1\nvalue2\nvalue3\n", result.BytesToString());
            }
        }

        [TestMethod]
        public void MGet_NonExistingKey_ReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("key1", "value1");
                client.Set("key2", "value2");
                client.Set("key3", "value3");
                var result = client.MGet("key1".ToByte(), "key2".ToByte(), "key333333".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("value1\nvalue2\n", result.BytesToString());
            }
        }

        [TestMethod]
        public void MGet_WrongDataType_ReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 1;
                client.HMSet("mykey", tbl);
                var result = client.MGet("mykey".ToByte()); //this op should never fail
                Assert.IsNotNull(result);
                Assert.AreEqual("", result.BytesToString());
            }
        }

        [TestMethod]
        public void MSet_ValidParama_SetKeysValues()
        {
            var keys = new byte[2][];
            var vals = new byte[2][];
            keys[0] = "key1".ToByte(); vals[0] = "val1".ToByte();
            keys[1] = "key2".ToByte(); vals[1] = "val2".ToByte();

            using (var client = new RedisClient(this.Host))
            {
                var result = client.MSet(keys, vals);
                Assert.AreEqual("OK", result);
            }
        }

        [TestMethod]
        public void MSet_WrongArgs_ReturnNil()
        {
            var keys = new byte[2][];
            var vals = new byte[1][];
            keys[0] = "key1".ToByte(); vals[0] = "val1".ToByte();
            keys[1] = "key2".ToByte(); //missing val for key2

            using (var client = new RedisClient(this.Host))
            {
                var result = client.MSet(keys, vals);
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void MSetNx_ValidParams_ReturnOne()
        {
            using (var client = new RedisClient(this.Host))
            {
                var keys = new byte[2][];
                var vals = new byte[2][];
                keys[0] = "key1".ToByte(); vals[0] = "val1".ToByte();
                keys[1] = "key2".ToByte(); vals[1] = "val2".ToByte();

                var result = client.MSetNx(keys, vals);
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void MSetNx_AnyKeyExist_ReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("key1", "val");
                var keys = new byte[2][];
                var vals = new byte[2][];
                keys[0] = "key1".ToByte(); vals[0] = "val1".ToByte(); //key1 already exist
                keys[1] = "key2".ToByte(); vals[1] = "val2".ToByte();

                var result = client.MSetNx(keys, vals);
                Assert.AreEqual(0, result);
                //Operation is atomic, so key2 should not be set if key1 is not
                var result2 = client.Get("key2");
                Assert.IsNull(result2);
            }
        }

        [TestMethod]
        public void Set_ValidKeyValue_ShouldSucceed()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "myvalue");
                var result = client.Get("mykey");
                Assert.IsNotNull(result);
                Assert.AreEqual("myvalue", result);
            }
        }

        [TestMethod]
        public void Set_WithEx_ShouldExpireKeyInSpecifiedSeconds()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "myvalue", 0, null, false, true); //expire immediately
                var result = client.Get("mykey");
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void Set_WithPx_ShouldExpireKeyInSpecifiedMilliSeconds()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "myvalue", null, 0, false, true); //expire immediately
                var result = client.Get("mykey");
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void Set_WithNx_SetIfKeyDoesNotExist()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "myvalue");
                client.Set("mykey", "modified value", null, null, true, false);
                var result = client.Get("mykey");
                Assert.IsNotNull(result);
                Assert.AreEqual("myvalue", result);
            }
        }

        [TestMethod]
        public void Set_WithXx_SetIfKeyAlreadyExist()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "myvalue");
                client.Set("mykey", "modified value", null, null, false, true);
                var result = client.Get("mykey");
                Assert.IsNotNull(result);
                Assert.AreEqual("modified value", result);
                client.Set("nonExistingKey", "modified value", null, null, false, true);
                result = client.Get("nonExistingKey");
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void SetBit_ValidParams_SetTheBits()
        {
            using (var client = new RedisClient(this.Host))
            {
                //7th position from the right at mykey: 0000000 -> 1000000
                //SetBit returns the original value stored that that place
                var result = client.SetBit("mykey".ToByte(), "7".ToByte(), "1".ToByte());
                Assert.AreEqual(0, result);
                result = client.SetBit("mykey".ToByte(), "7".ToByte(), "0".ToByte());
                Assert.AreEqual(1, result);
                var result2 = client.Get("mykey");
                Assert.IsNotNull(result2);
                Assert.AreEqual("\0", result2);
            }
        }

        [TestMethod]
        public void SetRange_ValidParams_SetAndReturnLengthOfValue()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "Hello World");
                var result = client.SetRange("mykey".ToByte(), "6".ToByte(), "Redis".ToByte());
                Assert.AreEqual(11, result);
                var result2 = client.Get("mykey");
                Assert.AreEqual("Hello Redis", result2);
            }
        }

        [TestMethod]
        public void StrLen_ValidKey_ReturnLength()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "Hello World");
                var result = client.StrLen("mykey".ToByte());
                Assert.AreEqual(11, result);
            }
        }

        [TestMethod]
        public void StrLen_NonExistingKey_ReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.StrLen("nonExistingKey".ToByte());
                Assert.AreEqual(0, result);
            }
        }
    }
}