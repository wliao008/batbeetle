using System.Collections;
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
                client.Strings.Set("existingKey", "Hello");
                client.Strings.Append("existingKey", " World");
                var result = client.Strings.Get("existingKey");
                Assert.AreEqual("Hello World", result);
            }
        }

        [TestMethod]
        public void Append_IfKeyNotExist_CreateWithValue()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Append("nonExistingKey", "Hello");
                client.Strings.Append("nonExistingKey", " World");
                var result = client.Strings.Get("nonExistingKey");
                Assert.AreEqual("Hello World", result);
            }
        }

        [TestMethod]
        public void BitCount_IfString_ReturnNumOfBitsSetTo1()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "foobar");
                var result = client.Strings.BitCount("mykey", null, null);
                Assert.AreEqual(26, result);
                client.Strings.Set("mykey", "1"); //-> note, 1 is treated as a char value of 49.
                result = client.Strings.BitCount("mykey", null, null);
                Assert.AreEqual(3, result);
            }
        }

        [TestMethod]
        public void BitCount_StartEnd_ReturnNumOfBitsSetTo1()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "foobar");
                var result = client.Strings.BitCount("mykey", 0, 0);
                Assert.AreEqual(4, result);
                result = client.Strings.BitCount("mykey", 1, 1);
                Assert.AreEqual(6, result);
            }
        }

        [TestMethod]
        public void BitCount_NonExistingKey_ReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.Strings.BitCount("nonExistingKey", null, null);
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void Decr_ExistingKey_DecremenNumbertBy1()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "10");
                var result = client.Strings.Decr("mykey");
                Assert.AreEqual(9, result);
            }
        }

        [TestMethod]
        public void Decr_NonExistingKey_ReturnMinusOne()
        {
            using (var client = new RedisClient(this.Host))
            {
                //set nonExistingKey to 0, then perform the operation
                var result = client.Strings.Decr("nonExistingKey");
                Assert.AreEqual(-1, result);
            }
        }

        [TestMethod]
        public void Decr_Non64bitIntParsableKey_ReturnError()
        {
            using (var client = new RedisClient(this.Host))
            {
                //limits to 64bit signed int.
                client.Strings.Set("mykey", "234293482390480948029348230948");
                var result = client.Strings.Decr("mykey");
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void DecrBy_ExistingKey_DecremenNumbertBySpecified()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "10");
                var result = client.Strings.DecrBy("mykey", 5);
                Assert.AreEqual(5, result);
            }
        }

        [TestMethod]
        public void DecrBy_NonExistingKey_ReturnMinusSpecified()
        {
            using (var client = new RedisClient(this.Host))
            {
                //set nonExistingKey to 0, then perform the operation
                var result = client.Strings.DecrBy("nonExistingKey", 5);
                Assert.AreEqual(-5, result);
            }
        }

        [TestMethod]
        public void DecrBy_Non64bitIntParsableKey_ReturnError()
        {
            using (var client = new RedisClient(this.Host))
            {
                //limits to 64bit signed int.
                client.Strings.Set("mykey", "234293482390480948029348230948");
                var result = client.Strings.DecrBy("mykey", 5);
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void Get_ExistingStringKey_ReturnValue()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "value");
                var result = client.Strings.Get("mykey");
                Assert.AreEqual("value", result);
            }
        }

        [TestMethod]
        public void Get_NonExistingKey_ReturnNull()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.Strings.Get("nonExistingKey");
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
                client.Hashes.HMSet("mykey", tbl);
                var result = client.Strings.Get("mykey");
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void GetBit_ExistingStringKey_ReturnBitAtSpecifiedOffset()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.SetBit("mykey".ToByte(), "7".ToByte(), "1".ToByte());
                var result = client.Strings.GetBit("mykey", 0);
                Assert.AreEqual(0, result);
                result = client.Strings.GetBit("mykey", 7);
                Assert.AreEqual(1, result);
                result = client.Strings.GetBit("mykey", 100);
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void GetBit_ExistingStringKeyWrongOffset_ReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.SetBit("mykey".ToByte(), "7".ToByte(), "1".ToByte());
                var result = client.Strings.GetBit("mykey", 100);
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void GetBit_NonExistingStringKey_ReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.Strings.GetBit("nonExistingKey", 1);
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void GetRange_ValidStringKey_ReturnSubstring()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "This is a string");
                var result = client.Strings.GetRange("mykey", 0, 3);
                Assert.AreEqual("This", result);
                result = client.Strings.GetRange("mykey", -3, -1);
                Assert.AreEqual("ing", result);
                result = client.Strings.GetRange("mykey", 0, -1);
                Assert.AreEqual("This is a string", result);
                result = client.Strings.GetRange("mykey", 10, 100);
                Assert.AreEqual("string", result);
            }
        }

        [TestMethod]
        public void GetRange_NonExistingStringKey_ReturnEmptyString()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.Strings.GetRange("nonExistingKey", 0, -1);
                Assert.AreEqual("", result);
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
                client.Hashes.HMSet("mykey", tbl);
                var result = client.Strings.GetRange("mykey", 0, -1);
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void GetSet_ValidParams_SetThenGetTheValueStoredAtKey()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Incr("mycounter"); //1
                var result = client.Strings.GetSet("mycounter", "0");
                var result2 = client.Strings.Get("mycounter");
                Assert.AreEqual("1", result);
                Assert.AreEqual("0", result2);
            }
        }

        [TestMethod]
        public void GetSet_InValidParams_ReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.Strings.GetSet("nonExistingKey", "0");
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
                client.Hashes.HMSet("mykey", tbl);
                var result = client.Strings.GetSet("mykey", "0");
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void Incr_ExistingKey_IncremenNumbertBy1()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "10");
                var result = client.Strings.Incr("mykey");
                Assert.AreEqual(11, result);
            }
        }

        [TestMethod]
        public void Incr_NonExistingKey_ReturnOne()
        {
            using (var client = new RedisClient(this.Host))
            {
                //set nonExistingKey to 0, then perform the operation
                var result = client.Strings.Incr("nonExistingKey");
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void Incr_Non64bitIntParsableKey_ReturnError()
        {
            using (var client = new RedisClient(this.Host))
            {
                //limits to 64bit signed int.
                client.Strings.Set("mykey", "234293482390480948029348230948");
                var result = client.Strings.Incr("mykey");
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void IncrBy_ExistingKey_IncremenNumbertBySpecified()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "10");
                var result = client.Strings.IncrBy("mykey", 5);
                Assert.AreEqual(15, result);
            }
        }

        [TestMethod]
        public void IncrBy_NonExistingKey_ReturnSpecified()
        {
            using (var client = new RedisClient(this.Host))
            {
                //set nonExistingKey to 0, then perform the operation
                var result = client.Strings.IncrBy("nonExistingKey", 5);
                Assert.AreEqual(5, result);
            }
        }

        [TestMethod]
        public void IncrBy_Non64bitIntParsableKey_ReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                //limits to 64bit signed int.
                client.Strings.Set("mykey", "234293482390480948029348230948");
                var result = client.Strings.IncrBy("mykey", 5);
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void IncrByFloat_ExistingKey_IncremenNumbertBySpecified()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "10.50");
                var result = client.Strings.IncrByFloat("mykey", 0.1);
                Assert.AreEqual(10.6, result);
            }
        }

        [TestMethod]
        public void IncrByFloat_ExistingKey_IncremenNumbertBySpecified2()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "5.0e3");
                var result = client.Strings.IncrByFloat("mykey", 2.0e2);
                Assert.AreEqual(5200, result);
            }
        }

        [TestMethod]
        public void IncrByFloat_NonExistingKey_ReturnSpecified()
        {
            using (var client = new RedisClient(this.Host))
            {
                //set nonExistingKey to 0, then perform the operation
                var result = client.Strings.IncrByFloat("nonExistingKey", 0.25);
                Assert.AreEqual(0.25, result);
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
                client.Hashes.HMSet("mykey", tbl);
                var result = client.Strings.IncrByFloat("mykey", 0.1);
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void MGet_ValidParams_ReturnValues()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("key1", "value1");
                client.Strings.Set("key2", "value2");
                client.Strings.Set("key3", "value3");
                var result = client.MGet("key1".ToByte(), "key2".ToByte(), "key3".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("value1\r\nvalue2\r\nvalue3\r\n", result.MultiBytesToString());
            }
        }

        [TestMethod]
        public void MGet_NonExistingKey_ReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("key1", "value1");
                client.Strings.Set("key2", "value2");
                client.Strings.Set("key3", "value3");
                var result = client.MGet("key1".ToByte(), "key2".ToByte(), "key333333".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("value1\r\nvalue2\r\n", result.MultiBytesToString());
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
                client.Hashes.HMSet("mykey", tbl);
                var result = client.MGet("mykey".ToByte()); //this op should never fail
                Assert.IsNotNull(result);
                Assert.AreEqual("", result.MultiBytesToString());
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
                client.Strings.Set("key1", "val");
                var keys = new byte[2][];
                var vals = new byte[2][];
                keys[0] = "key1".ToByte(); vals[0] = "val1".ToByte(); //key1 already exist
                keys[1] = "key2".ToByte(); vals[1] = "val2".ToByte();

                var result = client.MSetNx(keys, vals);
                Assert.AreEqual(0, result);
                //Operation is atomic, so key2 should not be set if key1 is not
                var result2 = client.Strings.Get("key2");
                Assert.IsNull(result2);
            }
        }

        [TestMethod]
        public void Set_ValidKeyValue_ShouldSucceed()
        {
            using (var client = new RedisClient(this.Host))
            {
                var setResult = client.Strings.Set("mykey", "myvalue");
                Assert.IsTrue(setResult);
                var result = client.Strings.Get("mykey");
                Assert.IsNotNull(result);
                Assert.AreEqual("myvalue", result);
            }
        }

        [TestMethod]
        public void Set_ValidKeyValueButNoConnection_ShouldFail()
        {
            var client = new RedisClient(this.Host);
            client.Quit();
            var setResult = client.Strings.Set("mykey", "myvalue");
            Assert.IsFalse(setResult);
        }

        [TestMethod]
        public void Set_WithEx_ShouldExpireKeyInSpecifiedSeconds()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "myvalue", 0, null, false, true); //expire immediately
                var result = client.Strings.Get("mykey");
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void Set_WithPx_ShouldExpireKeyInSpecifiedMilliSeconds()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "myvalue", null, 0, false, true); //expire immediately
                var result = client.Strings.Get("mykey");
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void Set_WithNx_SetIfKeyDoesNotExist()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "myvalue");
                client.Strings.Set("mykey", "modified value", null, null, true, false);
                var result = client.Strings.Get("mykey");
                Assert.IsNotNull(result);
                Assert.AreEqual("myvalue", result);
            }
        }

        [TestMethod]
        public void Set_WithXx_SetIfKeyAlreadyExist()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "myvalue");
                client.Strings.Set("mykey", "modified value", null, null, false, true);
                var result = client.Strings.Get("mykey");
                Assert.IsNotNull(result);
                Assert.AreEqual("modified value", result);
                client.Strings.Set("nonExistingKey", "modified value", null, null, false, true);
                result = client.Strings.Get("nonExistingKey");
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
                var result2 = client.Strings.Get("mykey");
                Assert.IsNotNull(result2);
                Assert.AreEqual("\0", result2);
            }
        }

        [TestMethod]
        public void SetRange_ValidParams_SetAndReturnLengthOfValue()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "Hello World");
                var result = client.SetRange("mykey".ToByte(), "6".ToByte(), "Redis".ToByte());
                Assert.AreEqual(11, result);
                var result2 = client.Strings.Get("mykey");
                Assert.AreEqual("Hello Redis", result2);
            }
        }

        [TestMethod]
        public void StrLen_ValidKey_ReturnLength()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "Hello World");
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
