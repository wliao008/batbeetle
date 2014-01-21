using Batbeetle;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitTest
{
    [TestClass]
    public class KeyTests : TestBase
    {
        [TestMethod]
        public void Del_ValidKeys_ReturnNumOfKeysDeleted()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("key1", "val1");
                client.Set("key2", "val2");
                client.Set("key3", "val3");
                var result = client.Del("key1".ToByte(), "key2".ToByte(), "key3".ToByte());
                Assert.AreEqual(3, result);
            }
        }

        [TestMethod]
        public void Del_InValidKeys_IgnoredIfDoesntExist()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.Del("key1".ToByte(), "key2".ToByte(), "key3".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void Dump_ValidKey_ReturnSerializedValue()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "10");
                var result = client.Dump("mykey".ToByte());
                Assert.IsNotNull(result.BytesToString());
            }
        }

        [TestMethod]
        public void Dump_InValidKey_ReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.Dump("nonExistingKey".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void Exists_ValidKey_ReturnOne()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "10");
                var result = client.Exists("mykey".ToByte());
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void Exists_InValidKey_ReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.Exists("nonExistingKey".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void Expire_ValidKey_ReturnOne()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "10");
                var result = client.Ttl("mykey".ToByte());
                Assert.AreEqual(-1, result);
                result = client.Expire("mykey".ToByte(), "10".ToByte());
                Assert.AreEqual(1, result);
                result = client.Ttl("mykey".ToByte());
                Assert.AreNotEqual(-1, result);
            }
        }

        [TestMethod]
        public void Expire_InValidKey_ReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.Expire("nonExistingKey".ToByte(), "10".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void ExpireAt_ValidKey_ReturnOne()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "10");
                var result = client.Exists("mykey".ToByte());
                Assert.AreEqual(1, result);
                client.ExpireAt("mykey".ToByte(), "1293840000".ToByte());
                result = client.Exists("mykey".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void ExpireAt_InValidKey_ReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.ExpireAt("nonExistingKey".ToByte(), "1293840000".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void Keys_ValidPattern_ReturnValuesOfMatched()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("one", "1");
                client.Set("two", "2");
                client.Set("three", "3");
                client.Set("four", "4");
                var result = client.Keys("*o*".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("four\r\ntwo\r\none\r\n", result.BytesToString());
            }
        }

        [TestMethod]
        public void Keys_ValidPattern_ReturnValuesOfMatched2()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("one", "1");
                client.Set("two", "2");
                client.Set("three", "3");
                client.Set("four", "4");
                var result = client.Keys("t??".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("two\r\n", result.BytesToString());
            }
        }

        [TestMethod]
        public void Keys_ValidPattern_ReturnValuesOfMatched3()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("one", "1");
                client.Set("two", "2");
                client.Set("three", "3");
                client.Set("four", "4");
                var result = client.Keys("*".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("four\r\nthree\r\ntwo\r\none\r\n", result.BytesToString());
            }
        }

        [TestMethod]
        public void Keys_InValidPattern_ReturnEmpty()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("one", "1");
                client.Set("two", "2");
                client.Set("three", "3");
                client.Set("four", "4");
                var result = client.Keys("nonmatching".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("", result.BytesToString());
            }
        }

        [TestMethod]
        public void Move_ValidKey_MoveKeyToSpecifiedDb()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "val");
                var result = client.Move("mykey".ToByte(), "1".ToByte());
                Assert.AreEqual(1, result);
                client.Select("1".ToByte());
                var result2 = client.Get("mykey");
                Assert.AreEqual("val\r\n", result2);
            }
        }

        [TestMethod]
        public void Move_InValidKey_ReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.Move("nonExistingKey".ToByte(), "1".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void Object_RefCountValidKey_ReturnRefCount()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "val");
                var result = client.Object(Commands.RefCount, "mykey".ToByte());
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void Object_RefCountInValidKey_ReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.Object(Commands.RefCount, "nonExistingKey".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void Object_IdleTimeValidKey_ReturnTimeIdled()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "val");
                var result = client.Object(Commands.IdleTime, "mykey".ToByte());
                Assert.IsNotNull(result);
            }
        }

        [TestMethod]
        public void ObjectEncoding_ValidKey_ReturnEncoding()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "val");
                var result = client.ObjectEncoding("mykey".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("raw\r\n", result.BytesToString());
            }
        }

        [TestMethod]
        public void ObjectEncoding_ValidKey_ReturnEncoding2()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "10");
                var result = client.ObjectEncoding("mykey".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("int\r\n", result.BytesToString());
            }
        }

        [TestMethod]
        public void ObjectEncoding_ValidKeyChangedData_ReturnCorrectEncoding()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "1000");
                var result = client.ObjectEncoding("mykey".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("int\r\n", result.BytesToString());
                client.Append("mykey".ToByte(), "bar".ToByte());
                result = client.ObjectEncoding("mykey".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("raw\r\n", result.BytesToString());
            }
        }

        [TestMethod]
        public void Persist_ValidKey_RemoveTimeout()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "1000", 10);
                var result = client.Ttl("mykey".ToByte());
                Assert.AreNotEqual(-1, result);
                client.Persist("mykey".ToByte()); 
                result = client.Ttl("mykey".ToByte());
                Assert.AreEqual(-1, result);
            }
        }

        [TestMethod]
        public void Persist_InValidKey_ReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.Persist("nonExistingKey".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void PExpire_ValidKey_ReturnOne()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "10");
                var result = client.Ttl("mykey".ToByte());
                Assert.AreEqual(-1, result);
                result = client.PExpire("mykey".ToByte(), "10000".ToByte());
                Assert.AreEqual(1, result);
                result = client.Ttl("mykey".ToByte());
                Assert.AreNotEqual(-1, result);
            }
        }

        [TestMethod]
        public void PExpire_InValidKey_ReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.PExpire("nonExistingKey".ToByte(), "10000".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void PExpireAt_ValidKey_ReturnOne()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "10");
                var result = client.Exists("mykey".ToByte());
                Assert.AreEqual(1, result);
                client.PExpireAt("mykey".ToByte(), "1293840000".ToByte());
                result = client.Exists("mykey".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void PExpireAt_InValidKey_ReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.PExpireAt("nonExistingKey".ToByte(), "1293840000".ToByte());
                Assert.AreEqual(0, result);
            }
        }
    }
}
