using System;
using Batbeetle;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections;
using System.Collections.Generic;

namespace UnitTest
{
    [TestClass]
    public class HashTests : TestBase
    {
        [TestMethod]
        public void HDel_KeyExist_ShouldRemoveSpecifiedField()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);
                client.HDel("mykey".ToByte(), "age".ToByte());
                var result = client.Hashes.HMGetAll("mykey");
                Assert.IsNotNull(result);
                Assert.AreEqual("test", result["name"]);
                Assert.AreEqual("test@ttest.com", result["email"]);

                //age should not exist anymore
                var field = client.HExists("mykey".ToByte(), "age".ToByte());
                Assert.AreEqual(0, field);
            }
        }

        [TestMethod]
        public void HDel_KeyNotExist_IgnoreAndShouldReturn0()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);
                var delResult = client.HDel("mykey".ToByte(), "nonExistingField".ToByte());
                Assert.AreEqual(0, delResult);
                var result = client.Hashes.HMGetAll("mykey");
            }
        }

        [TestMethod]
        public void HExists_KeyExist_ShouldReturnOne()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);
                var result = client.HExists("mykey".ToByte(), "age".ToByte());
                Assert.AreEqual(1, result);
                result = client.HExists("mykey".ToByte(), "nonExistingField".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void HExists_KeyNotExist_ShouldReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.HExists("nonExistingKey".ToByte(), "age".ToByte());
                Assert.AreEqual(0, result);
                result = client.HExists("nonExistingField".ToByte(), "nonExistingField".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void HGet_KeyExist_ShouldValueOfSpecifiedField()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);

                var result = client.HGet("mykey".ToByte(), "age".ToByte());
                Assert.AreEqual("20", result.BytesToString());
            }
        }

        [TestMethod]
        public void HGet_KeyExistFieldNotExist_ShouldReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);

                var result = client.HGet("mykey".ToByte(), "nonExistingField".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void HGet_KeyNotExistFieldNotExist_ShouldReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.HGet("nonExistingKey".ToByte(), "nonExistingField".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void HGetAll_KeyExist_ShouldReturnAllValues()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);

                var result = client.Hashes.HMGetAll("mykey");
                Assert.AreEqual("test", result["name"]);
                Assert.AreEqual("20", result["age"]);
                Assert.AreEqual("test@ttest.com", result["email"]);
            }
        }

        [TestMethod]
        public void HGetAll_KeyExistWithEmptyField_ShouldReturnAllValues()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = " ";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);

                var result = client.Hashes.HMGetAll("mykey");
                Assert.AreEqual(" ", result["name"]);
                Assert.AreEqual("20", result["age"]);
                Assert.AreEqual("test@ttest.com", result["email"]);
            }
        }

        [TestMethod]
        public void HGetAll_KeyExistWithEmptyField_ShouldReturnEmptyValue()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);

                var result = client.Hashes.HMGetAll("mykey");
                Assert.AreEqual("", result["name"]);
                Assert.AreEqual("20", result["age"]);
                Assert.AreEqual("test@ttest.com", result["email"]);
            }
        }

        [TestMethod]
        public void HGetAll_KeyNotExist_ShouldReturnEmpty()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.Hashes.HMGetAll("nonExistingKey");
                Assert.IsNotNull(result);
                Assert.AreEqual(0, result.Count);
            }
        }

        [TestMethod]
        public void HIncrBy_KeyExist_ShouldIncrValueAtField()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);

                var result = client.HIncrBy("mykey".ToByte(), "age".ToByte(), "1".ToByte());
                Assert.AreEqual(21, result);
            }
        }

        [TestMethod]
        public void HIncrBy_KeyExistFieldNotExist_ShouldCreateFieldThenIncrValueAtField()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);

                var result = client.HIncrBy("mykey".ToByte(), "num".ToByte(), "10".ToByte());
                Assert.AreEqual(10, result);
                var result2 = client.HExists("mykey".ToByte(), "num".ToByte());
                Assert.AreEqual(1, result2);
            }
        }

        [TestMethod]
        public void HIncrBy_KeyNotExist_ShouldCreateKeyAndFieldThenIncrValueAtField()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.HIncrBy("mykey".ToByte(), "num".ToByte(), "10".ToByte());
                Assert.AreEqual(10, result);
                var result2 = client.HExists("mykey".ToByte(), "num".ToByte());
                Assert.AreEqual(1, result2);
            }
        }

        [TestMethod]
        public void HIncrBy_ShouldBeLimitedBy64bitSignedInt()
        {
            using (var client = new RedisClient(this.Host))
            {
                //limits to 64bit signed int.
                var result = client.HIncrBy("mykey".ToByte(), "age".ToByte(), "234293482390480948029348230948".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void HIncrByFloat_ExistingKey_IncremenNumbertBySpecified()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);

                var result = client.HIncrByFloat("mykey".ToByte(), "age".ToByte(), "10.50".ToByte());
                Assert.AreEqual("30.5", result.BytesToString());
            }
        }

        [TestMethod]
        public void HIncrByFloat_ExistingKey_IncremenNumbertBySpecified2()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = "5.0e3";
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);

                var result = client.HIncrByFloat("mykey".ToByte(), "age".ToByte(), "2.0e2".ToByte());
                Assert.AreEqual("5200", result.BytesToString());
            }
        }

        [TestMethod]
        public void HIncrByFloat_NonExistingKey_ReturnSpecified()
        {
            using (var client = new RedisClient(this.Host))
            {
                //set nonExistingKey to 0, then perform the operation
                var result = client.HIncrByFloat("nonExistingKey".ToByte(), "age".ToByte(), "0.25".ToByte());
                Assert.AreEqual("0.25", result.BytesToString());
            }
        }

        [TestMethod]
        public void HKeys_KeyExist_ShouldReturnAllFieldNames()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);

                var result = client.HKeys("mykey".ToByte());
                Assert.IsNotNull(result);
                var split = result.MultiBytesToString().Split(new char[] { '\r', '\n' });
                var list = new List<string>();
                list.AddRange(split);
                Assert.IsTrue(list.Contains("name"));
                Assert.IsTrue(list.Contains("age"));
                Assert.IsTrue(list.Contains("email"));
            }
        }

        [TestMethod]
        public void HKeys_KeyNotExist_ShouldReturnEmpty()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.HKeys("nonExistingKey".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("", result.MultiBytesToString());
            }
        }

        [TestMethod]
        public void HLen_KeyExist_ShouldReturnNumberOfEntries()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);

                var result = client.HLen("mykey".ToByte());
                Assert.AreEqual(3, result);
            }
        }

        [TestMethod]
        public void HLen_KeyNotExist_ShouldReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.HLen("mykey".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void HMGet_KeyExist_ShouldReturnValuesOfSpecifiedFields()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "val";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);

                var result = client.HMGet("mykey".ToByte(), "age".ToByte(), "email".ToByte());
                Assert.IsNotNull(result);
                var split = result.MultiBytesToString().Split(new char[] { '\r', '\n' });
                var list = new List<string>();
                list.AddRange(split);
                Assert.IsTrue(list.Contains("20"));
                Assert.IsFalse(list.Contains("val"));
                Assert.IsTrue(list.Contains("test@ttest.com"));
            }
        }

        [TestMethod]
        public void HMGet_KeyNotExist_ShouldReturnNilValuesOfSpecifiedFields()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.HMGet("mykey".ToByte(), "age".ToByte(), "email".ToByte());
                Assert.IsNotNull(result);
                var split = result.MultiBytesToString().Split(new char[] { '\r', '\n' });
                var list = new List<string>();
                list.AddRange(split);
                Assert.IsFalse(list.Contains("20"));
                Assert.IsFalse(list.Contains("val"));
                Assert.IsFalse(list.Contains("test@ttest.com"));
            }
        }

        [TestMethod]
        public void HMSet_KeyAlreadyExist_ShouldReplaceExistingKey()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);

                var tbl2 = new Hashtable();
                tbl2["name"] = "test2";
                tbl2["newfield"] = "sup";
                client.Hashes.HMSet("mykey", tbl2);

                var result = client.Hashes.HMGetAll("mykey");
                Assert.IsNotNull(result);
                Assert.AreEqual("test2", result["name"]);
                Assert.AreEqual("20", result["age"]);
                Assert.AreEqual("test@ttest.com", result["email"]);
                Assert.AreEqual("sup", result["newfield"]);
            }
        }

        [TestMethod]
        public void HMSet_KeyNotExist_ShouldStoreHashtable()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);

                var result = client.Hashes.HMGetAll("mykey");
                Assert.IsNotNull(result);
                Assert.AreEqual("test", result["name"]);
                Assert.AreEqual("20", result["age"]);
                Assert.AreEqual("test@ttest.com", result["email"]);
            }
        }

        [TestMethod]
        public void HSet_KeyExistFieldNotExist_ShouldCreateAndSet()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);

                var result = client.HSet("mykey".ToByte(), "num".ToByte(), "10".ToByte());
                Assert.AreEqual(1, result);
                var result2 = client.HGet("mykey".ToByte(), "num".ToByte());
                Assert.AreEqual("10", result2.BytesToString());
            }
        }

        [TestMethod]
        public void HSet_KeyExistFieldExist_ShouldReplace()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);

                var result = client.HSet("mykey".ToByte(), "age".ToByte(), "10".ToByte());
                Assert.AreEqual(0, result);
                var result2 = client.HGet("mykey".ToByte(), "age".ToByte());
                Assert.AreEqual("10", result2.BytesToString());
            }
        }

        [TestMethod]
        public void HSet_KeyNotExist_ShouldCreateAndSet()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.HSet("mykey".ToByte(), "name".ToByte(), "test".ToByte());
                Assert.AreEqual(1, result);
                var result2 = client.HGet("mykey".ToByte(), "name".ToByte());
                Assert.AreEqual("test", result2.BytesToString());
            }
        }

        [TestMethod]
        public void HSetNx_KeyNotExist_ShouldSet()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.HSetNx("mykey".ToByte(), "name".ToByte(), "test".ToByte());
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void HSetNx_KeyExistFieldNotExist_ShouldSet()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);

                var result = client.HSetNx("mykey".ToByte(), "num".ToByte(), "10".ToByte());
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void HSetNx_KeyExistFieldExist_ShouldIgnore()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "test";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);

                var result = client.HSetNx("mykey".ToByte(), "age".ToByte(), "10".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void HSetNx_AnyKeyExist_ReturnZero()
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
        public void HVals_KeyExist_ShouldReturnAllFieldNames()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "val";
                tbl["age"] = 20;
                tbl["email"] = "test@ttest.com";
                client.Hashes.HMSet("mykey", tbl);

                var result = client.KVals("mykey".ToByte());
                Assert.IsNotNull(result);
                var split = result.MultiBytesToString().Split(new char[] { '\r', '\n' });
                var list = new List<string>();
                list.AddRange(split);
                Assert.IsTrue(list.Contains("val"));
                Assert.IsTrue(list.Contains("20"));
                Assert.IsTrue(list.Contains("test@ttest.com"));
            }
        }

        [TestMethod]
        public void HVals_KeyNotExist_ShouldReturnEmpty()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.KVals("nonExistingKey".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("", result.MultiBytesToString());
            }
        }
    }
}
