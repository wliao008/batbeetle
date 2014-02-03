using Batbeetle;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections;
using System.Collections.Generic;
using System;

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
                client.Strings.Set("key1", "val1");
                client.Strings.Set("key2", "val2");
                client.Strings.Set("key3", "val3");
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
                client.Strings.Set("mykey", "10");
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
                client.Strings.Set("mykey", "10");
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
                client.Strings.Set("mykey", "10");
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
                client.Strings.Set("mykey", "10");
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
                client.Strings.Set("one", "1");
                client.Strings.Set("two", "2");
                client.Strings.Set("three", "3");
                client.Strings.Set("four", "4");
                var result = client.Keys("*o*".ToByte());
                Assert.IsNotNull(result);
                var split = result.MultiBytesToString().Split(new char[] { '\r', '\n' });
                var list = new List<string>();
                list.AddRange(split);
                Assert.IsTrue(list.Contains("one"));
                Assert.IsTrue(list.Contains("two"));
                Assert.IsTrue(list.Contains("four"));
            }
        }

        [TestMethod]
        public void Keys_ValidPattern_ReturnValuesOfMatched2()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("one", "1");
                client.Strings.Set("two", "2");
                client.Strings.Set("three", "3");
                client.Strings.Set("four", "4");
                var result = client.Keys("t??".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("two\r\n", result.MultiBytesToString());
            }
        }

        [TestMethod]
        public void Keys_ValidPattern_ReturnValuesOfMatched3()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("one", "1");
                client.Strings.Set("two", "2");
                client.Strings.Set("three", "3");
                client.Strings.Set("four", "4");
                var result = client.Keys("*".ToByte());
                Assert.IsNotNull(result);
                var split = result.MultiBytesToString().Split(new char[] { '\r', '\n' });
                var list = new List<string>();
                list.AddRange(split);
                Assert.IsTrue(list.Contains("one"));
                Assert.IsTrue(list.Contains("two"));
                Assert.IsTrue(list.Contains("three"));
                Assert.IsTrue(list.Contains("four"));
            }
        }

        [TestMethod]
        public void Keys_InValidPattern_ReturnEmpty()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("one", "1");
                client.Strings.Set("two", "2");
                client.Strings.Set("three", "3");
                client.Strings.Set("four", "4");
                var result = client.Keys("nonmatching".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("", result.MultiBytesToString());
            }
        }

        [TestMethod]
        public void Move_ValidKey_MoveKeyToSpecifiedDb()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "val");
                var result = client.Move("mykey".ToByte(), "1".ToByte());
                Assert.AreEqual(1, result);
                client.Select("1".ToByte());
                var result2 = client.Strings.Get("mykey");
                Assert.AreEqual("val", result2);
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
                client.Strings.Set("mykey", "val");
                var result = client.Object(Commands.Refcount, "mykey".ToByte());
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void Object_RefCountInValidKey_ReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.Object(Commands.Refcount, "nonExistingKey".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void Object_IdleTimeValidKey_ReturnTimeIdled()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "val");
                var result = client.Object(Commands.Idletime, "mykey".ToByte());
                Assert.IsNotNull(result);
            }
        }

        [TestMethod]
        public void ObjectEncoding_ValidKey_ReturnEncoding()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "val");
                var result = client.ObjectEncoding("mykey".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("raw", result.BytesToString());
            }
        }

        [TestMethod]
        public void ObjectEncoding_ValidKey_ReturnEncoding2()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "10");
                var result = client.ObjectEncoding("mykey".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("int", result.BytesToString());
            }
        }

        [TestMethod]
        public void ObjectEncoding_ValidKeyChangedData_ReturnCorrectEncoding()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "1000");
                var result = client.ObjectEncoding("mykey".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("int", result.BytesToString());
                client.Strings.Append("mykey", "bar");
                result = client.ObjectEncoding("mykey".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("raw", result.BytesToString());
            }
        }

        [TestMethod]
        public void Persist_ValidKey_RemoveTimeout()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "1000", 10);
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
                client.Strings.Set("mykey", "10");
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
                client.Strings.Set("mykey", "10");
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

        [TestMethod]
        public void PTtl_KeyWithExpiration_ReturnTtlInMilliseconds()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "val");
                client.Expire("mykey".ToByte(), "1".ToByte());
                var result = client.PTtl("mykey".ToByte());
                Assert.AreNotEqual(-2, result);
                Assert.AreNotEqual(-1, result);
                Assert.AreNotEqual(0, result);
            }
        }

        [TestMethod]
        public void PTtl_KeyWithNoExpiration_ReturnMinusOne()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "val");
                var result = client.PTtl("mykey".ToByte());
                Assert.AreEqual(-1, result);
            }
        }

        [TestMethod]
        public void PTtl_NonExistingKey_ReturnMinusOne()
        {
            //this is only for v2.6 or older, 
            //in v2.8+, non existing key returns -2
            using (var client = new RedisClient(this.Host))
            {
                var result = client.PTtl("nonExistingKey".ToByte());
                Assert.AreEqual(-1, result);
            }
        }

        [TestMethod]
        public void RandomKey_ExistingKeys_ReturnARandomKey()
        {
            using (var client = new RedisClient(this.Host))
            {
                for (int i = 0; i < 10; ++i)
                    client.Strings.Set("key" + i, i.ToString());
                var result = client.RandomKey();
                Assert.IsNotNull(result);
            }
        }

        [TestMethod]
        public void RandomKey_NoExistingKeys_ReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.RandomKey();
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void Rename_ExistingKey_ShouldRenameToNewKey()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "val");
                client.Rename("mykey".ToByte(), "mynewkey".ToByte());
                var result = client.Strings.Get("mykey");
                Assert.IsNull(result);
                result = client.Strings.Get("mynewkey");
                Assert.IsNotNull(result);
                Assert.AreEqual("val", result);
            }
        }

        [TestMethod]
        public void Rename_SameKey_ReturnNull()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "val");
                var result = client.Rename("mykey".ToByte(), "mykey".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void Rename_TargetKeyExist_ShouldOverwrite()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "val");
                client.Strings.Set("mynewkey", "new value");
                client.Rename("mykey".ToByte(), "mynewkey".ToByte());
                var result = client.Strings.Get("mynewkey");
                Assert.IsNotNull(result);
                Assert.AreEqual("val", result);
            }
        }

        [TestMethod]
        public void RenameNx_TargetKeyExist_ShouldReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "val");
                client.Strings.Set("mynewkey", "new value");
                client.RenameNx("mykey".ToByte(), "mynewkey".ToByte());
                var result = client.Strings.Get("mynewkey");
                Assert.IsNotNull(result);
                Assert.AreEqual("new value", result);
            }
        }

        [TestMethod]
        public void RenameNx_TargetKeyNotExist_ShouldSetAndReturnOne()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "val");
                client.RenameNx("mykey".ToByte(), "mynewkey".ToByte());
                var result = client.Strings.Get("mynewkey");
                Assert.IsNotNull(result);
                Assert.AreEqual("val", result);
            }
        }

        [TestMethod]
        public void RenameNx_SameKey_ReturnNull()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "val");
                var result = client.RenameNx("mykey".ToByte(), "mykey".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void Restore_FromValidDeserializedValue_ShouldRestoreKeyVal()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "hi");
                var serializedVal = client.Dump("mykey".ToByte());
                client.Del("mykey".ToByte());
                var result = client.Restore("mykey".ToByte(), "0".ToByte(), serializedVal);
                Assert.IsNotNull(result);
                Assert.AreEqual("OK", result);
            }
        }

        [TestMethod]
        public void Sort_SimpleList_ShouldSort()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "5".ToByte(), "3".ToByte());
                var result = client.Sort("mylist".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(2, list.Count);
                Assert.AreEqual("3", list[0]);
                Assert.AreEqual("5", list[1]);
            }
        }

        [TestMethod]
        public void Sort_SortByDesc_ShouldSortByDesc()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "5".ToByte(), "3".ToByte());
                var result = client.Sort("mylist".ToByte(), null, null, null, null, false);
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(2, list.Count);
                Assert.AreEqual("5", list[0]);
                Assert.AreEqual("3", list[1]);
            }
        }

        [TestMethod]
        public void Sort_SimpleStringList_ShouldNotSortByDefault()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "abc".ToByte(), "bcd".ToByte());
                var result = client.Sort("mylist".ToByte());///TODO: should result be null here?
                var list = result.MultiBytesToList();
                Assert.AreEqual(0, list.Count);
            }
        }

        [TestMethod]
        public void Sort_SimpleStringListAlpha_ShouldSort()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "abc".ToByte(), "bcd".ToByte());
                var result = client.Sort("mylist".ToByte(), null, null, null, null, true, true);
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(2, list.Count);
                Assert.AreEqual("abc", list[0]);
                Assert.AreEqual("bcd", list[1]);
            }
        }

        [TestMethod]
        public void Sort_SimpleStringListAlphaDesc_ShouldSortByDesc()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "abc".ToByte(), "bcd".ToByte());
                var result = client.Sort("mylist".ToByte(), null, null, null, null, false, true);
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(2, list.Count);
                Assert.AreEqual("bcd", list[0]);
                Assert.AreEqual("abc", list[1]);
            }
        }

        [TestMethod]
        public void Sort_Limit_ShouldSortAndReturnLimitElements()
        {
            Random rand = new Random((int)DateTime.Now.Ticks);
            using (var client = new RedisClient(this.Host))
            {
                for (int i = 0; i < 100; ++i)
                    client.RPush("mylist".ToByte(), rand.Next(1, 100).ToByte());
                var result = client.Sort("mylist".ToByte(), null, "0".ToByte(), "10".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(10, list.Count);
            }
        }

        [TestMethod]
        public void Sort_LimitStore_ShouldSortAndStoreReturnLimitElements()
        {
            Random rand = new Random((int)DateTime.Now.Ticks);
            using (var client = new RedisClient(this.Host))
            {
                for (int i = 0; i < 100; ++i)
                    client.RPush("mylist".ToByte(), rand.Next(1, 100).ToByte());
                var result = client.Sort(
                    "mylist".ToByte(),
                    null,
                    "0".ToByte(),
                    "10".ToByte(),
                    null,
                    true,
                    false,
                    "newkey".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(1, list.Count);
                Assert.AreEqual("10", list[0]);
            }
        }

        [TestMethod]
        public void Type_ValidKeyString_ReturnStringDataType()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("mykey", "val");
                var result = client.Type("mykey".ToByte());
                Assert.AreEqual("string", result);
            }
        }

        [TestMethod]
        public void Type_ValidKeyHash_ReturnHashDataType()
        {
            using (var client = new RedisClient(this.Host))
            {
                Hashtable tbl = new Hashtable();
                tbl["key1"] = "val1";
                tbl["age"] = 2;
                client.Hashes.HMSet("mykey", tbl);
                var result = client.Type("mykey".ToByte());
                Assert.AreEqual("hash", result);
            }
        }

        [TestMethod]
        public void Type_InValidKey_ReturnNone()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.Type("mykey".ToByte());
                Assert.AreEqual("none", result);
            }
        }
    }
}
