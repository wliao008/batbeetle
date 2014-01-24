using Batbeetle;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitTest
{
    [TestClass]
    public class SetTests : TestBase
    {
        [TestMethod]
        public void SAdd_KeyNotExist_CreateNewAndAddMembers()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.SAdd("myset".ToByte(), "Hello".ToByte());
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void SAdd_KeyExistMemberExists_ReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.SAdd("myset".ToByte(), "Hello".ToByte());
                Assert.AreEqual(1, result);
                result = client.SAdd("myset".ToByte(), "World".ToByte());
                Assert.AreEqual(1, result);
                result = client.SAdd("myset".ToByte(), "World".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void SCard_KeyExist_ReturnCardinality()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.SAdd("myset".ToByte(), "Hello".ToByte());
                Assert.AreEqual(1, result);
                result = client.SAdd("myset".ToByte(), "World".ToByte());
                Assert.AreEqual(1, result);
                result = client.SCard("myset".ToByte());
                Assert.AreEqual(2, result);
            }
        }

        [TestMethod]
        public void SCard_KeyNotExist_ReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.SCard("myset".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void SDiff_ValidSets_ReturnDifferencesInMembers()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.SAdd("key1".ToByte(), "a".ToByte());
                client.SAdd("key1".ToByte(), "b".ToByte());
                client.SAdd("key1".ToByte(), "c".ToByte());
                client.SAdd("key2".ToByte(), "c".ToByte());
                client.SAdd("key2".ToByte(), "d".ToByte());
                client.SAdd("key2".ToByte(), "e".ToByte());
                var result = client.SDiff("key1".ToByte(), "key2".ToByte());
                var list = result.MultiBytesToList();
                Assert.AreEqual(2, list.Count);
                Assert.IsTrue(list.Contains("a"));
                Assert.IsTrue(list.Contains("b"));
                Assert.IsFalse(list.Contains("c"));
            }
        }

        [TestMethod]
        public void SDiff_InvalidKeys_ReturnEmpty()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.SDiff("key1".ToByte(), "key2".ToByte());
                var list = result.MultiBytesToList();
                Assert.AreEqual(0, list.Count);
            }
        }

        [TestMethod]
        public void SDiffStore_DestinationNotExist_StoreResultThere()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.SAdd("key1".ToByte(), "a".ToByte());
                client.SAdd("key1".ToByte(), "b".ToByte());
                client.SAdd("key1".ToByte(), "c".ToByte());
                client.SAdd("key2".ToByte(), "c".ToByte());
                client.SAdd("key2".ToByte(), "d".ToByte());
                client.SAdd("key2".ToByte(), "e".ToByte());
                var result = client.SDiffStore("newkey".ToByte(), "key1".ToByte(), "key2".ToByte()); ;
                Assert.AreEqual(2, result);
            }
        }

        [TestMethod]
        public void SDiffStore_DestinationExist_Overwrite()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.SAdd("key1".ToByte(), "a".ToByte());
                client.SAdd("key1".ToByte(), "b".ToByte());
                client.SAdd("key1".ToByte(), "c".ToByte());
                client.SAdd("key2".ToByte(), "c".ToByte());
                client.SAdd("key2".ToByte(), "d".ToByte());
                client.SAdd("key2".ToByte(), "e".ToByte());
                client.SAdd("newkey".ToByte(), "new".ToByte());
                var result = client.SDiffStore("newkey".ToByte(), "key1".ToByte(), "key2".ToByte()); ;
                Assert.AreEqual(2, result);
            }
        }

        [TestMethod]
        public void SInter_SetsWithValues_ReturnIntersection()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.SAdd("key1".ToByte(), "a".ToByte());
                client.SAdd("key1".ToByte(), "b".ToByte());
                client.SAdd("key1".ToByte(), "c".ToByte());
                client.SAdd("key2".ToByte(), "c".ToByte());
                client.SAdd("key2".ToByte(), "d".ToByte());
                client.SAdd("key2".ToByte(), "e".ToByte());
                var result = client.SInter("key1".ToByte(), "key2".ToByte()); ;
                var list = result.MultiBytesToList();
                Assert.AreEqual(1, list.Count);
                Assert.AreEqual("c", list[0]);
            }
        }

        [TestMethod]
        public void SInter_KeyNotExisting_ReturnEmpty()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.SInter("key1".ToByte(), "key2".ToByte()); ;
                var list = result.MultiBytesToList();
                Assert.AreEqual(0, list.Count);
            }
        }

        [TestMethod]
        public void SInterStore_DestinationNotExist_StoreResultThere()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.SAdd("key1".ToByte(), "a".ToByte());
                client.SAdd("key1".ToByte(), "b".ToByte());
                client.SAdd("key1".ToByte(), "c".ToByte());
                client.SAdd("key2".ToByte(), "c".ToByte());
                client.SAdd("key2".ToByte(), "d".ToByte());
                client.SAdd("key2".ToByte(), "e".ToByte());
                var result = client.SInterStore("newkey".ToByte(), "key1".ToByte(), "key2".ToByte()); ;
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void SInterStore_DestinationExist_Overwrite()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.SAdd("key1".ToByte(), "a".ToByte());
                client.SAdd("key1".ToByte(), "b".ToByte());
                client.SAdd("key1".ToByte(), "c".ToByte());
                client.SAdd("key2".ToByte(), "c".ToByte());
                client.SAdd("key2".ToByte(), "d".ToByte());
                client.SAdd("key2".ToByte(), "e".ToByte());
                client.SAdd("newkey".ToByte(), "new".ToByte());
                var result = client.SInterStore("newkey".ToByte(), "key1".ToByte(), "key2".ToByte()); ;
                Assert.AreEqual(1, result);
            }
        }
    }
}
