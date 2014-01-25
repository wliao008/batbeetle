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

        [TestMethod]
        public void SIsMember_KeyExistsMemberExists_ShouldReturnOne()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.SAdd("myset".ToByte(), "Hello".ToByte());
                var result = client.SIsMember("myset".ToByte(), "Hello".ToByte());
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void SIsMember_KeyExistsMemberNotExists_ShouldReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.SAdd("myset".ToByte(), "Hello".ToByte());
                var result = client.SIsMember("myset".ToByte(), "World".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void SIsMember_KeyNotExists_ShouldReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.SIsMember("myset".ToByte(), "Hello".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void SMembers_KeyExists_ShouldReturnAllMembers()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.SAdd("myset".ToByte(), "Hello".ToByte(), "World".ToByte());
                var result = client.SMembers("myset".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(2, list.Count);
                Assert.IsTrue(list.Contains("Hello"));
                Assert.IsTrue(list.Contains("World"));
            }
        }

        [TestMethod]
        public void SMembers_KeyNotExists_ShouldReturnEmpty()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.SMembers("myset".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(0, list.Count);
            }
        }

        [TestMethod]
        public void SMove_SourceNotExists_ShouldDoNothing()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.SMove("source".ToByte(), "dest".ToByte(), "mem".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void SMove_SourceExistsMemberNotExist_ShouldDoNothingAndReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.SAdd("source".ToByte(), "Hello".ToByte());
                var result = client.SMove("source".ToByte(), "dest".ToByte(), "mem".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void SMove_SourceDestinationExist_ShouldMoveElement()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.SAdd("source".ToByte(), "Hello".ToByte());
                client.SAdd("destination".ToByte(), "World".ToByte());
                var result = client.SMove("source".ToByte(), "destination".ToByte(), "Hello".ToByte());
                Assert.AreEqual(1, result);
                result = client.SCard("source".ToByte());
                Assert.AreEqual(0, result);
                result = client.SCard("destination".ToByte());
                Assert.AreEqual(2, result);
            }
        }

        [TestMethod]
        public void SMove_SourceDestinationMemberExist_ShouldJustRemoveElementFromSource()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.SAdd("source".ToByte(), "World".ToByte());
                client.SAdd("destination".ToByte(), "World".ToByte());
                var result = client.SMove("source".ToByte(), "destination".ToByte(), "World".ToByte());
                Assert.AreEqual(1, result);
                result = client.SCard("source".ToByte());
                Assert.AreEqual(0, result);
                result = client.SCard("destination".ToByte());
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void SMove_WrontDataType_ShouldReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("source", "source");
                client.Set("destination", "destination");
                var result = client.SMove("source".ToByte(), "destination".ToByte(), "World".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void SPop_SetExists_ShouldRemoveAndReturnRandomElement()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.SAdd("myset".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                var result = client.SPop("myset".ToByte());
                Assert.IsNotNull(result);
                var num = client.SCard("myset".ToByte());
                Assert.AreEqual(2, num);
            }
        }

        [TestMethod]
        public void SPop_SetNotExists_ShouldReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.SPop("myset".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void SRandMember_KeyExists_ShouldReturnRandomElement()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.SAdd("myset".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                var result = client.SRandMember("myset".ToByte(), null);
                Assert.IsNotNull(result);
                var num = client.SCard("myset".ToByte());
                Assert.AreEqual(3, num);
            }
        }

        [TestMethod]
        public void SRandMember_KeyExistsWithCount_ShouldReturnCountRandomElement()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.SAdd("myset".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                var result = client.SRandMember("myset".ToByte(), "2".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(2, list.Count);
                var num = client.SCard("myset".ToByte());
                Assert.AreEqual(3, num);
            }
        }

        [TestMethod]
        public void SRandMember_KeyNotExists_ShouldReturnEmpty()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.SRandMember("myset".ToByte(), null);
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void SRandMember_KeyNotExistsWithCount_ShouldReturnEmptyList()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.SRandMember("myset".ToByte(), "2".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(0, list.Count);
            }
        }

        [TestMethod]
        public void SRem_KeyExistsMembersExists_ShouldRemoveMembers()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.SAdd("myset".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                client.SAdd("myset".ToByte(), "four".ToByte(), "five".ToByte(), "six".ToByte());
                var result = client.SRem("myset".ToByte(), "five".ToByte(), "six".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual(2, result);
            }
        }

        [TestMethod]
        public void SRem_KeyExistsMembersNotExists_ShouldOnlyRemoveExistingMembers()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.SAdd("myset".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                client.SAdd("myset".ToByte(), "four".ToByte(), "five".ToByte(), "six".ToByte());
                var result = client.SRem("myset".ToByte(), "five".ToByte(), "seven".ToByte());
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void SRem_KeyNotExists_ShouldReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.SRem("myset".ToByte(), "five".ToByte(), "seven".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void SRem_WrongDataType_ShouldReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("myset", "val");
                var result = client.SRem("myset".ToByte(), "five".ToByte(), "seven".ToByte());
                Assert.IsNull(result);
            }
        }
    }
}
