using Batbeetle;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitTest
{
    [TestClass]
    public class SortedSetTests : TestBase
    {
        [TestMethod]
        public void ZAdd_ValidParams_ShouldAddMemberWithScore()
        {
            using (var client = new RedisClient(this.Host))
            {
                var scores = new byte[1][];
                var members = new byte[1][];
                scores[0] = "1".ToByte();
                members[0] = "one".ToByte();
                var result = client.ZAdd("key1".ToByte(), scores, members);
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void ZAdd_MultipleValidParams_ShouldAddMembersWithScores()
        {
            using (var client = new RedisClient(this.Host))
            {
                var scores = new byte[2][];
                var members = new byte[2][];
                scores[0] = "1".ToByte(); scores[1] = "2".ToByte();
                members[0] = "one".ToByte(); members[1] = "two".ToByte();
                var result = client.ZAdd("key1".ToByte(), scores, members);
                Assert.AreEqual(2, result);
            }
        }

        [TestMethod]
        public void ZAdd_MemberExists_ShouldJustUpdateScore()
        {
            using (var client = new RedisClient(this.Host))
            {
                var scores = new byte[2][];
                var members = new byte[2][];
                scores[0] = "1".ToByte(); scores[1] = "2".ToByte();
                members[0] = "one".ToByte(); members[1] = "two".ToByte();
                var result = client.ZAdd("key1".ToByte(), scores, members);
                Assert.AreEqual(2, result);
                scores[1] = "11".ToByte(); //update score for member[1]
                result = client.ZAdd("key1".ToByte(), scores, members);
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void ZCard_KeyExist_ReturnCardinality()
        {
            using (var client = new RedisClient(this.Host))
            {
                var scores = new byte[2][];
                var members = new byte[2][];
                scores[0] = "1".ToByte(); scores[1] = "2".ToByte();
                members[0] = "one".ToByte(); members[1] = "two".ToByte();

                var result = client.ZAdd("key1".ToByte(), scores, members);
                Assert.AreEqual(2, result);
                result = client.ZCard("key1".ToByte());
                Assert.AreEqual(2, result);
            }
        }

        [TestMethod]
        public void ZCard_KeyNotExist_ReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.ZCard("myset".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void ZCount_KeyExist_ReturnInclusiveNumOfElementsBetweenRange()
        {
            using (var client = new RedisClient(this.Host))
            {
                var scores = new byte[3][];
                var members = new byte[3][];
                scores[0] = "1".ToByte();
                scores[1] = "2".ToByte();
                scores[2] = "3".ToByte();
                members[0] = "one".ToByte();
                members[1] = "two".ToByte();
                members[2] = "three".ToByte();

                client.ZAdd("key".ToByte(), scores, members);
                var result = client.ZCount("key".ToByte(), "1".ToByte(), "3".ToByte());
                Assert.AreEqual(3, result);
            }
        }

        [TestMethod]
        public void ZCount_KeyExist_ReturnExclusiveNumOfElementsBetweenRange()
        {
            using (var client = new RedisClient(this.Host))
            {
                var scores = new byte[3][];
                var members = new byte[3][];
                scores[0] = "1".ToByte();
                scores[1] = "2".ToByte();
                scores[2] = "3".ToByte();
                members[0] = "one".ToByte();
                members[1] = "two".ToByte();
                members[2] = "three".ToByte();

                client.ZAdd("key".ToByte(), scores, members);
                var result = client.ZCount("key".ToByte(), "(1".ToByte(), "3".ToByte());
                Assert.AreEqual(2, result);
            }
        }

        [TestMethod]
        public void ZCount_KeyExist_ReturnAllNumOfElementsBetweenRange()
        {
            using (var client = new RedisClient(this.Host))
            {
                var scores = new byte[3][];
                var members = new byte[3][];
                scores[0] = "1".ToByte();
                scores[1] = "2".ToByte();
                scores[2] = "3".ToByte();
                members[0] = "one".ToByte();
                members[1] = "two".ToByte();
                members[2] = "three".ToByte();

                client.ZAdd("key".ToByte(), scores, members);
                var result = client.ZCount("key".ToByte(), "-inf".ToByte(), "+inf".ToByte());
                Assert.AreEqual(3, result);
            }
        }

        [TestMethod]
        public void ZIncrBy_KeyExists_IncrementScoreOfMemberBySpecified()
        {
            using (var client = new RedisClient(this.Host))
            {
                var scores = new byte[3][];
                var members = new byte[3][];
                scores[0] = "1".ToByte();
                scores[1] = "2".ToByte();
                scores[2] = "3".ToByte();
                members[0] = "one".ToByte();
                members[1] = "two".ToByte();
                members[2] = "three".ToByte();
                client.ZAdd("key".ToByte(), scores, members);
                var result = client.ZIncrBy("key".ToByte(), "2".ToByte(), "one".ToByte());
                Assert.AreEqual("3", result.BytesToString());
            }
        }

        [TestMethod]
        public void ZIncrBy_KeyNotExists_CreateThenIncrementScoreOfMemberBySpecified()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.ZIncrBy("key".ToByte(), "2".ToByte(), "one".ToByte());
                Assert.AreEqual("2", result.BytesToString());
            }
        }

        [TestMethod]
        public void ZIncrBy_WrongDateType_ShouldReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("key", "val");
                var result = client.ZIncrBy("key".ToByte(), "2".ToByte(), "one".ToByte());
                Assert.IsNull(result);
            }
        }
    }
}
