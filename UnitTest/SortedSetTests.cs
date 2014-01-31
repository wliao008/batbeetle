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
                client.Strings.Set("key", "val");
                var result = client.ZIncrBy("key".ToByte(), "2".ToByte(), "one".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void ZRange_KeyExist_ShouldReturnValuesInRange()
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
                var result = client.ZRange("key".ToByte(), "0".ToByte(), "-1".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(3, list.Count);
            }
        }

        [TestMethod]
        public void ZRange_KeyExistWithScores_ShouldReturnValuesInRange()
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
                var result = client.ZRange("key".ToByte(), "0".ToByte(), "-1".ToByte(), true);
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(6, list.Count);
            }
        }

        [TestMethod]
        public void ZRange_KeyExistStartLargerThanEnd_ShouldReturnEmpty()
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

                var result = client.ZRange("key".ToByte(), "3".ToByte(), "1".ToByte(), true);
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(0, list.Count);
            }
        }

        [TestMethod]
        public void ZRange_KeyExistStopLargerThanStart_ShouldReturnEmpty()
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

                var result = client.ZRange("key".ToByte(), "10".ToByte(), "1".ToByte(), true);
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(0, list.Count);
            }
        }

        [TestMethod]
        public void ZRangeByScore_KeyExistInf_ShouldReturnAll()
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
                var result = client.ZRangeByScore("key".ToByte(), "-inf".ToByte(), "+inf".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(3, list.Count);
            }
        }

        [TestMethod]
        public void ZRangeByScore_KeyExistInfWithScore_ShouldReturnAllValuesWithScores()
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
                var result = client.ZRangeByScore("key".ToByte(), "-inf".ToByte(), "+inf".ToByte(), true);
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(6, list.Count);
            }
        }

        [TestMethod]
        public void ZRangeByScore_KeyExistInfWithLimit_ShouldReturnAllValuesByLimit()
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
                var result = client.ZRangeByScore("key".ToByte(), "-inf".ToByte(), "+inf".ToByte(), false, true, "1".ToByte(), "2".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(2, list.Count);
            }
        }

        [TestMethod]
        public void ZRangeByScore_KeyExistInfWithScoreLimit_ShouldReturnAllValuesWithScores()
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
                var result = client.ZRangeByScore("key".ToByte(), "-inf".ToByte(), "+inf".ToByte(), true, true, "1".ToByte(), "2".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(4, list.Count);
            }
        }

        [TestMethod]
        public void ZRangeByScore_KeyExistRange_ShouldReturnValueInRange()
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
                var result = client.ZRangeByScore("key".ToByte(), "1".ToByte(), "2".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(2, list.Count);
            }
        }

        [TestMethod]
        public void ZRangeByScore_KeyExistExclusiveMin_ShouldReturnValueInRange()
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
                var result = client.ZRangeByScore("key".ToByte(), "(1".ToByte(), "2".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(1, list.Count);
            }
        }

        [TestMethod]
        public void ZRangeByScore_KeyExistExclusiveMinMax_ShouldReturnValueInRange()
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
                var result = client.ZRangeByScore("key".ToByte(), "(1".ToByte(), "(2".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(0, list.Count);
            }
        }

        [TestMethod]
        public void ZRank_KeyMemberExist_ReturnRank()
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
                var result = client.ZRank("key".ToByte(), "two".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void ZRank_KeyExistMemberNotExist_ReturnNil()
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
                var result = client.ZRank("key".ToByte(), "four".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void ZRank_KeyNotExistMemberNotExist_ReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.ZRank("key".ToByte(), "four".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void ZRem_KeyExistsMembersExists_ShouldRemoveMembers()
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
                var result = client.ZRem("key".ToByte(), "one".ToByte(), "two".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual(2, result);
            }
        }

        [TestMethod]
        public void ZRem_KeyExistsMembersNotExists_ShouldOnlyRemoveExistingMembers()
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
                var result = client.ZRem("key".ToByte(), "six".ToByte(), "seven".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void ZRem_KeyNotExists_ShouldReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.ZRem("key".ToByte(), "five".ToByte(), "seven".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void ZRem_WrongDataType_ShouldReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Strings.Set("key", "val");
                var result = client.ZRem("key".ToByte(), "five".ToByte(), "seven".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void ZRemRangeByRank_KeyExists_ShouldRemoveMembersInRank()
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

                var result = client.ZRemRangeByRank("key".ToByte(), "0".ToByte(), "1".ToByte());
                Assert.AreEqual(2, result);
            }
        }

        [TestMethod]
        public void ZRemRangeByRank_KeyExistsStartEqualsStop_ShouldRemove1Member()
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

                var result = client.ZRemRangeByRank("key".ToByte(), "0".ToByte(), "0".ToByte());
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void ZRemRangeByRank_KeyExistsStartGreaterThanStop_ShouldRemove0Member()
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

                var result = client.ZRemRangeByRank("key".ToByte(), "1".ToByte(), "0".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void ZRemRangeByScore_KeyExists_ShouldRemoveMembersInScores()
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

                var result = client.ZRemRangeByScore("key".ToByte(), "1".ToByte(), "2".ToByte());
                Assert.AreEqual(2, result);
            }
        }

        [TestMethod]
        public void ZRemRangeByScore_KeyExistsInf_ShouldRemoveAll()
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

                var result = client.ZRemRangeByScore("key".ToByte(), "-inf".ToByte(), "+inf".ToByte());
                Assert.AreEqual(3, result);
            }
        }

        [TestMethod]
        public void ZRemRangeByScore_KeyExistsExclusive_ShouldRemoveOnlyAffectedRange()
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

                var result = client.ZRemRangeByScore("key".ToByte(), "(1".ToByte(), "(3".ToByte());
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void ZRemRangeByScore_KeyExistsMaxGreaterThanMin_ShouldRemoveMembersInScores()
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

                var result = client.ZRemRangeByScore("key".ToByte(), "2".ToByte(), "1".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void ZRevRange_KeyExist_ShouldReturnValuesInRangeInRevOrder()
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
                var result = client.ZRevRange("key".ToByte(), "0".ToByte(), "-1".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(3, list.Count);
                Assert.AreEqual("three", list[0]);
                Assert.AreEqual("two", list[1]);
                Assert.AreEqual("one", list[2]);
            }
        }

        [TestMethod]
        public void ZRevRange_KeyExistWithScores_ShouldReturnValuesInRangeInRevOrder()
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
                var result = client.ZRevRange("key".ToByte(), "0".ToByte(), "-1".ToByte(), true);
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(6, list.Count);
                Assert.AreEqual("three", list[0]);
                Assert.AreEqual("3", list[1]);
                Assert.AreEqual("two", list[2]);
                Assert.AreEqual("2", list[3]);
                Assert.AreEqual("one", list[4]);
                Assert.AreEqual("1", list[5]);
            }
        }

        [TestMethod]
        public void ZRevRange_KeyExistStartLargerThanEnd_ShouldReturnEmpty()
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

                var result = client.ZRevRange("key".ToByte(), "3".ToByte(), "1".ToByte(), true);
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(0, list.Count);
            }
        }

        [TestMethod]
        public void ZRevRange_KeyExistStopLargerThanStart_ShouldReturnEmpty()
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

                var result = client.ZRevRange("key".ToByte(), "10".ToByte(), "1".ToByte(), true);
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(0, list.Count);
            }
        }

        [TestMethod]
        public void ZRevRangeByScore_KeyExistInf_ShouldReturnAll()
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
                var result = client.ZRevRangeByScore("key".ToByte(), "+inf".ToByte(), "-inf".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(3, list.Count);
                Assert.AreEqual("three", list[0]);
                Assert.AreEqual("two", list[1]);
                Assert.AreEqual("one", list[2]);
            }
        }

        [TestMethod]
        public void ZRevRangeByScore_KeyExistInfWithScore_ShouldReturnAllValuesWithScores()
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
                var result = client.ZRevRangeByScore("key".ToByte(), "+inf".ToByte(), "-inf".ToByte(), true);
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(6, list.Count);
                Assert.AreEqual(6, list.Count);
                Assert.AreEqual("three", list[0]);
                Assert.AreEqual("3", list[1]);
                Assert.AreEqual("two", list[2]);
                Assert.AreEqual("2", list[3]);
                Assert.AreEqual("one", list[4]);
                Assert.AreEqual("1", list[5]);
            }
        }

        [TestMethod]
        public void ZRevRangeByScore_KeyExistInfWithLimit_ShouldReturnAllValuesByLimit()
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
                var result = client.ZRevRangeByScore("key".ToByte(), "+inf".ToByte(), "-inf".ToByte(), false, true, "1".ToByte(), "2".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(2, list.Count);
                Assert.AreEqual("two", list[0]);
                Assert.AreEqual("one", list[1]);
            }
        }

        [TestMethod]
        public void ZRevRangeByScore_KeyExistInfWithScoreLimit_ShouldReturnAllValuesWithScores()
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
                var result = client.ZRevRangeByScore("key".ToByte(), "+inf".ToByte(), "-inf".ToByte(), true, true, "1".ToByte(), "2".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(4, list.Count);
                Assert.AreEqual("two", list[0]);
                Assert.AreEqual("2", list[1]);
                Assert.AreEqual("one", list[2]);
                Assert.AreEqual("1", list[3]);
            }
        }

        [TestMethod]
        public void ZRevRangeByScore_KeyExistRange_ShouldReturnValueInRange()
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
                var result = client.ZRevRangeByScore("key".ToByte(), "2".ToByte(), "1".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(2, list.Count);
            }
        }

        [TestMethod]
        public void ZRevRangeByScore_KeyExistExclusiveMin_ShouldReturnValueInRange()
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
                var result = client.ZRevRangeByScore("key".ToByte(), "2".ToByte(), "(1".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(1, list.Count);
            }
        }

        [TestMethod]
        public void ZRevRangeByScore_KeyExistExclusiveMinMax_ShouldReturnValueInRange()
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
                var result = client.ZRevRangeByScore("key".ToByte(), "(2".ToByte(), "(1".ToByte());
                Assert.IsNotNull(result);
                var list = result.MultiBytesToList();
                Assert.AreEqual(0, list.Count);
            }
        }

        [TestMethod]
        public void ZRevRank_KeyMemberExist_ReturnRank()
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
                var result = client.ZRevRank("key".ToByte(), "two".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void ZRevRank_KeyExistMemberNotExist_ReturnNil()
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
                var result = client.ZRevRank("key".ToByte(), "four".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void ZRevRank_KeyNotExistMemberNotExist_ReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.ZRevRank("key".ToByte(), "four".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void ZScore_KeyExistMemberExist_ShouldReturnScore()
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
                var result = client.ZScore("key".ToByte(), "three".ToByte());
                Assert.AreEqual("3", result.BytesToString());
            }
        }

        [TestMethod]
        public void ZScore_KeyExistMemberNotExist_ShouldReturnNil()
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
                var result = client.ZScore("key".ToByte(), "thrfouree".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void ZScore_KeyNotExist_ShouldReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.ZScore("key".ToByte(), "thrfouree".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void ZUnionStore_ValidSets_ShouldGetUnionAndStoreInNewSet()
        {
            using (var client = new RedisClient(this.Host))
            {
                var scores1 = new byte[2][];
                var members1 = new byte[2][];
                scores1[0] = "1".ToByte();
                scores1[1] = "2".ToByte();
                members1[0] = "one".ToByte();
                members1[1] = "two".ToByte();

                var scores2 = new byte[3][];
                var members2 = new byte[3][];
                scores2[0] = "1".ToByte();
                scores2[1] = "2".ToByte();
                scores2[2] = "3".ToByte();
                members2[0] = "one".ToByte();
                members2[1] = "two".ToByte();
                members2[2] = "three".ToByte();

                client.ZAdd("set1".ToByte(), scores1, members1);
                client.ZAdd("set2".ToByte(), scores2, members2);

                var result = client.ZUnionStore(
                    "out".ToByte(),
                    "2".ToByte(),
                    new byte[2][] { "set1".ToByte(), "set2".ToByte() });

                Assert.AreEqual(3, result);
            }
        }

        [TestMethod]
        public void ZUnionStore_ValidSetsWithWeight_ShouldGetUnionAndStoreInNewSet()
        {
            using (var client = new RedisClient(this.Host))
            {
                var scores1 = new byte[2][];
                var members1 = new byte[2][];
                scores1[0] = "1".ToByte();
                scores1[1] = "2".ToByte();
                members1[0] = "one".ToByte();
                members1[1] = "two".ToByte();

                var scores2 = new byte[3][];
                var members2 = new byte[3][];
                scores2[0] = "1".ToByte();
                scores2[1] = "2".ToByte();
                scores2[2] = "3".ToByte();
                members2[0] = "one".ToByte();
                members2[1] = "two".ToByte();
                members2[2] = "three".ToByte();

                client.ZAdd("set1".ToByte(), scores1, members1);
                client.ZAdd("set2".ToByte(), scores2, members2);

                var result = client.ZUnionStore(
                    "out".ToByte(),
                    "2".ToByte(),
                    new byte[2][] { "set1".ToByte(), "set2".ToByte() },
                    true,
                    new byte[2][] { "2".ToByte(), "3".ToByte() });

                Assert.AreEqual(3, result);

                var result2 = client.ZRange("out".ToByte(), "0".ToByte(), "-1".ToByte(), true);
                var list = result2.MultiBytesToList();
                Assert.AreEqual(6, list.Count);
                Assert.AreEqual("one", list[0]);
                Assert.AreEqual("5", list[1]);
                Assert.AreEqual("three", list[2]);
                Assert.AreEqual("9", list[3]);
                Assert.AreEqual("two", list[4]);
                Assert.AreEqual("10", list[5]);
            }
        }
    }
}
