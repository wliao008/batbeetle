using System.Collections.Generic;
using System.ComponentModel;
using System.Threading;
using Batbeetle;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitTest
{
    [TestClass]
    public class ListTests : TestBase
    {
        [TestMethod]
        public void BlPop_EntriesInList_ShouldPop()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                var result = client.BLPop("0".ToByte(), "mylist".ToByte());
                var list = result.MultiBytesToList();
                Assert.AreEqual(2, list.Count);
                Assert.AreEqual("mylist", list[0]);
                Assert.AreEqual("one", list[1]);
            }
        }

        [TestMethod]
        public void BlPop_NoEntriesInList_ShouldBlock()
        {
            BackgroundWorker pop = new BackgroundWorker();
            var resetEvent = new ManualResetEvent(false);
            pop.DoWork += (s, e) =>
            {
                using (var client = new RedisClient(this.Host))
                {
                    client.RPush("mylist".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                    client.LPop("mylist".ToByte());
                    client.LPop("mylist".ToByte());
                    client.LPop("mylist".ToByte()); //now mylist should contains no entry
                    var result = client.BLPop("0".ToByte(), "mylist".ToByte()); //should block

                    var list = result.MultiBytesToList();
                    Assert.AreEqual(2, list.Count);
                    Assert.AreEqual("mylist", list[0]);
                    Assert.AreEqual("four", list[1]);
                }
            };
            pop.RunWorkerCompleted += (s, e) =>
            {
                resetEvent.Set();
            };

            pop.RunWorkerAsync();

            BackgroundWorker push = new BackgroundWorker();
            push.DoWork += (s, e) =>
            {
                using (var client = new RedisClient(this.Host))
                {
                    client.RPush("mylist".ToByte(), "four".ToByte());
                }
            };

            push.RunWorkerAsync();
            resetEvent.WaitOne();
        }

        [TestMethod]
        public void BRPopLPush_EntriesInList_ShouldPopThenPush()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                var result = client.BRPopLPush("mylist".ToByte(), "mynewlist".ToByte(), "0".ToByte());
                Assert.IsNotNull(result);
                Assert.AreEqual("three", result.BytesToString());
                var len1 = client.LLen("mylist".ToByte());
                var len2 = client.LLen("mynewlist".ToByte());
                Assert.AreEqual(2, len1);
                Assert.AreEqual(1, len2);
            }
        }

        [TestMethod]
        public void BRPopLPush_NoEntriesInList_ShouldBlock()
        {
            BackgroundWorker pop = new BackgroundWorker();
            var resetEvent = new ManualResetEvent(false);
            pop.DoWork += (s, e) =>
            {
                using (var client = new RedisClient(this.Host))
                {
                    client.RPush("mylist".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                    client.LPop("mylist".ToByte());
                    client.LPop("mylist".ToByte());
                    client.LPop("mylist".ToByte()); //now mylist should contains no entry
                    var result = client.BRPopLPush("mylist".ToByte(), "mynewlist".ToByte(), "0".ToByte()); ; //should block
                    Assert.IsNotNull(result);
                    Assert.AreEqual("four", result.BytesToString());
                }
            };
            pop.RunWorkerCompleted += (s, e) =>
            {
                resetEvent.Set();
            };

            pop.RunWorkerAsync();

            BackgroundWorker push = new BackgroundWorker();
            push.DoWork += (s, e) =>
            {
                using (var client = new RedisClient(this.Host))
                {
                    client.RPush("mylist".ToByte(), "four".ToByte());
                }
            };

            push.RunWorkerAsync();
            resetEvent.WaitOne();
        }

        [TestMethod]
        public void LPush_KeyNotExist_ShouldCreateNewListAndPushValue()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.LPush("mywordlist".ToByte(), "world".ToByte());
                Assert.AreEqual(1, result);
                var result2 = client.LRange("mywordlist".ToByte(), "0".ToByte(), "-1".ToByte());
                Assert.IsNotNull(result2);
                var split = result2.MultiBytesToString().Split(new char[] { '\r', '\n' }, System.StringSplitOptions.RemoveEmptyEntries);
                var list = new List<string>();
                list.AddRange(split);
                Assert.IsTrue(list.Contains("world"));
            }
        }

        [TestMethod]
        public void LPush_KeyNotExistPushMultipleValues_ShouldCreateNewListAndPushValues()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.LPush("mylist".ToByte(), "world".ToByte(), "hello".ToByte());
                Assert.AreEqual(2, result);
                var result2 = client.LRange("mylist".ToByte(), "0".ToByte(), "-1".ToByte());
                Assert.IsNotNull(result2);
                var split = result2.MultiBytesToString().Split(new char[] { '\r', '\n' });
                var list = new List<string>();
                list.AddRange(split);
                Assert.IsTrue(list.Contains("hello"));
                Assert.IsTrue(list.Contains("world"));
            }
        }

        [TestMethod]
        public void LPush_KeyExist_ShouldPushValueToTheHeadOfTheList()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.LPush("mylist".ToByte(), "world".ToByte()); //mylist should now exist
                Assert.AreEqual(1, result);
                client.LPush("mylist".ToByte(), "hello".ToByte());
                Assert.AreEqual(1, result);
                var result2 = client.LRange("mylist".ToByte(), "0".ToByte(), "-1".ToByte());
                Assert.IsNotNull(result2);
                var split = result2.MultiBytesToString().Split(new char[] { '\r', '\n' });
                var list = new List<string>();
                list.AddRange(split);
                Assert.IsTrue(list.Contains("world"));
            }
        }

        [TestMethod]
        public void LPush_KeyExistButWrongType_ShouldReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mykey", "hi");
                var result = client.LPush("mykey".ToByte(), "world".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void LPushX_KeyNotExist_ShouldIgnore()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.LPushx("mykey".ToByte(), "world".ToByte());
                Assert.AreEqual(0, result);
            }
        }

        [TestMethod]
        public void LPushX_KeyExist_ShouldPushValue()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.LPush("mylist".ToByte(), "world".ToByte()); //mykey now exist
                var result = client.LPushx("mylist".ToByte(), "hello".ToByte());
                Assert.IsNotNull(result);
                var result2 = client.LRange("mylist".ToByte(), "0".ToByte(), "-1".ToByte());
                Assert.IsNotNull(result2);
                var split = result2.MultiBytesToString().Split(new char[] { '\r', '\n' });
                var list = new List<string>();
                list.AddRange(split);
                Assert.IsTrue(list.Contains("hello"));
                Assert.IsTrue(list.Contains("world"));
            }
        }

        [TestMethod]
        public void LRange_KeyExist_ShouldReturnValuesInRange()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                var result = client.LRange("mylist".ToByte(), "0".ToByte(), "0".ToByte());
                var list = result.MultiBytesToList();
                Assert.IsTrue(list.Contains("one"));

                result = client.LRange("mylist".ToByte(), "-3".ToByte(), "2".ToByte());
                list = result.MultiBytesToList();
                Assert.IsTrue(list.Contains("one"));
                Assert.IsTrue(list.Contains("two"));
                Assert.IsTrue(list.Contains("three"));

                result = client.LRange("mylist".ToByte(), "-100".ToByte(), "100".ToByte());
                list = result.MultiBytesToList();
                Assert.IsTrue(list.Contains("one"));
                Assert.IsTrue(list.Contains("two"));
                Assert.IsTrue(list.Contains("three"));
            }
        }

        [TestMethod]
        public void LRange_KeyExistStartLargerThanEnd_ShouldReturnEmpty()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                var result = client.LRange("mylist".ToByte(), "5".ToByte(), "10".ToByte());
                var list = result.MultiBytesToList();
                Assert.AreEqual(0, list.Count);
            }
        }

        [TestMethod]
        public void LRange_KeyExistStopLargerThanStart_ShouldReturnEmpty()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                var result = client.LRange("mylist".ToByte(), "3".ToByte(), "0".ToByte());
                var list = result.MultiBytesToList();
                Assert.AreEqual(0, list.Count);
            }
        }

        [TestMethod]
        public void LRem_KeyExistCountLessThanZero_ShouldRemoveEleStartingTowardTail()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "hello".ToByte(), "hello".ToByte(), "foo".ToByte(), "hello".ToByte());
                var result = client.LRem("mylist".ToByte(), "-2".ToByte(), "hello".ToByte()); //remove the last 2 occurrence
                Assert.AreEqual(2, result);
                var tmp = client.LRange("mylist".ToByte(), "0".ToByte(), "-1".ToByte());
                var list = tmp.MultiBytesToList();
                Assert.AreEqual("hello", list[0]);
                Assert.AreEqual("foo", list[1]);
            }
        }

        [TestMethod]
        public void LRem_KeyExistCountGreaterThanZero_ShouldRemoveEleStartingTowardHead()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "hello".ToByte(), "hello".ToByte(), "foo".ToByte(), "hello".ToByte());
                var result = client.LRem("mylist".ToByte(), "2".ToByte(), "hello".ToByte()); //remove the first 2 occurrence
                Assert.AreEqual(2, result);
                var tmp = client.LRange("mylist".ToByte(), "0".ToByte(), "-1".ToByte());
                var list = tmp.MultiBytesToList();
                Assert.AreEqual("foo", list[0]);
                Assert.AreEqual("hello", list[1]);
            }
        }

        [TestMethod]
        public void LRem_KeyExistCountEqualsZero_ShouldRemoveAllOccurrences()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "hello".ToByte(), "hello".ToByte(), "foo".ToByte(), "hello".ToByte());
                var result = client.LRem("mylist".ToByte(), "0".ToByte(), "hello".ToByte()); //remove all occurrences
                Assert.AreEqual(3, result);
                var tmp = client.LRange("mylist".ToByte(), "0".ToByte(), "-1".ToByte());
                var list = tmp.MultiBytesToList();
                Assert.AreEqual("foo", list[0]);
            }
        }

        [TestMethod]
        public void LSet_ValidIndex_ShouldSetValue()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                client.LSet("mylist".ToByte(), "0".ToByte(), "four".ToByte());
                client.LSet("mylist".ToByte(), "-2".ToByte(), "five".ToByte());
                var tmp = client.LRange("mylist".ToByte(), "0".ToByte(), "-1".ToByte());
                var list = tmp.MultiBytesToList();
                Assert.AreEqual("four", list[0]);
                Assert.AreEqual("five", list[1]);
                Assert.AreEqual("three", list[2]);
            }
        }

        [TestMethod]
        public void LSet_OutOfRangeIndex_ShouldReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                var result = client.LSet("mylist".ToByte(), "5".ToByte(), "four".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void LTrim_ValidParams_ShouldTrimElements()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                client.LTrim("mylist".ToByte(), "1".ToByte(), "-1".ToByte());
                var tmp = client.LRange("mylist".ToByte(), "0".ToByte(), "-1".ToByte());
                var list = tmp.MultiBytesToList();
                Assert.AreEqual("two", list[0]);
                Assert.AreEqual("three", list[1]);
            }
        }

        [TestMethod]
        public void LTrim_KeyExistStartLargerThanEnd_ShouldReturnEmpty()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                client.LTrim("mylist".ToByte(), "5".ToByte(), "10".ToByte());
                var tmp = client.LRange("mylist".ToByte(), "0".ToByte(), "-1".ToByte());
                var list = tmp.MultiBytesToList();
                Assert.AreEqual(0, list.Count);
            }
        }

        [TestMethod]
        public void LTrim_KeyExistStopLargerThanStart_ShouldReturnEmpty()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                client.LTrim("mylist".ToByte(), "5".ToByte(), "0".ToByte());
                var result = client.LRange("mylist".ToByte(), "0".ToByte(), "-1".ToByte());
                var list = result.MultiBytesToList();
                Assert.AreEqual(0, list.Count);
            }
        }

        [TestMethod]
        public void RPop_ValidKey_ShouldPopValue()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                var popResult = client.RPop("mylist".ToByte());
                var result = client.LRange("mylist".ToByte(), "0".ToByte(), "-1".ToByte());
                var list = result.MultiBytesToList();
                Assert.AreEqual(2, list.Count);
                Assert.AreEqual("one", list[0]);
                Assert.AreEqual("two", list[1]);
                Assert.AreEqual("three", popResult.BytesToString());
            }
        }

        [TestMethod]
        public void RPop_InValidKey_ShouldReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                var popResult = client.RPop("mylist".ToByte());
                Assert.IsNull(popResult);
            }
        }

        [TestMethod]
        public void RPopLPush_ValidParams_ShouldPopThenPush()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                var popResult = client.RPopLPush("mylist".ToByte(), "mynewlist".ToByte());
                var result = client.LRange("mylist".ToByte(), "0".ToByte(), "-1".ToByte());
                var list = result.MultiBytesToList();
                Assert.AreEqual(2, list.Count);
                Assert.AreEqual("one", list[0]);
                Assert.AreEqual("two", list[1]);
                Assert.AreEqual("three", popResult.BytesToString());

                var result2 = client.LRange("mynewlist".ToByte(), "0".ToByte(), "-1".ToByte());
                var list2 = result2.MultiBytesToList();
                Assert.AreEqual(1, list2.Count);
                Assert.AreEqual("three", list2[0]);
            }
        }

        [TestMethod]
        public void RPopLPush_NoSource_ShouldBeNoOperation()
        {
            using (var client = new RedisClient(this.Host))
            {
                var popResult = client.RPopLPush("mylist".ToByte(), "mynewlist".ToByte());
                Assert.IsNull(popResult);
            }
        }

        [TestMethod]
        public void RPopLPush_SameSourceDestination_ShouldPopThenPushToHead()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                var popResult = client.RPopLPush("mylist".ToByte(), "mylist".ToByte());
                var result = client.LRange("mylist".ToByte(), "0".ToByte(), "-1".ToByte());
                var list = result.MultiBytesToList();
                Assert.AreEqual(3, list.Count);
                Assert.AreEqual("three", list[0]);
                Assert.AreEqual("one", list[1]);
                Assert.AreEqual("two", list[2]);
                Assert.AreEqual("three", popResult.BytesToString());
            }
        }

        [TestMethod]
        public void RPush_ValidParams_ShouldPushValuesToTail()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                var result = client.LRange("mylist".ToByte(), "0".ToByte(), "-1".ToByte());
                var list = result.MultiBytesToList();
                Assert.AreEqual(3, list.Count);
                Assert.AreEqual("one", list[0]);
                Assert.AreEqual("two", list[1]);
                Assert.AreEqual("three", list[2]);
            }
        }

        [TestMethod]
        public void RPush_KeyExists_ShouldAppendValuesToTail()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                client.RPush("mylist".ToByte(), "four".ToByte(), "five".ToByte());
                var result = client.LRange("mylist".ToByte(), "0".ToByte(), "-1".ToByte());
                var list = result.MultiBytesToList();
                Assert.AreEqual(5, list.Count);
                Assert.AreEqual("one", list[0]);
                Assert.AreEqual("two", list[1]);
                Assert.AreEqual("three", list[2]);
                Assert.AreEqual("four", list[3]);
                Assert.AreEqual("five", list[4]);
            }
        }

        [TestMethod]
        public void RPush_WrongDataType_ShouldReturnNil()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.Set("mylist", "val");
                var result = client.RPush("mylist".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                Assert.IsNull(result);
            }
        }

        [TestMethod]
        public void RPushx_KeyExists_ShouldAppendValuesToTail()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.RPush("mylist".ToByte(), "one".ToByte(), "two".ToByte(), "three".ToByte());
                client.RPushX("mylist".ToByte(), "four".ToByte());
                var result = client.LRange("mylist".ToByte(), "0".ToByte(), "-1".ToByte());
                var list = result.MultiBytesToList();
                Assert.AreEqual(4, list.Count);
                Assert.AreEqual("one", list[0]);
                Assert.AreEqual("two", list[1]);
                Assert.AreEqual("three", list[2]);
                Assert.AreEqual("four", list[3]);
            }
        }

        [TestMethod]
        public void RPushx_KeyNotExists_ShouldReturnZero()
        {
            using (var client = new RedisClient(this.Host))
            {
                var result = client.RPushX("mylist".ToByte(), "one".ToByte());
                Assert.AreEqual(0, result);
            }
        }
    }
}
