using System;
using System.Collections;
using System.Text;

namespace Batbeetle
{
    public class RedisClient : BaseClient
    {
        public Strings Strings { get; set; }
        public Hashes Hashes { get; set; }
        public Connection Connection { get; set; }

        public RedisClient(string host, int port = 6379)
            : base(host, port)
        {
            this.Strings = new Strings(this);
            this.Connection = new Connection(this);
            this.Hashes = new Hashes(this);
        }
    }

    public class Keys
    {
    }

    public class Strings
    {
        private RedisClient client;
        public Strings(RedisClient client)
        {
            this.client = client;
        }

        public int Append(string key, string value)
        {
            var result = this.client.Append(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value));
            return result;
        }

        public int BitCount(string key, int? start, int? end)
        {
            var result = this.client.BitCount(
                Encoding.UTF8.GetBytes(key),
                start.HasValue ? Encoding.UTF8.GetBytes(start.ToString()) : null, 
                end.HasValue ? Encoding.UTF8.GetBytes(end.ToString()) : null);
            return result;
        }

        public int? Decr(string key)
        {
            var result = this.client.Decr(Encoding.UTF8.GetBytes(key));
            return result;
        }

        public int? DecrBy(string key, int decrement)
        {
            var result = this.client.DecrBy(
                Encoding.UTF8.GetBytes(key),
                Encoding.UTF8.GetBytes(decrement.ToString()));
            return result;
        }

        /// <summary>
        /// Get the stored value in raw bytes.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public byte[] GetRaw(string key)
        {
            var bytes = this.client.Get(Encoding.UTF8.GetBytes(key));
            return bytes;
        }

        public string Get(string key)
        {
            var bytes = this.GetRaw(key);
            if (bytes == null) return null;
            return Encoding.UTF8.GetString(bytes);
        }

        public int GetBit(string key, int offset)
        {
            var result = this.client.GetBit(
                Encoding.UTF8.GetBytes(key),
                Encoding.UTF8.GetBytes(offset.ToString()));
            return result;
        }

        public string GetRange(string key, int start, int end)
        {
            var bytes = this.client.GetRange(
                Encoding.UTF8.GetBytes(key),
                Encoding.UTF8.GetBytes(start.ToString()),
                Encoding.UTF8.GetBytes(end.ToString()));
            if (bytes == null) return null;
            return Encoding.UTF8.GetString(bytes);
        }

        /// <summary>
        /// Atomically sets key to value and returns the old value stored at key. Returns an error when key exists but does not hold a string value.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public string GetSet(string key, string value)
        {
            var bytes = this.client.GetSet(
                Encoding.UTF8.GetBytes(key),
                Encoding.UTF8.GetBytes(value));
            if (bytes == null) return null;
            return Encoding.UTF8.GetString(bytes);
        }

        public int? Incr(string key)
        {
            var result = this.client.Incr(Encoding.UTF8.GetBytes(key));
            return result;
        }

        public int? IncrBy(string key, int increment)
        {
            var result = this.client.IncrBy(
                Encoding.UTF8.GetBytes(key),
                Encoding.UTF8.GetBytes(increment.ToString()));
            return result;
        }

        public bool Set(string key, string value)
        {
            var result = this.client.Set(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value));
            if (result == null) return false;
            return result.Equals("OK");
        }

        public double? IncrByFloat(string key, double increment)
        {
            var result = this.client.IncrByFloat(
                Encoding.UTF8.GetBytes(key),
                Encoding.UTF8.GetBytes(increment.ToString()));
            if (result == null) return null;
            return double.Parse(result.BytesToString());
        }

        public bool Set(string key, string value, int expireInSec)
        {
            var result = this.client.Set(
                Encoding.UTF8.GetBytes(key),
                Encoding.UTF8.GetBytes(value),
                Encoding.UTF8.GetBytes(expireInSec.ToString()),
                null,
                false,
                false);
            if (result == null) return false;
            return result.Equals("OK");
        }

        public bool Set(
            string key,
            string value,
            int? expireInSec,
            int? expireInMillisec,
            bool setIfNotExist,
            bool setIfExist)
        {
            var result = this.client.Set(
                Encoding.UTF8.GetBytes(key),
                Encoding.UTF8.GetBytes(value),
                expireInSec.HasValue ? Encoding.UTF8.GetBytes(expireInSec.Value.ToString()) : null,
                expireInMillisec.HasValue ? Encoding.UTF8.GetBytes(expireInMillisec.ToString()) : null,
                setIfNotExist,
                setIfExist);
            if (result == null) return false;
            return result.Equals("OK");
        }
    }

    public class Hashes
    {
        private RedisClient client;
        public Hashes(RedisClient client)
        {
            this.client = client;
        }

        public void HMSet(string key, Hashtable hash)
        {
            var len = hash.Count;
            var keys = new byte[len][];
            var vals = new byte[len][];
            int i = 0;
            foreach (DictionaryEntry de in hash)
            {
                keys[i] = de.Key.ToByte();
                vals[i] = de.Value.ToByte();
                i++;
            }

            this.client.HMSet(key.ToByte(), keys, vals);
        }

        public Hashtable HMGetAll(string key)
        {
            var data = this.client.HMGetAll(key.ToByte());
            var str = data.MultiBytesToString();
            var sc = str.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            Hashtable tbl = new Hashtable();
            for (int i = 0; i < sc.Length; i += 2)
                tbl[sc[i]] = sc[i + 1];
            return tbl;
        }
    }

    public class Lists
    {
    }

    public class Sets
    {
    }

    public class SortedSet
    {
    }

    public class Connection
    {
        private RedisClient client;
        public Connection(RedisClient client)
        {
            this.client = client;
        }

        public bool Ping()
        {
            var result = this.client.Ping();
            if (result == null) return false;
            return result.Equals("PONG");
        }
    }
}
