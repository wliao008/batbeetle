using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Batbeetle
{
    public abstract class BaseClient : IDisposable
    {
        public readonly string Host = "127.0.0.1";
        public readonly int Port = 6379;
        public readonly byte[] Crlf = new byte[] { 0x0D, 0x0A };
        public Socket Socket { get; set; }
        private const int BUFFERSIZE = 8192;
        private byte[] buf = new byte[512];
        private BufferedStream bs;
        private bool disposed;

        public BaseClient()
            : this("127.0.0.1")
        {
        }

        public BaseClient(string host, int port = 6379)
        {
            this.Host = host;
            this.Port = port;
        }

        public void Connect()
        {
            this.Socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            this.Socket.NoDelay = true;
            IPAddress ip = IPAddress.Parse(this.Host);
            IPEndPoint ep = new IPEndPoint(ip, this.Port);
            this.Socket.Connect(ep);

            if (!this.Socket.Connected)
                Dispose();

            bs = new BufferedStream(new NetworkStream(this.Socket), BUFFERSIZE);
        }

        #region Keys
        public int Del(params byte[][] keys)
        {
            var cmd = new RedisCommand(Commands.Del);
            foreach(var key in keys)
            cmd.ArgList.Add(key);
                this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public byte[] Dump(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Dump);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public int Exists(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Exists);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int Expire(byte[] key, byte[] seconds)
        {
            var cmd = new RedisCommand(Commands.Expire);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(seconds);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int ExpireAt(byte[] key, byte[] timestamp)
        {
            var cmd = new RedisCommand(Commands.Expireat);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(timestamp);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public byte[][] Keys(byte[] pattern)
        {
            var cmd = new RedisCommand(Commands.Keys);
            cmd.ArgList.Add(pattern);
            this.SendCommand(cmd);
            return this.ReadMultibulkResponse();
        }

        public string Migrate(
            byte[] host, 
            byte[] port, 
            byte[] key,
            byte[] destinationDb,
            byte[] timeout,
            bool copy,
            bool replace)
        {
            var cmd = new RedisCommand(Commands.Migrate);
            cmd.ArgList.Add(host);
            cmd.ArgList.Add(port);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(destinationDb);
            cmd.ArgList.Add(timeout);
            if (copy)
                cmd.ArgList.Add(Commands.Copy);
            if (replace)
                cmd.ArgList.Add(Commands.Replace);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public int Move(byte[] key, byte[] db)
        {
            var cmd = new RedisCommand(Commands.Move);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(db);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        /// <summary>
        /// For RefCount, IdleTime
        /// </summary>
        public int? Object(byte[] subcommand, byte[] key)
        {
            var cmd = new RedisCommand(Commands.Object);
            cmd.ArgList.Add(subcommand);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            var result = this.ReadIntResponse();
            if (result == -1)
                return null;
            return result;
        }

        public byte[] ObjectEncoding(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Object);
            cmd.ArgList.Add(Commands.Encoding);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public int Persist(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Persist);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int PExpire(byte[] key, byte[] seconds)
        {
            var cmd = new RedisCommand(Commands.Pexpire);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(seconds);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int PExpireAt(byte[] key, byte[] millisecondTimestamp)
        {
            var cmd = new RedisCommand(Commands.Pexpireat);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(millisecondTimestamp);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int PTtl(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Pttl);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public byte[] RandomKey()
        {
            var cmd = new RedisCommand(Commands.Randomkey);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public string Rename(byte[] key, byte[] newkey)
        {
            var cmd = new RedisCommand(Commands.Rename);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(newkey);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public int? RenameNx(byte[] key, byte[] newkey)
        {
            var cmd = new RedisCommand(Commands.Renamenx);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(newkey);
            this.SendCommand(cmd);
            var result = this.ReadIntResponse();
            return result.HasValue ? result.Value : 0;
        }

        public string Restore(byte[] key, byte[] ttl, byte[] serializedValue)
        {
            var cmd = new RedisCommand(Commands.Restore);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(ttl);
            cmd.ArgList.Add(serializedValue);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public int Ttl(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Ttl);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public string Type(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Type);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }
        #endregion

        #region Strings
        protected internal int Append(byte[] key, byte[] value)
        {
            var cmd = new RedisCommand(Commands.Append);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(value);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        protected internal int BitCount(byte[] key, byte[] start, byte[] end)
        {
            var cmd = new RedisCommand(Commands.Bitcount);
            cmd.ArgList.Add(key);
            if (start != null)
                cmd.ArgList.Add(start);
            if (end != null)
                cmd.ArgList.Add(end);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        protected internal int? Decr(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Decr);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse();
        }

        protected internal int? DecrBy(byte[] key, byte[] decrement)
        {
            var cmd = new RedisCommand(Commands.Decrby);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(decrement);
            this.SendCommand(cmd);
            return this.ReadIntResponse();
        }

        protected internal byte[] Get(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Get);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        protected internal int GetBit(byte[] key, byte[] offset)
        {
            var cmd = new RedisCommand(Commands.Getbit);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(offset);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        protected internal byte[] GetRange(byte[] key, byte[] start, byte[] end)
        {
            var cmd = new RedisCommand(Commands.Getrange);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(start);
            cmd.ArgList.Add(end);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public byte[] GetSet(byte[] key, byte[] value)
        {
            var cmd = new RedisCommand(Commands.Getset);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(value);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public int? Incr(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Incr);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse();
        }

        public int? IncrBy(byte[] key, byte[] increment)
        {
            var cmd = new RedisCommand(Commands.Incrby);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(increment);
            this.SendCommand(cmd);
            return this.ReadIntResponse();
        }

        public byte[] IncrByFloat(byte[] key, byte[] increment)
        {
            var cmd = new RedisCommand(Commands.Incrbyfloat);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(increment);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public byte[][] MGet(params byte[][] keys)
        {
            var cmd = new RedisCommand(Commands.Mget);
            foreach(var key in keys)
                cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadMultibulkResponse();
        }

        public string MSet(byte[][] keys, byte[][] values)
        {
            if (keys.Length != values.Length)
                return null;

            var cmd = new RedisCommand(Commands.Mset);
            for (int i = 0; i < keys.Length; ++i)
            {
                cmd.ArgList.Add(keys[i]);
                cmd.ArgList.Add(values[i]);
            }
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public int? MSetNx(byte[][] keys, byte[][] values)
        {
            if (keys.Length != values.Length)
                return null;

            var cmd = new RedisCommand(Commands.Msetnx);
            for (int i = 0; i < keys.Length; ++i)
            {
                cmd.ArgList.Add(keys[i]);
                cmd.ArgList.Add(values[i]);
            }
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        protected internal string Set(byte[] key, byte[] value)
        {
            return this.Set(key, value, null, null, false, false);
        }

        protected internal string Set(byte[] key, byte[] value, byte[] ex, byte[] px, bool nx, bool xx)
        {
            var cmd = new RedisCommand(Commands.Set);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(value);
            if (ex != null)
            {
                cmd.ArgList.Add(Commands.Ex);
                cmd.ArgList.Add(ex);
            }

            if (px != null)
            {
                cmd.ArgList.Add(Commands.Px);
                cmd.ArgList.Add(px);
            }

            if (nx)
                cmd.ArgList.Add(Commands.Nx);

            if (xx)
                cmd.ArgList.Add(Commands.Xx);

            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public int SetBit(byte[] key, byte[] offset, byte[] value)
        {
            var cmd = new RedisCommand(Commands.Setbit);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(offset);
            cmd.ArgList.Add(value);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int SetRange(byte[] key, byte[] offset, byte[] value)
        {
            var cmd = new RedisCommand(Commands.Setrange);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(offset);
            cmd.ArgList.Add(value);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int StrLen(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Strlen);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }
        #endregion

        #region Hashes
        public int HDel(byte[] key, params byte[][] fields)
        {
            var cmd = new RedisCommand(Commands.Hdel);
            cmd.ArgList.Add(key);
            foreach (var field in fields)
                cmd.ArgList.Add(field);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int HExists(byte[] key, byte[] field)
        {
            var cmd = new RedisCommand(Commands.Hexists);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(field);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public byte[] HGet(byte[] key, byte[] field)
        {
            var cmd = new RedisCommand(Commands.Hget);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(field);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        protected internal byte[][] HMGetAll(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Hgetall);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            var resp = this.ReadMultibulkResponse();
            return resp;
        }

        public int? HIncrBy(byte[] key, byte[] field, byte[] increment)
        {
            var cmd = new RedisCommand(Commands.Hincrby);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(field);
            cmd.ArgList.Add(increment);
            this.SendCommand(cmd);
            return this.ReadIntResponse();
        }

        public byte[] HIncrByFloat(byte[] key, byte[] field, byte[] increment)
        {
            var cmd = new RedisCommand(Commands.Hincrbyfloat);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(field);
            cmd.ArgList.Add(increment);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public byte[][] HKeys(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Hkeys);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            var resp = this.ReadMultibulkResponse();
            return resp;
        }

        public int HLen(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Hlen);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public byte[][] HMGet(byte[] key, params byte[][] fields)
        {
            var cmd = new RedisCommand(Commands.Hmget);
            cmd.ArgList.Add(key);
            foreach(var field in fields)
                cmd.ArgList.Add(field);
            this.SendCommand(cmd);
            var resp = this.ReadMultibulkResponse();
            return resp;
        }

        protected internal string HMSet(byte[] key, byte[][] fieldKeys, byte[][] fieldValues)
        {
            var cmd = new RedisCommand(Commands.Hmset);
            cmd.ArgList.Add(key);
            for (int i = 0; i < fieldKeys.Length; ++i)
            {
                cmd.ArgList.Add(fieldKeys[i]);
                cmd.ArgList.Add(fieldValues[i]);
            }
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public int HSet(byte[] key, byte[] field, byte[] value)
        {
            var cmd = new RedisCommand(Commands.Hset);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(field);
            cmd.ArgList.Add(value);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int HSetNx(byte[] key, byte[] field, byte[] value)
        {
            var cmd = new RedisCommand(Commands.Hsetnx);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(field);
            cmd.ArgList.Add(value);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public byte[][] KVals(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Hvals);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            var resp = this.ReadMultibulkResponse();
            return resp;
        }
        #endregion

        #region Lists
        public byte[][] BLPop(byte[] timeout, params byte[][] keys)
        {
            var cmd = new RedisCommand(Commands.Blpop);
            foreach(var key in keys)
                cmd.ArgList.Add(key);
            cmd.ArgList.Add(timeout);
            this.SendCommand(cmd);
            return this.ReadMultibulkResponse();
        }

        public byte[][] BRPop(byte[] timeout, params byte[][] keys)
        {
            var cmd = new RedisCommand(Commands.Brpop);
            foreach (var key in keys)
                cmd.ArgList.Add(key);
            cmd.ArgList.Add(timeout);
            this.SendCommand(cmd);
            return this.ReadMultibulkResponse();
        }

        public byte[] BRPopLPush(byte[] source, byte[] destination, byte[] timeout)
        {
            var cmd = new RedisCommand(Commands.Brpoplpush);
            cmd.ArgList.Add(source);
            cmd.ArgList.Add(destination);
            cmd.ArgList.Add(timeout);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public byte[] LIndex(byte[] key, byte[] index)
        {
            var cmd = new RedisCommand(Commands.Brpoplpush);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(index);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public int LInsert(byte[] key, byte[] beforeafter, byte[] pivot, byte[] value)
        {
            var cmd = new RedisCommand(Commands.Linsert);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(beforeafter);
            cmd.ArgList.Add(pivot);
            cmd.ArgList.Add(value);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int LLen(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Llen);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public byte[] LPop(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Lpop);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public int? LPush(byte[] key, params byte[][] values)
        {
            var cmd = new RedisCommand(Commands.Lpush);
            cmd.ArgList.Add(key);
            foreach (var value in values)
                cmd.ArgList.Add(value);
            this.SendCommand(cmd);
            return this.ReadIntResponse();
        }

        public int LPushx(byte[] key, byte[] value)
        {
            var cmd = new RedisCommand(Commands.Lpushx);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(value);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public byte[][] LRange(byte[] key, byte[] start, byte[] stop)
        {
            var cmd = new RedisCommand(Commands.Lrange);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(start);
            cmd.ArgList.Add(stop);
            this.SendCommand(cmd);
            return this.ReadMultibulkResponse();
        }

        public int LRem(byte[] key, byte[] count, byte[] value)
        {
            var cmd = new RedisCommand(Commands.Lrem);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(count);
            cmd.ArgList.Add(value);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public string LSet(byte[] key, byte[] index, byte[] value)
        {
            var cmd = new RedisCommand(Commands.Lset);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(index);
            cmd.ArgList.Add(value);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public string LTrim(byte[] key, byte[] start, byte[] stop)
        {
            var cmd = new RedisCommand(Commands.Ltrim);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(start);
            cmd.ArgList.Add(stop);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public byte[] RPop(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Rpop);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public byte[] RPopLPush(byte[] source, byte[] destination)
        {
            var cmd = new RedisCommand(Commands.Rpoplpush);
            cmd.ArgList.Add(source);
            cmd.ArgList.Add(destination);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public int? RPush(byte[] key, params byte[][] values)
        {
            var cmd = new RedisCommand(Commands.Rpush);
            cmd.ArgList.Add(key);
            foreach(var value in values)
                cmd.ArgList.Add(value);
            this.SendCommand(cmd);
            return this.ReadIntResponse();
        }

        public int RPushX(byte[] key, byte[] value)
        {
            var cmd = new RedisCommand(Commands.Rpushx);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(value);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        #endregion

        #region Sets
        public int SAdd(byte[] key, params byte[][] members)
        {
            var cmd = new RedisCommand(Commands.Sadd);
            cmd.ArgList.Add(key);
            foreach (var member in members)
                cmd.ArgList.Add(member);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int SCard(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Scard);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public byte[][] SDiff(params byte[][] keys)
        {
            var cmd = new RedisCommand(Commands.Sdiff);
            foreach (var key in keys)
                cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadMultibulkResponse();
        }

        public int SDiffStore(byte[] destination, params byte[][] keys)
        {
            var cmd = new RedisCommand(Commands.Sdiffstore);
            cmd.ArgList.Add(destination);
            foreach(var key in keys)
                cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public byte[][] SInter(params byte[][] keys)
        {
            var cmd = new RedisCommand(Commands.Sinter);
            foreach (var key in keys)
                cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadMultibulkResponse();
        }

        public int SInterStore(byte[] destination, params byte[][] keys)
        {
            var cmd = new RedisCommand(Commands.Sinterstore);
            cmd.ArgList.Add(destination);
            foreach (var key in keys)
                cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int SIsMember(byte[] key, byte[] member)
        {
            var cmd = new RedisCommand(Commands.Sismember);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(member);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public byte[][] SMembers(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Smembers);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadMultibulkResponse();
        }

        public int? SMove(byte[] source, byte[] destination, byte[] member)
        {
            var cmd = new RedisCommand(Commands.Smove);
            cmd.ArgList.Add(source);
            cmd.ArgList.Add(destination);
            cmd.ArgList.Add(member);
            this.SendCommand(cmd);
            return this.ReadIntResponse();
        }

        public byte[] SPop(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Spop);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public byte[][] SRandMember(byte[] key, byte[] count)
        {
            var cmd = new RedisCommand(Commands.Srandmember);
            cmd.ArgList.Add(key);
            if (count != null)
                cmd.ArgList.Add(count);
            this.SendCommand(cmd);
            if (count != null)
                return this.ReadMultibulkResponse();
            else
            {
                var result = this.ReadBulkResponse();
                if (result == null)
                    return null;
                var bytes = new byte[1][];
                bytes[0] = result;
                return bytes;
            }
        }

        public int? SRem(byte[] key, params byte[][] members)
        {
            var cmd = new RedisCommand(Commands.Srem);
            cmd.ArgList.Add(key);
            foreach(var member in members)
                cmd.ArgList.Add(member);
            this.SendCommand(cmd);
            return this.ReadIntResponse();
        }

        public byte[][] SUnion(params byte[][] keys)
        {
            var cmd = new RedisCommand(Commands.Sunion);
            foreach(var key in keys)
                cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadMultibulkResponse();
        }

        public int SUnionStore(byte[] destination, params byte[][] keys)
        {
            var cmd = new RedisCommand(Commands.Sunionstore);
            cmd.ArgList.Add(destination);
            foreach (var key in keys)
                cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }
        #endregion

        #region Sorted Sets
        public int? ZAdd(byte[] key, byte[][] scores, byte[][] members)
        {
            if (scores.Length != members.Length)
                return null;

            var cmd = new RedisCommand(Commands.Zadd);
            cmd.ArgList.Add(key);
            for (int i = 0; i < scores.Length; ++i)
            {
                cmd.ArgList.Add(scores[i]);
                cmd.ArgList.Add(members[i]);
            }
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int ZCard(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Zcard);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int ZCount(byte[] key, byte[] min, byte[] max)
        {
            var cmd = new RedisCommand(Commands.Zcount);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(min);
            cmd.ArgList.Add(max);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public byte[] ZIncrBy(byte[] key, byte[] increment, byte[] member)
        {
            var cmd = new RedisCommand(Commands.Zincrby);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(increment);
            cmd.ArgList.Add(member);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public byte[][] ZRange(byte[] key, byte[] start, byte[] stop, bool withScores = false)
        {
            var cmd = new RedisCommand(Commands.Zrange);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(start);
            cmd.ArgList.Add(stop);
            if (withScores)
                cmd.ArgList.Add("WITHSCORES".ToByte());
            this.SendCommand(cmd);
            return this.ReadMultibulkResponse();
        }

        public byte[][] ZRangeByScore(byte[] key, byte[] min, byte[] max, bool withScores = false, bool limit = false, byte[] offset = null, byte[] count = null)
        {
            var cmd = new RedisCommand(Commands.Zrangebyscore);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(min);
            cmd.ArgList.Add(max);
            if (withScores)
                cmd.ArgList.Add("WITHSCORES".ToByte());
            if (limit)
            {
                cmd.ArgList.Add("LIMIT".ToByte());
                cmd.ArgList.Add(offset);
                cmd.ArgList.Add(count);
            }
            this.SendCommand(cmd);
            return this.ReadMultibulkResponse();
        }

        public int? ZRank(byte[] key, byte[] member)
        {
            var cmd = new RedisCommand(Commands.Zrank);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(member);
            this.SendCommand(cmd);
            var result = this.ReadIntResponse();
            if (result == -1)
                return null;
            return result;
        }

        public int? ZRem(byte[] key, params byte[][] members)
        {
            var cmd = new RedisCommand(Commands.Zrem);
            cmd.ArgList.Add(key);
            foreach(var member in members)
                cmd.ArgList.Add(member);
            this.SendCommand(cmd);
            return this.ReadIntResponse();;
        }

        public int? ZRemRangeByRank(byte[] key, byte[] start, byte[] stop)
        {
            var cmd = new RedisCommand(Commands.Zremrangebyrank);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(start);
            cmd.ArgList.Add(stop);
            this.SendCommand(cmd);
            return this.ReadIntResponse(); ;
        }

        public int? ZRemRangeByScore(byte[] key, byte[] min, byte[] max)
        {
            var cmd = new RedisCommand(Commands.Zremrangebyscore);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(min);
            cmd.ArgList.Add(max);
            this.SendCommand(cmd);
            return this.ReadIntResponse(); ;
        }

        public byte[][] ZRevRange(byte[] key, byte[] start, byte[] stop, bool withScores = false)
        {
            var cmd = new RedisCommand(Commands.Zrevrange);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(start);
            cmd.ArgList.Add(stop);
            if (withScores)
                cmd.ArgList.Add("WITHSCORES".ToByte());
            this.SendCommand(cmd);
            return this.ReadMultibulkResponse();
        }

        public byte[][] ZRevRangeByScore(byte[] key, byte[] max, byte[] min, bool withScores = false, bool limit = false, byte[] offset = null, byte[] count = null)
        {
            var cmd = new RedisCommand(Commands.Zrevrangebyscore);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(max);
            cmd.ArgList.Add(min);
            if (withScores)
                cmd.ArgList.Add("WITHSCORES".ToByte());
            if (limit)
            {
                cmd.ArgList.Add("LIMIT".ToByte());
                cmd.ArgList.Add(offset);
                cmd.ArgList.Add(count);
            }
            this.SendCommand(cmd);
            return this.ReadMultibulkResponse();
        }

        public int? ZRevRank(byte[] key, byte[] member)
        {
            var cmd = new RedisCommand(Commands.Zrevrank);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(member);
            this.SendCommand(cmd);
            var result = this.ReadIntResponse();
            if (result == -1)
                return null;
            return result;
        }

        public byte[] ZScore(byte[] key, byte[] member)
        {
            var cmd = new RedisCommand(Commands.Zscore);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(member);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public int? ZUnionStore(
            byte[] destination, 
            byte[] numkeys,
            byte[][] keys,
            bool getWeights = false,
            byte[][] weights = null,
            bool getAggregate = false,
            byte[] aggregateType = null)
        {
            var cmd = new RedisCommand(Commands.Zunionstore);
            cmd.ArgList.Add(destination);
            cmd.ArgList.Add(numkeys);
            foreach (var key in keys)
                cmd.ArgList.Add(key);
            if (getWeights)
            {
                cmd.ArgList.Add("WEIGHTS".ToByte());
                foreach (var weight in weights)
                    cmd.ArgList.Add(weight);
            }
            if (getAggregate)
            {
                cmd.ArgList.Add("AGGREGATE".ToByte());
                if (aggregateType == null)
                    aggregateType = "SUM".ToByte();
                cmd.ArgList.Add(aggregateType);
            }
            this.SendCommand(cmd);
            var result = this.ReadIntResponse();
            if (result == -1)
                return null;
            return result;
        }
        #endregion

        #region Pub/Sub
        public byte[][] PSubscribe(params byte[][] patterns)
        {
            var cmd = new RedisCommand(Commands.Psubscribe);
            foreach (var pattern in patterns)
                cmd.ArgList.Add(pattern);
            this.SendCommand(cmd);
            return this.ReadMultibulkResponse();
        }

        public int Publish(byte[] channel, byte[] message)
        {
            var cmd = new RedisCommand(Commands.Publish);
            cmd.ArgList.Add(channel);
            cmd.ArgList.Add(message);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public byte[][] Subscribe(params byte[][] channels)
        {
            var cmd = new RedisCommand(Commands.Subscribe);
            foreach (var channel in channels)
                cmd.ArgList.Add(channel);
            this.SendCommand(cmd);
            return this.ReadMultibulkResponse();
        }
        #endregion

        #region Transactions
        public string Multi()
        {
            var cmd = new RedisCommand(Commands.Multi);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public byte[][] Exec()
        {
            var cmd = new RedisCommand(Commands.Exec);
            this.SendCommand(cmd);
            return this.ReadMultibulkResponse();
        }

        public string Discard()
        {
            var cmd = new RedisCommand(Commands.Discard);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public string Watch(params byte[][] keys)
        {
            var cmd = new RedisCommand(Commands.Watch);
            foreach (var key in keys)
                cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public string Unwatch()
        {
            var cmd = new RedisCommand(Commands.Unwatch);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        #endregion

        #region Connection
        public string Auth(byte[] password)
        {
            var cmd = new RedisCommand(Commands.Auth);
            cmd.ArgList.Add(password);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public byte[] Echo(byte[] message)
        {
            var cmd = new RedisCommand(Commands.Echo);
            cmd.ArgList.Add(message);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        protected internal string Ping()
        {
            var cmd = new RedisCommand(Commands.Ping);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public string Quit()
        {
            var cmd = new RedisCommand(Commands.Quit);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public string Select(byte[] index)
        {
            var cmd = new RedisCommand(Commands.Select);
            cmd.ArgList.Add(index);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }
        #endregion

        #region Server
        public string[] Info()
        {
            var cmd = new RedisCommand(Commands.Info);
            this.SendCommand(cmd);
            var resp = this.ReadBulkResponse();
            var str = Encoding.UTF8.GetString(resp);
            var data = str.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            return data;
        }

        public string FlushAll()
        {
            var cmd = new RedisCommand(Commands.Flushall);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }
        #endregion

        #region Send
        protected internal void SendCommand(RedisCommand cmd)
        {
            if (this.Socket == null)
                Connect();

            var bytes = cmd.ToBytes();
            this.Socket.Send(bytes);
        }
        #endregion

        #region Parsing response
        private string ReadLine()
        {
            if (this.Socket == null)
                return null;

            try
            {
                var sb = new StringBuilder();
                int c;
                while ((c = bs.ReadByte()) != -1)
                {
                    if (c == '\r') continue;
                    if (c == '\n') break;
                    sb.Append((char)c);
                }
                return sb.ToString();
            }
            catch
            {
                return null;
            }
        }

        public bool IsConnected()
        {
            bool part1 = this.Socket.Poll(1000, SelectMode.SelectRead);
            bool part2 = (this.Socket.Available == 0);
            if (part1 & part2)
                return false;
            else
                return true;
        }

        public int Available()
        {
            return this.Socket.Available;
        }

        private string ReadStringResponse()
        {
            var str = this.ReadLine();
            if (string.IsNullOrEmpty(str) || str[0] == '-')
            {
                HandlError(str);
                return null;
            }

            return str.Substring(1);
        }

        private int? ReadIntResponse()
        {
            var resp = this.ReadStringResponse();
            int ret = 0;
            if(int.TryParse(resp, out ret))
                return ret;
            return null;
        }

        private byte[] ReadBulkResponse()
        {
            var str = this.ReadLine();
            if (str == null || str == "$-1") return null;
            if (str[0] == '-')
            {
                HandlError(str);
                return null;
            }
            if (str[0] == ':')
                return str.Substring(1).ToByte();

            int len = 0;
            var parsed = int.TryParse(str.Substring(1), out len);
            if (len >=0 && parsed)
            {
                //len += 2;
                var tmp = new byte[len];
                var bytesRecd = 0;
                while (bytesRecd < len)
                {
                    int rcd = bs.Read(tmp, bytesRecd, len - bytesRecd);
                    bytesRecd += rcd;
                }

                bs.ReadByte();
                bs.ReadByte();

                return tmp;
            }
            else
            {
                //this bulk response could be a result of a transaction, so the
                //response is delayed and came in a bulk, that is different
                //than the usual bulk reply.
                return str.ToByte();
            }
        }

        protected internal byte[][] ReadMultibulkResponse()
        {
            var lines = this.ReadIntResponse();
            List<byte[]> listbytes = new List<byte[]>();
            while (lines > 0)
            {
                var bytes = this.ReadBulkResponse();
                if (bytes != null)
                {
                    listbytes.Add(bytes);
                }
                lines--;
            }

            return listbytes.ToArray();
        }

        private void HandlError(string error)
        {
            //log the error
        }
        #endregion

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            // For thread safety, use a lock around these  
            // operations, as well as in methods that use the resource. 
            if (!disposed)
            {
                if (disposing)
                {
                    if (this.Socket != null)
                    {
                        this.Socket.Dispose();
                        Console.WriteLine("Object disposed.");
                    }
                }

                this.Socket = null;

                // Indicate that the instance has been disposed.
                disposed = true;
            }
        }
    }
}
