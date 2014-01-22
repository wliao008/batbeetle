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

        public byte[] Keys(byte[] pattern)
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
        public int Append(byte[] key, byte[] value)
        {
            var cmd = new RedisCommand(Commands.Append);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(value);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int BitCount(byte[] key, byte[] start, byte[] end)
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

        public int? Decr(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Decr);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse();
        }

        public int? DecrBy(byte[] key, byte[] decrement)
        {
            var cmd = new RedisCommand(Commands.Decrby);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(decrement);
            this.SendCommand(cmd);
            return this.ReadIntResponse();
        }

        public string Set(byte[] key, byte[] value)
        {
            return this.Set(key, value, null, null, false, false);
        }

        public byte[] Get(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Get);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public int GetBit(byte[] key, byte[] offset)
        {
            var cmd = new RedisCommand(Commands.Getbit);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(offset);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public byte[] GetRange(byte[] key, byte[] start, byte[] end)
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

        public byte[] MGet(params byte[][] keys)
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

        public string Set(byte[] key, byte[] value, byte[] ex, byte[] px, bool nx, bool xx)
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

        #region Hash
        public string HMSet(byte[] key, byte[][] fieldKeys, byte[][] fieldValues)
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

        public byte[] HMGetAll(byte[] key)
        {
            var cmd = new RedisCommand(Commands.Hgetall);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            var resp = this.ReadMultibulkResponse();
            return resp;
        }
        #endregion

        #region Pub/Sub
        public byte[] PSubscribe(params byte[][] patterns)
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

        public byte[] Subscribe(params byte[][] channels)
        {
            var cmd = new RedisCommand(Commands.Subscribe);
            foreach (var channel in channels)
                cmd.ArgList.Add(channel);
            this.SendCommand(cmd);
            return this.ReadMultibulkResponse();
        }
        #endregion

        #region Transaction
        public string Multi()
        {
            var cmd = new RedisCommand(Commands.Multi);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public byte[] Exec()
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

        public string Ping()
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

        protected internal void SendCommand(RedisCommand cmd)
        {
            if (this.Socket == null)
                Connect();

            var bytes = cmd.ToBytes();
            this.Socket.Send(bytes);
        }

        #region Parsing response
        private string ReadLine()
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

        public int Available()
        {
            return this.Socket.Available;
        }

        private string ReadStringResponse()
        {
            var str = this.ReadLine();
            if (string.IsNullOrEmpty(str))
                throw new Exception("Response is empty");
            if (str[0] == '-')
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
            if (str == "$-1") return null;
            if (str[0] == '-')
            {
                HandlError(str);
                return null;
            }

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

        protected internal byte[] ReadMultibulkResponse()
        {
            var lines = this.ReadIntResponse();
            var actualines = 0;
            List<byte[]> listbytes = new List<byte[]>();
            var totalLen = 0;
            while (lines > 0)
            {
                var bytes = this.ReadBulkResponse();
                if (bytes != null)
                {
                    totalLen += bytes.Length;
                    listbytes.Add(bytes);
                    listbytes.Add(new byte[] { 0x0A });
                    actualines++;
                }
                lines--;
            }

            byte[] sum = new byte[totalLen + actualines];
            int idx = 0;
            listbytes.ForEach(x =>
            {
                x.CopyTo(sum, idx);
                idx += x.Length;
            });
            return sum;
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
