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

        #region Strings
        public int Append(byte[] key, byte[] value)
        {
            var cmd = new Command(Commands.Append);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(value);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int BitCount(byte[] key, byte[] start, byte[] end)
        {
            var cmd = new Command(Commands.Bitcount);
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
            var cmd = new Command(Commands.Decr);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse();
        }

        public int? DecrBy(byte[] key, byte[] decrement)
        {
            var cmd = new Command(Commands.Decrby);
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
            var cmd = new Command(Commands.Get);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public int GetBit(byte[] key, byte[] offset)
        {
            var cmd = new Command(Commands.Getbit);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(offset);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public byte[] GetRange(byte[] key, byte[] start, byte[] end)
        {
            var cmd = new Command(Commands.Getrange);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(start);
            cmd.ArgList.Add(end);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public byte[] GetSet(byte[] key, byte[] value)
        {
            var cmd = new Command(Commands.Getset);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(value);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public int? Incr(byte[] key)
        {
            var cmd = new Command(Commands.Incr);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse();
        }

        public int? IncrBy(byte[] key, byte[] increment)
        {
            var cmd = new Command(Commands.Incrby);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(increment);
            this.SendCommand(cmd);
            return this.ReadIntResponse();
        }

        public byte[] IncrByFloat(byte[] key, byte[] increment)
        {
            var cmd = new Command(Commands.Incrbyfloat);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(increment);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public byte[] MGet(params byte[][] keys)
        {
            var cmd = new Command(Commands.Mget);
            foreach(var key in keys)
                cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadMultibulkResponse();
        }

        public string MSet(byte[][] keys, byte[][] values)
        {
            if (keys.Length != values.Length)
                return null;

            var cmd = new Command(Commands.Mset);
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

            var cmd = new Command(Commands.Msetnx);
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
            var cmd = new Command(Commands.Set);
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
            var cmd = new Command(Commands.Setbit);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(offset);
            cmd.ArgList.Add(value);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int SetRange(byte[] key, byte[] offset, byte[] value)
        {
            var cmd = new Command(Commands.Setrange);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(offset);
            cmd.ArgList.Add(value);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int StrLen(byte[] key)
        {
            var cmd = new Command(Commands.Strlen);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }
        #endregion

        #region Hash
        public string HMSet(byte[] key, byte[][] fieldKeys, byte[][] fieldValues)
        {
            var cmd = new Command(Commands.Hmset);
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
            var cmd = new Command(Commands.Hgetall);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            var resp = this.ReadMultibulkResponse();
            return resp;
        }
        #endregion

        #region Keys
        public int Del(params byte[][] keys)
        {
            var cmd = new Command(Commands.Del);
            foreach(var key in keys)
            cmd.ArgList.Add(key);
                this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public byte[] Dump(byte[] key)
        {
            var cmd = new Command(Commands.Dump);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public int Exists(byte[] key)
        {
            var cmd = new Command(Commands.Exists);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int Expire(byte[] key, byte[] seconds)
        {
            var cmd = new Command(Commands.Expire);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(seconds);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int ExpireAt(byte[] key, byte[] timestamp)
        {
            var cmd = new Command(Commands.Expireat);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(timestamp);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public byte[] Keys(byte[] pattern)
        {
            var cmd = new Command(Commands.Keys);
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
            var cmd = new Command(Commands.Migrate);
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
            var cmd = new Command(Commands.Move);
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
            var cmd = new Command(Commands.Object);
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
            var cmd = new Command(Commands.Object);
            cmd.ArgList.Add(Commands.Encoding);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public int Persist(byte[] key)
        {
            var cmd = new Command(Commands.Persist);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int PExpire(byte[] key, byte[] seconds)
        {
            var cmd = new Command(Commands.Pexpire);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(seconds);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int PExpireAt(byte[] key, byte[] millisecondTimestamp)
        {
            var cmd = new Command(Commands.Pexpireat);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(millisecondTimestamp);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int PTtl(byte[] key)
        {
            var cmd = new Command(Commands.Pttl);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public byte[] RandomKey()
        {
            var cmd = new Command(Commands.Randomkey);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public string Rename(byte[] key, byte[] newkey)
        {
            var cmd = new Command(Commands.Rename);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(newkey);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public int? RenameNx(byte[] key, byte[] newkey)
        {
            var cmd = new Command(Commands.Renamenx);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(newkey);
            this.SendCommand(cmd);
            var result = this.ReadIntResponse();
            return result.HasValue ? result.Value : 0;
        }

        public string Restore(byte[] key, byte[] ttl, byte[] serializedValue)
        {
            var cmd = new Command(Commands.Restore);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(ttl);
            cmd.ArgList.Add(serializedValue);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public int Ttl(byte[] key)
        {
            var cmd = new Command(Commands.Ttl);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }
        #endregion

        #region Server
        public string[] Info()
        {
            var cmd = new Command(Commands.Info);
            this.SendCommand(cmd);
            var resp = this.ReadBulkResponse();
            var str = Encoding.UTF8.GetString(resp);
            var data = str.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            return data;
        }

        public string FlushAll()
        {
            var cmd = new Command(Commands.Flushall);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }
        #endregion

        #region Transaction
        public string Multi()
        {
            var cmd = new Command(Commands.Multi);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public byte[] Exec()
        {
            var cmd = new Command(Commands.Exec);
            this.SendCommand(cmd);
            return this.ReadMultibulkResponse();
        }

        public string Discard()
        {
            var cmd = new Command(Commands.Discard);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public string Watch(params byte[][] keys)
        {
            var cmd = new Command(Commands.Watch);
            foreach (var key in keys)
                cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public string Unwatch()
        {
            var cmd = new Command(Commands.Unwatch);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        #endregion

        #region Connection
        public string Auth(byte[] password)
        {
            var cmd = new Command(Commands.Auth);
            cmd.ArgList.Add(password);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public byte[] Echo(byte[] message)
        {
            var cmd = new Command(Commands.Echo);
            cmd.ArgList.Add(message);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public string Ping()
        {
            var cmd = new Command(Commands.Ping);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public string Quit()
        {
            var cmd = new Command(Commands.Quit);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

        public string Select(byte[] index)
        {
            var cmd = new Command(Commands.Select);
            cmd.ArgList.Add(index);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }
        #endregion

        protected internal void SendCommand(Command cmd)
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

        private byte[] ReadMultibulkResponse()
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
