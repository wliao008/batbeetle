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
            var cmd = new Command(Commands.DecrBy);
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
            var cmd = new Command(Commands.GetBit);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(offset);
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public byte[] GetRange(byte[] key, byte[] start, byte[] end)
        {
            var cmd = new Command(Commands.GetRange);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(start);
            cmd.ArgList.Add(end);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public byte[] GetSet(byte[] key, byte[] value)
        {
            var cmd = new Command(Commands.GetSet);
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
            var cmd = new Command(Commands.IncrBy);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(increment);
            this.SendCommand(cmd);
            return this.ReadIntResponse();
        }

        public byte[] IncrByFloat(byte[] key, byte[] increment)
        {
            var cmd = new Command(Commands.IncrByFloat);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(increment);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
        }

        public byte[] MGet(params byte[][] keys)
        {
            var cmd = new Command(Commands.MGet);
            foreach(var key in keys)
                cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadMultibulkResponse();
        }

        public string MSet(byte[][] keys, byte[][] values)
        {
            if (keys.Length != values.Length)
                return null;

            var cmd = new Command(Commands.MSet);
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

            var cmd = new Command(Commands.MSetNx);
            for (int i = 0; i < keys.Length; ++i)
            {
                cmd.ArgList.Add(keys[i]);
                cmd.ArgList.Add(values[i]);
            }
            this.SendCommand(cmd);
            return this.ReadIntResponse().Value;
        }

        public int SetBit(byte[] key, byte[] offset, byte[] value)
        {
            var cmd = new Command(Commands.SetBit);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(offset);
            cmd.ArgList.Add(value);
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
        #endregion

        #region Hash
        public string HMSet(byte[] key, byte[][] fieldKeys, byte[][] fieldValues)
        {
            var cmd = new Command(Commands.HMSet);
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
            var cmd = new Command(Commands.HGetAll);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            var resp = this.ReadMultibulkResponse();
            return resp;
        }
        #endregion

        #region Keys
        public int Del(byte[] key)
        {
            var cmd = new Command(Commands.Del);
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

        public string Dump(byte[] key)
        {
            var cmd = new Command(Commands.Dump);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }
        #endregion

        #region Server
        public string Ping()
        {
            var cmd = new Command(Commands.Ping);
            this.SendCommand(cmd);
            return this.ReadStringResponse();
        }

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
            var cmd = new Command(Commands.FlushAll);
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
                sb.Append((char)c);
                if (c == '\n') break;
            }
            return sb.ToString();
        }

        private string ReadStringResponse()
        {
            var str = this.ReadLine();
            if (string.IsNullOrEmpty(str))
                throw new Exception("Response is empty");
            Console.WriteLine("ReadStringResponse: " + str);
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
            if (str == "$-1\r\n") return null;
            if (str[0] == '-')
            {
                HandlError(str);
                return null;
            }

            int len = 0;
            var parsed = int.TryParse(str.Substring(1), out len);
            if (len >=0 && parsed)
            {
                len += 2;
                var tmp = new byte[len];
                var bytesRecd = 0;
                while (bytesRecd < len)
                {
                    int rcd = bs.Read(tmp, bytesRecd, len - bytesRecd);
                    bytesRecd += rcd;
                }
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
            List<byte[]> listbytes = new List<byte[]>();
            var totalLen = 0;
            while (lines > 0)
            {
                var bytes = this.ReadBulkResponse();
                if (bytes != null)
                {
                    totalLen += bytes.Length;
                    listbytes.Add(bytes);
                }
                lines--;
            }

            byte[] sum = new byte[totalLen];
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
