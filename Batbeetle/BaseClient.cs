using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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

        #region Strings
        public int Append(byte[] key, byte[] value)
        {
            var cmd = new Command(Commands.Append);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(value);
            this.SendCommand(cmd);
            return this.ReadIntResponse();
        }

        public int Bitcount(byte[] key, byte[] start, byte[] end)
        {
            var cmd = new Command(Commands.Bitcount);
            cmd.ArgList.Add(key);
            if (start != null)
                cmd.ArgList.Add(start);
            if (end != null)
                cmd.ArgList.Add(end);
            this.SendCommand(cmd);
            return this.ReadIntResponse();
        }

        public string Set(byte[] key, byte[] value)
        {
            return this.Set(key, value, null, null, false, false);
        }

        public string Set(byte[] key, byte[] value, byte[] ex, byte[] px, bool nx, bool xx)
        {
            var cmd = new Command(Commands.Set);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(value);
            if (ex != null && ex[0] != 0x30)
            {
                cmd.ArgList.Add(Commands.Ex);
                cmd.ArgList.Add(ex);
            }

            if (px != null && px[0] != 0x30)
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

        public byte[] Get(byte[] key)
        {
            var cmd = new Command(Commands.Get);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            return this.ReadBulkResponse();
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
            return this.ReadIntResponse();
        }

        public int Expire(byte[] key, byte[] seconds)
        {
            var cmd = new Command(Commands.Expire);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(seconds);
            this.SendCommand(cmd);
            return this.ReadIntResponse();
        }

        public string Dump(byte[] key)
        {
            var cmd = new Command(Commands.Dump);
            cmd.ArgList.Add(key);
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

        private int ReadIntResponse()
        {
            var resp = this.ReadStringResponse();
            int ret = 0;
            int.TryParse(resp, out ret);
            return ret;
        }

        private byte[] ReadBulkResponse()
        {
            var str = this.ReadLine();
            if (str == "$-1\r\n") return null;
            int len = 0;
            int.TryParse(str.Substring(1), out len);
            if (len > 0)
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

        [Obsolete]
        private byte[] ReadResponse()
        {
            var str = this.ReadLine();
            if (string.IsNullOrEmpty(str))
                throw new Exception("Response is empty");

            switch (str[0])
            {
                case '+'://status
                case '-'://error
                    return Encoding.UTF8.GetBytes(str.Substring(1));
                case ':'://integer
                    return str.Substring(1).ToByte();
                case '$'://bulk
                    if (str == "$-1\r\n") return null;
                    int len = int.Parse(str.Substring(1)) + 2;
                    var buf = new byte[len];
                    var bytesRecd = 0;
                    while (bytesRecd < len)
                    {
                        int rcd = bs.Read(buf, bytesRecd, len - bytesRecd);
                        bytesRecd += rcd;
                    }

                    return buf;
                case '*'://multi bulk
                    var lines = int.Parse(str.Substring(1));
                    List<byte[]> listbytes = new List<byte[]>();
                    var totalLen = 0;
                    while (lines > 0)
                    {
                        var bytes = ReadResponse();
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

            return str.ToByte();
        }
        #endregion
    }
}
