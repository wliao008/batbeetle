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

        public byte[] Ping()
        {
            var cmd = new Command(Commands.Ping);
            this.SendCommand(cmd);
            return this.ReadResponse();
        }

        public string[] Info()
        {
            var cmd = new Command(Commands.Info);
            this.SendCommand(cmd);
            var resp = this.ReadResponse();
            var str = Encoding.UTF8.GetString(resp);
            var data = str.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            return data;
        }

        public void Set(byte[] key, byte[] value)
        {
            this.Set(key, value, null, null, false, false);
        }

        public void Set(byte[] key, byte[] value, byte[] ex, byte[] px, bool nx, bool xx)
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
            var resp = this.ReadResponse();
            if(resp != null)
                Console.WriteLine(Encoding.UTF8.GetString(resp));
        }

        public byte[] Get(byte[] key)
        {
            var cmd = new Command(Commands.Get);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);

            return this.ReadResponse();
        }

        public void HMSet(byte[] key, byte[][] fieldKeys, byte[][] fieldValues)
        {
            var cmd = new Command(Commands.HMSet);
            cmd.ArgList.Add(key);
            for (int i = 0; i < fieldKeys.Length; ++i)
            {
                cmd.ArgList.Add(fieldKeys[i]);
                cmd.ArgList.Add(fieldValues[i]);
            }
            this.SendCommand(cmd);
            var resp = this.ReadResponse();
            if (resp != null)
                Console.WriteLine(Encoding.UTF8.GetString(resp));
        }

        public byte[] HMGetAll(byte[] key)
        {
            var cmd = new Command(Commands.HGetAll);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            var resp = this.ReadResponse();
            return resp;
            //var tmp = this.Socket.Receive(buf); //this.ParseResponse();
            //var str = Encoding.UTF8.GetString(buf, 0, tmp);
            //return str.ToByte();
        }

        #region Keys
        public byte[] Del(byte[] key)
        {
            var cmd = new Command(Commands.Del);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            var resp = this.ReadResponse();
            return resp;
        }

        public byte[] Expire(byte[] key, byte[] seconds)
        {
            var cmd = new Command(Commands.Expire);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(seconds);
            this.SendCommand(cmd);
            var resp = this.ReadResponse();
            return resp;
        }

        public byte[] Dump(byte[] key)
        {
            var cmd = new Command(Commands.Dump);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
            var resp = this.ReadResponse();
            return resp;
        }
        #endregion

        public void Dispose()
        {
            try
            {
                if (this.Socket != null && this.Socket.Connected)
                {
                    this.Socket.Close();
                    this.Socket.Dispose();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private void SendCommand(Command cmd)
        {
            if (this.Socket == null)
                Connect();

            var bytes = cmd.ToBytes();
            this.Socket.Send(bytes);
        }

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

        private void DevNull()
        {
            this.Socket.Receive(buf);
        }
    }
}
