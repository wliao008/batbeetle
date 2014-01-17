using System;
using System.Collections.Generic;
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

        public void Ping()
        {
            var cmd = new Command(Commands.Ping);
            this.SendCommand(cmd);
        }

        public void Set(byte[] key, byte[] value)
        {
            var cmd = new Command(Commands.Set);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(value);
            this.SendCommand(cmd);
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
        }

        public void Get(byte[] key)
        {
            var cmd = new Command(Commands.Get);
            cmd.ArgList.Add(key);
            this.SendCommand(cmd);
        }

        private void SendCommand(Command cmd)
        {
            if (this.Socket == null)
                Connect();

            Console.WriteLine("Sending " + cmd.ToString());
            var bytes = cmd.ToBytes();
            this.Socket.Send(bytes);

            var sb = new StringBuilder();
            int byteRecd = this.Socket.Receive(bytes);
            sb.Append(Encoding.UTF8.GetString(bytes, 0, byteRecd));
            Console.WriteLine("\nRESPONSE: " + sb.ToString());
        }

        /*
        public void Set(string key, object value)
        {
            this.Set(key, Encoding.UTF8.GetBytes(value.ToString()), null);
        }

        public async Task Set(string key, string value)
        {
            //this.Set(key, Encoding.UTF8.GetBytes(value), null);
            Command cmd = new Command(Commands.Set);
            cmd.ArgList.Add(key);
            cmd.ArgList.Add(value);
            await this.SendCommandAsync(cmd);
        }

        public void Set(string key, byte[] value, int? expireInSec = null)
        {
            List<byte[]> args = new List<byte[]>();
            args.Add(Commands.Set);
            args.Add(Encoding.UTF8.GetBytes(key));
            args.Add(value);

            if (expireInSec.HasValue)
            {
                args.Add(Commands.Ex);
                args.Add(Encoding.UTF8.GetBytes(expireInSec.Value.ToString()));
            }

        }*/

        public Task Connect2()
        {
            IPAddress ip = IPAddress.Parse(this.Host);
            IPEndPoint ep = new IPEndPoint(ip, this.Port);
            var task = Task.Factory.FromAsync(
                this.Socket.BeginConnect, 
                this.Socket.EndConnect, 
                ep, 
                null);
            return task;
        }

        private Task SendCommandAsync(Command cmd)
        {
            var cmdBytes = cmd.ToBytes();
            var task = Task.Factory.FromAsync<int>(
                this.Socket.BeginSend(cmdBytes, 0, cmdBytes.Length, SocketFlags.None, null, null),
                this.Socket.EndSend);

            return task.ContinueWith((r) => this.ReceiveResponseAsync());
        }

        private Task<Response> ReceiveResponseAsync()
        {
            Response resp = new Response();
            StringBuilder sb = new StringBuilder();
            //var task = Task.Factory.FromAsync<int>(
            //    this.Socket.BeginReceive(buf, 0, buf.Length, SocketFlags.None, null, null),
            //    this.Socket.EndReceive);

            var task = Task.Factory.StartNew(() =>
            {
                Console.WriteLine("Response:\n");
                int byteRecd = 0;
                int totalByteRecd = 0;

                while (this.Socket.Available > 0)
                {
                    byteRecd = this.Socket.Receive(buf);
                    //Console.Write(Encoding.UTF8.GetString(buf, 0, byteRecd));
                    sb.Append(Encoding.UTF8.GetString(buf, 0, byteRecd));
                    totalByteRecd += byteRecd;
                }
                Console.WriteLine("Bytes recd: " + totalByteRecd);
                var str = sb.ToString();
                if (!string.IsNullOrEmpty(str))
                {
                    //parse the response
                    switch (str[0])
                    {
                        case '+'://status
                            resp.Reply = Replies.Status;
                            break;
                        case '-'://error
                            break;
                        case ':'://integer
                            break;
                        case '$'://bulk
                            break;
                        case '*'://multibulk
                            break;
                    }
                    resp.Data = str;
                    resp.ToArray();
                    Console.WriteLine(resp.Data);
                }

                return resp;
            });

            return task;
        }

        /*
        public void Set(string key, object value, int? expireInSec = null)
        {
            List<byte[]> args = new List<byte[]>();

            args.Add(Commands.Set);
            args.Add(Encoding.UTF8.GetBytes(key));
            args.Add(Encoding.UTF8.GetBytes(value.ToString()));

            if (expireInSec.HasValue)
            {
                args.Add(Commands.Ex);
                args.Add(Encoding.UTF8.GetBytes(expireInSec.Value.ToString()));
            }

            this.SendCommand(args);                
        }

        public async Task SetAsync(string key, object value)
        {
            await ConnectAsync().ContinueWith((task) =>
            {
                this.SendCommand(
                    Commands.Set,
                    Encoding.UTF8.GetBytes(key),
                    Encoding.UTF8.GetBytes(value.ToString()));
            });
        }
        */

        /// <summary>
        /// Format the command bytes according to redis protoco spec
        /// </summary>
        private byte[] BuildCmdBytes(CmdPrefix prefix, int num)
        {
            byte[] prefixbytes = prefix == CmdPrefix.NumArgs ? new byte[] { 0x2A } : new byte[] { 0x24 };
            byte[] intbytes = Encoding.UTF8.GetBytes(num.ToString());
            byte[] ret = new byte[prefixbytes.Length + intbytes.Length + 2];
            System.Buffer.BlockCopy(prefixbytes, 0, ret, 0, prefixbytes.Length);
            System.Buffer.BlockCopy(intbytes, 0, ret, prefixbytes.Length, intbytes.Length);
            System.Buffer.BlockCopy(Crlf, 0, ret, prefixbytes.Length + intbytes.Length, Crlf.Length);
            return ret;
        }

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
    }

    public enum CmdPrefix
    {
        NumArgs,
        NumBytes
    }
}
