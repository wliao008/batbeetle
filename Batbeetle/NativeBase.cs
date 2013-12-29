using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Batbeetle
{
    public abstract class NativeBase : IDisposable
    {        
        public readonly string Host = "127.0.0.1";
        public readonly int Port = 6379;
        public readonly byte[] Crlf = new byte[] { 0x0D, 0x0A };
        public Socket Socket { get; set; }
        private byte[] buf = new byte[512];
        private IList<ArraySegment<byte>> buffers;

        public NativeBase()
            : this("127.0.0.1")
        {
        }

        public NativeBase(string host, int port = 6379)
        {
            this.Host = host;
            this.Port = port;
            this.Socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            buffers = new List<ArraySegment<byte>>();
        }

        public async Task Ping()
        {
            Command cmd = new Command(Commands.Ping);
            await this.SendCommandAsync(cmd);
        }

        public void Set(string key, object value)
        {
            this.Set(key, Encoding.UTF8.GetBytes(value.ToString()), null);
        }

        public void Set(string key, string value)
        {
            this.Set(key, Encoding.UTF8.GetBytes(value), null);
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

            this.SendCommand(args);
        }

        public Task Connect()
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
            var task = Task.Factory.StartNew(() =>
            {
                var bytes = cmd.ToBytes();
                var len = bytes.Length;
                this.Socket.Send(bytes);
                Console.WriteLine("Command sent");
            });

            return task.ContinueWith((r) => this.ReceiveResponseAsync());
        }

        private Task ReceiveResponseAsync()
        {
            var task = Task.Factory.StartNew(() =>
            {
                var buffs = new byte[5];
                this.Socket.Receive(buffs);
                Console.WriteLine("Response:");
                Console.WriteLine(Encoding.UTF8.GetString(buffs));
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

        private void SendCommand(List<byte[]> cmds)
        {
            this.SendCommand(cmds.ToArray());
        }

        private void SendCommand(params byte[][] args)
        {
            this.Write2Buffer(this.BuildCmdBytes(CmdPrefix.NumArgs, args.Length));
            foreach (var arg in args)
            {
                if (arg == null) continue;
                this.Write2Buffer(this.BuildCmdBytes(CmdPrefix.NumBytes, arg.Length));
                this.Write2Buffer(arg);
                this.Write2Buffer(Crlf);
            }

            if (!this.Socket.Connected)
                Connect();

            this.Socket.BeginSend(buffers, SocketFlags.None, (result) =>
            {
                Console.WriteLine("Command sent");
            }, null);
        }

        private void Write2Buffer(byte[] bytes)
        {
            buffers.Add(new ArraySegment<byte>(bytes, 0, bytes.Length));
        }

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
