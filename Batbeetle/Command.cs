using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Batbeetle
{
    public class Command
    {
        /*
*<number of arguments> CR LF
$<number of bytes of argument 1> CR LF
<argument data> CR LF
...
$<number of bytes of argument N> CR LF
<argument data> CR LF*/

        public readonly byte[] Crlf = new byte[] { 0x0D, 0x0A };
        public int NumArgs { get; set; }
        public byte[] Cmd { get; set; }
        public List<byte[]> ArgList { get; set; }
        public Command(byte[] cmd)
        {
            this.Cmd = cmd;
            this.ArgList = new List<byte[]>();
        }

        public byte[] ToBytes()
        {
            var bytes = new List<byte>();
            //number of arguments
            this.NumArgs = this.ArgList.Count + 1;
            bytes.Add(0x2A);
            bytes.AddRange(Encoding.UTF8.GetBytes(this.NumArgs.ToString()));
            bytes.AddRange(Crlf);
            //command length
            bytes.Add(0x24);
            bytes.AddRange(Encoding.UTF8.GetBytes(this.Cmd.Length.ToString()));
            bytes.AddRange(Crlf);
            //command byte
            bytes.AddRange(this.Cmd);
            bytes.AddRange(Crlf);
            //command args
            this.ArgList.ForEach(x =>
            {
                bytes.Add(0x24);
                bytes.AddRange(Encoding.UTF8.GetBytes(x.Length.ToString()));
                bytes.AddRange(Crlf);
                bytes.AddRange(x);
                bytes.AddRange(Crlf);
            });
            
            return bytes.ToArray();
        }

        private byte[] IntToBytes(int num)
        {
            byte[] bytes = BitConverter.GetBytes(num);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(bytes);
            return bytes;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            var bytes = this.ToBytes();
            foreach (var b in bytes)
            {
                if (b == '\r')
                    sb.Append("\\r");
                if (b == '\n')
                    sb.Append("\\n");
                else
                    sb.Append((char)b);
            }

            return sb.ToString();
        }
    }
}
