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
        public Command(byte[] cmd)
        {
            this.Cmd = cmd;
            this.NumArgs = 1;
        }

        public byte[] ToBytes()
        {
            var bytes = new List<byte>();
            //number of arguments
            bytes.Add(0x2A);
            bytes.AddRange(Encoding.ASCII.GetBytes(this.NumArgs.ToString()));
            bytes.AddRange(Crlf);
            //command length
            bytes.Add(0x24);
            bytes.AddRange(Encoding.ASCII.GetBytes(this.Cmd.Length.ToString()));
            bytes.AddRange(Crlf);
            //command byte
            bytes.AddRange(this.Cmd);
            bytes.AddRange(Crlf);
            
            return bytes.ToArray();
        }
    }
}
