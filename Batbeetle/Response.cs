using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Batbeetle
{
    public class Response
    {
        public Replies Reply { get; set; }
        public string Data { get; set; }

        public void ToArray()
        {
            if (!string.IsNullOrEmpty(this.Data))
            {
                var data = this.Data.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            }
        }
    }

    public enum Replies
    {
        Status,
        Error,
        Integer,
        Bulk,
        MultiBulk
    }
}
