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
