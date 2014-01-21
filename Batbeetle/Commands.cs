﻿namespace Batbeetle
{
    public static class Commands
    {
        public static readonly byte[] Info = new byte[] { 0x49, 0x4E, 0x46, 0x4F };

        //string
        public static readonly byte[] Append = new byte[] { 0x41, 0x50, 0x50, 0x45, 0x4E, 0x44 };
        public static readonly byte[] Bitcount = new byte[] { 0x42, 0x49, 0x54, 0x43, 0x4f, 0x55, 0x4E, 0x54 };
        public static readonly byte[] Decr = new byte[] { 0x44, 0x45, 0x43, 0x52 };
        public static readonly byte[] DecrBy = new byte[] { 0x44, 0x45, 0x43, 0x52, 0x42, 0x59 };
        public static readonly byte[] Get = new byte[] { 0x47, 0x45, 0x54 };
        public static readonly byte[] GetBit = new byte[] { 0x47, 0x45, 0x54, 0x42, 0x49, 0x54 };
        public static readonly byte[] GetRange = new byte[] { 0x47, 0x45, 0x54, 0x52, 0x41, 0x4E, 0x47, 0x45 };
        public static readonly byte[] GetSet = new byte[] { 0x47, 0x45, 0x54, 0x53, 0x45, 0x54 };
        public static readonly byte[] Incr = new byte[] { 0x49, 0x4E, 0x43, 0x52 };
        public static readonly byte[] IncrBy = new byte[] { 0x49, 0x4E, 0x43, 0x52, 0x42, 0x59 };
        public static readonly byte[] IncrByFloat = new byte[] { 0x49, 0x4E, 0x43, 0x52, 0x42, 0x59, 0x46, 0x4C, 0x4F, 0x41, 0x54 };
        public static readonly byte[] MGet = new byte[] { 0x4D, 0x47, 0x45, 0x54 };
        public static readonly byte[] MSet = new byte[] { 0x4D, 0x53, 0x45, 0x54 };
        public static readonly byte[] MSetNx = new byte[] { 0x4D, 0x53, 0x45, 0x54, 0x4E, 0x58 };
        public static readonly byte[] Set = new byte[] { 0x53, 0x45, 0x54 };
        public static readonly byte[] SetBit = new byte[] { 0x53, 0x45, 0x54, 0x42, 0x49, 0x54 };
        public static readonly byte[] SetRange = new byte[] { 0x53, 0x45, 0x54, 0x52, 0x41, 0x4E, 0x47, 0x45 };
        public static readonly byte[] StrLen = new byte[] { 0x53, 0x54, 0x52, 0x4C, 0x45, 0x4E };

        //hash
        public static readonly byte[] HMSet = new byte[] { 0x48, 0x4D, 0x53, 0x45, 0x54 };
        public static readonly byte[] HGetAll = new byte[] { 0x48, 0x47, 0x45, 0x54, 0x41, 0x4C, 0x4C };

        //transaction
        public static readonly byte[] Multi = new byte[] { 0x4D, 0x55, 0x4C, 0x54, 0x49 };
        public static readonly byte[] Watch = new byte[] { 0x57, 0x41, 0x54, 0x43, 0x48 };
        public static readonly byte[] Unwatch = new byte[] { 0x55, 0x4E, 0x57, 0x41, 0x54, 0x43, 0x48 };
        public static readonly byte[] Exec = new byte[] { 0x45, 0x58, 0x45, 0x43 };
        public static readonly byte[] Discard = new byte[] { 0x44, 0x49, 0x53, 0x43, 0x41, 0x52, 0x44 };

        //keys
        public static readonly byte[] Del = new byte[] { 0x44, 0x45, 0x4C };
        public static readonly byte[] Expire = new byte[] { 0x45, 0x58, 0x50, 0x49, 0x52, 0x45 };
        public static readonly byte[] Dump = new byte[] { 0x44, 0x55, 0x4d, 0x50 };
        public static readonly byte[] Exists = new byte[] { 0x45, 0x58, 0x49, 0x53, 0x54, 0x53 };
        public static readonly byte[] Ttl = new byte[] { 0x54, 0x54, 0x4C };
        public static readonly byte[] ExpireAt = new byte[] { 0x45, 0x58, 0x50, 0x49, 0x52, 0x45, 0x41, 0x54 };
        public static readonly byte[] Keys = new byte[] { 0x4B, 0x45, 0x59, 0x53 };
        public static readonly byte[] Migrate = new byte[] { 0x4D, 0x49, 0x47, 0x52, 0x41, 0x54, 0x45 };
        public static readonly byte[] Move = new byte[] { 0x4D, 0x4F, 0x56, 0x45 };
        public static readonly byte[] Object = new byte[] { 0x4F, 0x42, 0x4A, 0x45, 0x43, 0x54 };
        public static readonly byte[] RefCount = new byte[] { 0x52, 0x45, 0x46, 0x43, 0x4F, 0x55, 0x4E, 0x54 };
        public static readonly byte[] Encoding = new byte[] { 0x45, 0x4E, 0x43, 0x4F, 0x44, 0x49, 0x4E, 0x47 };
        public static readonly byte[] IdleTime = new byte[] { 0x49, 0x44, 0x4C, 0x45, 0x54, 0x49, 0x4D, 0x45 };
        public static readonly byte[] Persist = new byte[] { 0x50, 0x45, 0x52, 0x53, 0x49, 0x53, 0x54 };
        public static readonly byte[] PExpire = new byte[] { 0x50, 0x45, 0x58, 0x50, 0x49, 0x52, 0x45 };
        public static readonly byte[] PExpireAt = new byte[] { 0x50, 0x45, 0x58, 0x50, 0x49, 0x52, 0x45, 0x41, 0x54 };
        public static readonly byte[] PTtl = new byte[] { 0x50, 0x54, 0x54, 0x4C };
        public static readonly byte[] RandomKey = new byte[] { 0x52, 0x41, 0x4E, 0x44, 0x4F, 0x4D, 0x4B, 0x45, 0x59 };
        public static readonly byte[] Rename = new byte[] { 0x52, 0x45, 0x4E, 0x41, 0x4D, 0x45 };

        //connection
        public static readonly byte[] Ping = new byte[] { 0x50, 0x49, 0x4E, 0x47 };
        public static readonly byte[] Auth = new byte[] { 0x41, 0x55, 0x54, 0x48 };
        public static readonly byte[] Echo = new byte[] { 0x45, 0x43, 0x48, 0x4F };
        public static readonly byte[] Quit = new byte[] { 0x51, 0x55, 0x49, 0x54 };
        public static readonly byte[] Select = new byte[] { 0x53, 0x45, 0x4C, 0x45, 0x43, 0x54 };

        //server
        public static readonly byte[] FlushAll = new byte[] { 0x46, 0x4C, 0x55, 0x53, 0x48, 0x41, 0x4C, 0x4C };

        //args for the Set command
        public static readonly byte[] Ex = new byte[] { 0x45, 0x58 };
        public static readonly byte[] Px = new byte[] { 0x50, 0x58 };
        public static readonly byte[] Nx = new byte[] { 0x4E, 0x58 };
        public static readonly byte[] Xx = new byte[] { 0x58, 0x58 };

        //args for Migrate
        public static readonly byte[] Copy = new byte[] { 0x43, 0x4F, 0x50, 0x59 };
        public static readonly byte[] Replace = new byte[] { 0x52, 0x45, 0x50, 0x4C, 0x41, 0x43, 0x45 };
    }
}
