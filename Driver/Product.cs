//------------------------------------------------------------------------------
// <auto-generated>
//    This code was generated from a template.
//
//    Manual changes to this file may cause unexpected behavior in your application.
//    Manual changes to this file will be overwritten if the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace Driver
{
    using System;
    using System.Collections.Generic;
using ProtoBuf;
    
    [ProtoContract]
    public partial class Product
    {
        [ProtoMember(1)]
        public int ID { get; set; }
        [ProtoMember(2)]
        public int VendorID { get; set; }
        [ProtoMember(3)]
        public int ProductTypeID { get; set; }
        [ProtoMember(4)]
        public Nullable<int> ProductSubTypeID { get; set; }
        [ProtoMember(5)]
        public string Code { get; set; }
        [ProtoMember(6)]
        public string InternalName { get; set; }
        [ProtoMember(7)]
        public string ExternalShortName { get; set; }
        [ProtoMember(8)]
        public string ExternalFullName { get; set; }
        [ProtoMember(9)]
        public bool Locked { get; set; }
        [ProtoMember(10)]
        public bool Deleted { get; set; }
        [ProtoMember(11)]
        public string InternalDescription { get; set; }
        [ProtoMember(12)]
        public string ExternalDescription { get; set; }
        [ProtoMember(13)]
        public string ExternalDescriptionWmd { get; set; }
    }
}
