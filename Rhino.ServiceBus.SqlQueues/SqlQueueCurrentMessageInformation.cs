using System;
using Rhino.ServiceBus.Impl;

namespace Rhino.ServiceBus.SqlQueues
{
    [CLSCompliant(false)]
    public class SqlQueueCurrentMessageInformation : CurrentMessageInformation
    {
        public Uri ListenUri { get; set; }
        public IQueue Queue { get; set; }
        public Message TransportMessage { get; set; }
    }
}