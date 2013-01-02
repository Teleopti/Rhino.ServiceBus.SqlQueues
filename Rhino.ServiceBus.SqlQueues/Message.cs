using System;
using System.Collections.Specialized;

namespace Rhino.ServiceBus.SqlQueues
{
    public class Message
    {
        public int Id { get; set; }
        public byte[] Data { get; set; }
        public string Queue { get; set; }
        public string SubQueue { get; set; }
        public DateTime SentAt { get; set; }
        public NameValueCollection Headers { get; set; }

        public bool FinishedProcessing { get; set; }
        public int ProcessedCount { get; set; }
    }
}