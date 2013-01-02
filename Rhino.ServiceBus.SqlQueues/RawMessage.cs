using System;
using System.Collections.Specialized;

namespace Rhino.ServiceBus.SqlQueues
{
    public class RawMessage
    {
        public int MessageId { get; set; }
        public int QueueId { get; set; }
        public string SubQueueName { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime ProcessingUntil { get; set; }
        public bool Processed { get; set; }
        public string Headers { get; set; }
        public byte[] Payload { get; set; }
        public int ProcessedCount { get; set; }

        public Message ToMessage()
        {
            var message = new Message
                              {
                                  Data = Payload,
                                  Id = MessageId,
                                  SentAt = CreatedAt,
                                  SubQueue = SubQueueName,
                                  ProcessedCount = ProcessedCount
                              };
            message.Headers = extractHeaders(Headers);
            return message;
        }

        private static NameValueCollection extractHeaders(string headers)
        {
            var items = headers.Split(new[] { "##" }, StringSplitOptions.RemoveEmptyEntries);
            var nameValue = new NameValueCollection(items.Length);
            foreach (var item in items)
            {
                var values = item.Split('#');
                nameValue.Add(values[0], values[1]);
            }
            return nameValue;
        }

        public void SetHeaders(NameValueCollection nameValueCollection)
        {
            Headers = MessagePayload.CompressHeaders(nameValueCollection);
        }
    }
}