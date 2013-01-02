using System;
using System.Collections.Specialized;
using System.Text;

namespace Rhino.ServiceBus.SqlQueues
{
    public class MessagePayload
    {
        public int Id { get; set; }
        public byte[] Data { get; set; }
        public string Queue { get; set; }
        public string SubQueue { get; set; }
        public DateTime SentAt { get; set; }
        public NameValueCollection Headers { get; set; }

    	public DateTime? DeliverBy { get; set; }

    	public int? MaxAttempts { get; set; }

    	public static string CompressHeaders(NameValueCollection nameValueCollection)
        {
            StringBuilder stringBuilder = new StringBuilder();
            foreach (string key in nameValueCollection.Keys)
            {
                if (stringBuilder.Length > 0)
                {
                    stringBuilder.Append("##");
                }
                stringBuilder.Append(key);
                stringBuilder.Append("#");
                stringBuilder.Append(nameValueCollection[key]);
            }
            return stringBuilder.ToString();
        }
    }
}