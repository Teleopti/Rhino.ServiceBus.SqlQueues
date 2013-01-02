namespace Rhino.ServiceBus.SqlQueues
{
    public class AddItemRequest
    {
        public string Key { get; set; }

        public byte[] Data { get; set; }
    }
}