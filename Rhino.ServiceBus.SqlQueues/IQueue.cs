using System.Collections.Generic;

namespace Rhino.ServiceBus.SqlQueues
{
    public interface IQueue
    {
        void MoveTo(string subQueue, Message message);
        void EnqueueDirectlyTo(string subQueue, MessagePayload messagePayload);
        string QueueName { get; }
        IEnumerable<Message> GetAllMessages(string queue);
        Message PeekById(int messageId);
    }
}