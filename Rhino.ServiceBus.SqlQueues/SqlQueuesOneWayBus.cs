using System;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Internal;

namespace Rhino.ServiceBus.SqlQueues
{
    [CLSCompliant(false)]
    public class SqlQueuesOneWayBus : SqlQueuesTransport,IOnewayBus
    {
        private MessageOwnersSelector messageOwners;
        public static readonly Uri NullEndpoint = new Uri("null://nowhere:24689/middle");
        public SqlQueuesOneWayBus(MessageOwner[] messageOwners, IMessageSerializer messageSerializer, string connectionString, IMessageBuilder<MessagePayload> messageBuilder)
            : base(NullEndpoint, new EndpointRouter(), messageSerializer, 0, connectionString, 5, messageBuilder)

        {
            this.messageOwners = new MessageOwnersSelector(messageOwners, new EndpointRouter());
            Start();
        }

        public void Send(params object[] msgs)
        {
            base.Send(messageOwners.GetEndpointForMessageBatch(msgs), msgs);
        }
    }
}