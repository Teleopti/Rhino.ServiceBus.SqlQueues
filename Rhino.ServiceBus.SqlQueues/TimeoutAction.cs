using System;
using System.Collections.Generic;
using System.Threading;
using System.Xml;
using Common.Logging;
using Rhino.ServiceBus.DataStructures;
using Rhino.ServiceBus.Transport;

namespace Rhino.ServiceBus.SqlQueues
{
    public class TimeoutAction : IDisposable
    {
        private readonly ISqlQueue queue;
        private readonly ILog logger = LogManager.GetLogger(typeof (TimeoutAction));
        private readonly Timer timeoutTimer;
        private readonly OrderedList<DateTime, IList<int>> timeoutMessageIds =
			new OrderedList<DateTime, IList<int>>();

        [CLSCompliant(false)]
        public TimeoutAction(ISqlQueue queue)
        {
            this.queue = queue;
        	timeoutMessageIds.Write(writer =>
        	                        	{
        	                        		var allMessages = queue.GetAllMessages(SubQueue.Timeout.ToString());
        	                        		foreach (var message in allMessages)
        	                        		{
        	                        			var time = message.Headers["time-to-send"];
        	                        			if (!string.IsNullOrEmpty(time))
        	                        			{
        	                        				var timeToSend = XmlConvert.ToDateTime(time,
        	                        				                                       XmlDateTimeSerializationMode.Unspecified);
        	                        				logger.DebugFormat("Registering message {0} to be sent at {1} on {2}",
        	                        				                   message.Id, timeToSend, queue.QueueName);
        	                        				writer.WriteMessageId(timeToSend, message);
        	                        			}
        	                        		}
        	                        	});
            timeoutTimer = new Timer(OnTimeoutCallback, null, TimeSpan.FromSeconds(0), TimeSpan.FromSeconds(1));
        }

	    public static DateTime CurrentTime
        {
            get { return DateTime.Now; }
        }

        private void OnTimeoutCallback(object state)
        {
            bool haveTimeoutMessages = false;

            timeoutMessageIds.Read(reader =>
                                   haveTimeoutMessages = reader.HasAnyBefore(CurrentTime)
                );

            if (haveTimeoutMessages == false)
                return;

            timeoutMessageIds.Write(writer =>
            {
                KeyValuePair<DateTime, List<IList<int>>> pair;
                while (writer.TryRemoveFirstUntil(CurrentTime, out pair))
                {
                    if (pair.Key > CurrentTime)
                        return;

                    foreach (var messageIdCollection in pair.Value)
                    {
                        try
                        {
                            logger.DebugFormat("Moving message {0} to main queue: {1}",
                                               messageIdCollection, queue.QueueName);
                            using (var tx = queue.BeginTransaction())
                            {
	                            foreach (var id in messageIdCollection)
	                            {
									var message = queue.PeekById(id);
									if (message == null)
									{
										logger.DebugFormat("Failed to move message {0} to main queue: {1}, not found.",
												   id, queue.QueueName);
										continue;
									}
									queue.MoveTo(null, message);
	                            }
                                tx.Transaction.Commit();
                            }
                        }
                        catch (Exception)
                        {
                            logger.DebugFormat(
                                "Could not move message {0} to main queue: {1}",
                                pair.Value,
                                queue.QueueName);

                            if ((CurrentTime - pair.Key).TotalMinutes >= 1.0D)
                            {
                                logger.DebugFormat("Tried to send message {0} for over a minute, giving up",
                                                   pair.Value);
                                continue;
                            }

                            writer.Add(pair.Key, messageIdCollection);
                            logger.DebugFormat("Will retry moving message {0} to main queue {1} in 1 second",
                                               pair.Value,
                                               queue.QueueName);
                        }
                    }
                }
            });
        }

        public void Dispose()
        {
            if (timeoutTimer != null)
                timeoutTimer.Dispose();
        }

        [CLSCompliant(false)]
        public void Register(Message message)
        {
            timeoutMessageIds.Write(writer =>
            {
                var timeToSend = XmlConvert.ToDateTime(message.Headers["time-to-send"], XmlDateTimeSerializationMode.Unspecified);

                logger.DebugFormat("Registering message {0} to be sent at {1} on {2}",
                                   message.Id, timeToSend, queue.QueueName);

                writer.WriteMessageId(timeToSend, message);
            });
        }
    }

	public static class OrderedListExtensions
	{
		public static void WriteMessageId(this OrderedList<DateTime, IList<int>>.Writer writer, DateTime timeToSend, Message message)
		{
			List<IList<int>> list;
			if (writer.TryGetValue(timeToSend, out list) && list.Count > 0)
			{
				list[0].Add(message.Id);
			}
			else
			{
				writer.Add(timeToSend, new List<int> { message.Id });
			}
		}
	}
}