using System;
using System.Collections.Generic;
using Rhino.ServiceBus.Config;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Internal;

namespace Rhino.ServiceBus.SqlQueues.Config
{
	public class SqlQueuesOneWayBusConfigurationAware : IBusConfigurationAware
	{
		public void Configure(AbstractRhinoServiceBusConfiguration config, IBusContainerBuilder builder, IServiceLocator locator)
		{
			var oneWayConfig = config as OnewayRhinoServiceBusConfiguration;
			if (oneWayConfig == null)
				return;

			var messageOwners = new List<MessageOwner>();
			var messageOwnersReader = new MessageOwnersConfigReader(config.ConfigurationSection, messageOwners);
			messageOwnersReader.ReadMessageOwners();

			if (!messageOwnersReader.EndpointScheme.Equals("sql.queues", StringComparison.InvariantCultureIgnoreCase))
				return;

			oneWayConfig.MessageOwners = messageOwners.ToArray();
			RegisterSqlQueuesOneWay(config, builder, locator);
		}

		private void RegisterSqlQueuesOneWay(AbstractRhinoServiceBusConfiguration c, IBusContainerBuilder b, IServiceLocator l)
		{
			var oneWayConfig = (OnewayRhinoServiceBusConfiguration)c;
			var busConfig = c.ConfigurationSection.Bus;

			b.RegisterSingleton<IMessageBuilder<MessagePayload>>(() => new SqlQueuesMessageBuilder(
				l.Resolve<IMessageSerializer>(),
				l.Resolve<IServiceLocator>()));

			b.RegisterSingleton<IOnewayBus>(() => new SqlQueuesOneWayBus(
				oneWayConfig.MessageOwners,
				l.Resolve<IMessageSerializer>(),
				busConfig.Path,
				l.Resolve<IMessageBuilder<MessagePayload>>()));
		}
	}
}
