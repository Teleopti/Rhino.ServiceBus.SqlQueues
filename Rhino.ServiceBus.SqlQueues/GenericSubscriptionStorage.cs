using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using Common.Logging;
using Rhino.ServiceBus.DataStructures;
using Rhino.ServiceBus.Exceptions;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Internal;
using Rhino.ServiceBus.MessageModules;
using Rhino.ServiceBus.Messages;
using Rhino.ServiceBus.Transport;

namespace Rhino.ServiceBus.SqlQueues
{
    public class GenericSubscriptionStorage : ISubscriptionStorage, IDisposable, IMessageModule
    {
        private const string SubscriptionsKey = "subscriptions_";

        private readonly Hashtable<string, List<WeakReference>> localInstanceSubscriptions =
            new Hashtable<string, List<WeakReference>>();

        private readonly ILog logger = LogManager.GetLogger(typeof(GenericSubscriptionStorage));

        private readonly IMessageSerializer messageSerializer;
        private readonly IReflection reflection;

        private readonly MultiValueIndexHashtable<Guid, string, Uri, int> remoteInstanceSubscriptions =
            new MultiValueIndexHashtable<Guid, string, Uri, int>();

        private readonly Hashtable<TypeAndUriKey, IList<int>> subscriptionMessageIds =
            new Hashtable<TypeAndUriKey, IList<int>>();

        private readonly Hashtable<string, HashSet<Uri>> subscriptions = new Hashtable<string, HashSet<Uri>>();
        private readonly string localEndpoint;
        
        private readonly IStorage storage;

        private bool currentlyLoadingPersistentData;
        
        public GenericSubscriptionStorage(
            IStorage storage, 
            string localEndpoint,
            IMessageSerializer messageSerializer,
            IReflection reflection)
        {
            this.storage = storage;
            this.localEndpoint = localEndpoint;
            this.messageSerializer = messageSerializer;
            this.reflection = reflection;
        }

        #region IDisposable Members

        public void Dispose()
        {
        }

        #endregion

        #region IMessageModule Members

        void IMessageModule.Init(ITransport transport, IServiceBus bus)
        {
            transport.AdministrativeMessageArrived += HandleAdministrativeMessage;
        }

        void IMessageModule.Stop(ITransport transport, IServiceBus bus)
        {
            transport.AdministrativeMessageArrived -= HandleAdministrativeMessage;
        }

        #endregion

        #region ISubscriptionStorage Members

        public void Initialize()
        {
            currentlyLoadingPersistentData = true;

            var messages = storage.GetItemsByKey(SubscriptionsKey + localEndpoint, DeserializeMessages);
            foreach (var currentMessageInformation in messages)
            {
                HandleAdministrativeMessage(currentMessageInformation);
            }
            currentlyLoadingPersistentData = false;
        }

        private object[] DeserializeMessages(SqlBinary serializedData)
        {
            object[] msgs;
            try
            {
                msgs = messageSerializer.Deserialize(new MemoryStream(serializedData.Value));
            }
            catch (Exception e)
            {
                throw new SubscriptionException("Could not deserialize message from subscription queue", e);
            }
            return msgs;
        }

        public IEnumerable<Uri> GetSubscriptionsFor(Type type)
        {
            HashSet<Uri> subscriptionForType = null;
            subscriptions.Read(reader => reader.TryGetValue(type.FullName, out subscriptionForType));
            var subscriptionsFor = subscriptionForType ?? new HashSet<Uri>();

            List<Uri> instanceSubscriptions;
            remoteInstanceSubscriptions.TryGet(type.FullName, out instanceSubscriptions);

            subscriptionsFor.UnionWith(instanceSubscriptions);

            return subscriptionsFor;
        }

        public void RemoveLocalInstanceSubscription(IMessageConsumer consumer)
        {
            var messagesConsumes = reflection.GetMessagesConsumed(consumer);
            var changed = false;
            var list = new List<WeakReference>();

            localInstanceSubscriptions.Write(writer =>
                                                 {
                                                     foreach (var type in messagesConsumes)
                                                     {
                                                         List<WeakReference> value;

                                                         if (writer.TryGetValue(type.FullName, out value) == false)
                                                             continue;
                                                         writer.Remove(type.FullName);
                                                         list.AddRange(value);
                                                     }
                                                 });

            foreach (var reference in list)
            {
                if (ReferenceEquals(reference.Target, consumer))
                    continue;

                changed = true;
            }
            if (changed)
                RaiseSubscriptionChanged();
        }

        public object[] GetInstanceSubscriptions(Type type)
        {
            List<WeakReference> value = null;

            localInstanceSubscriptions.Read(reader => reader.TryGetValue(type.FullName, out value));

            if (value == null)
                return new object[0];

            var liveInstances = value
                .Select(x => x.Target)
                .Where(x => x != null)
                .ToArray();

            if (liveInstances.Length != value.Count) //cleanup
            {
                localInstanceSubscriptions.Write(writer => value.RemoveAll(x => x.IsAlive == false));
            }

            return liveInstances;
        }

        public event Action SubscriptionChanged;

        public bool AddSubscription(string type, string endpoint)
        {
            var added = false;
            subscriptions.Write(writer =>
                                    {
                                        HashSet<Uri> subscriptionsForType;
                                        if (writer.TryGetValue(type, out subscriptionsForType) == false)
                                        {
                                            subscriptionsForType = new HashSet<Uri>();
                                            writer.Add(type, subscriptionsForType);
                                        }

                                        var uri = new Uri(endpoint);
                                        added = subscriptionsForType.Add(uri);
                                        logger.InfoFormat("Added subscription for {0} on {1}",
                                                          type, uri);
                                    });

            RaiseSubscriptionChanged();
            return added;
        }

        public void RemoveSubscription(string type, string endpoint)
        {
            var uri = new Uri(endpoint);
            RemoveSubscriptionMessageFromStorage(type, uri);

            subscriptions.Write(writer =>
                                    {
                                        HashSet<Uri> subscriptionsForType;

                                        if (writer.TryGetValue(type, out subscriptionsForType) == false)
                                        {
                                            subscriptionsForType = new HashSet<Uri>();
                                            writer.Add(type, subscriptionsForType);
                                        }

                                        subscriptionsForType.Remove(uri);

                                        logger.InfoFormat("Removed subscription for {0} on {1}",
                                                          type, endpoint);
                                    });

            RaiseSubscriptionChanged();
        }

        public void AddLocalInstanceSubscription(IMessageConsumer consumer)
        {
            localInstanceSubscriptions.Write(writer =>
                                                 {
                                                     foreach (var type in reflection.GetMessagesConsumed(consumer))
                                                     {
                                                         List<WeakReference> value;
                                                         if (writer.TryGetValue(type.FullName, out value) == false)
                                                         {
                                                             value = new List<WeakReference>();
                                                             writer.Add(type.FullName, value);
                                                         }
                                                         value.Add(new WeakReference(consumer));
                                                     }
                                                 });
            RaiseSubscriptionChanged();
        }

        #endregion

        private void AddMessageIdentifierForTracking(int messageId, string messageType, Uri uri)
        {
            subscriptionMessageIds.Write(writer =>
                                             {
                                                 var key = new TypeAndUriKey {TypeName = messageType, Uri = uri};
                                                 IList<int> value;
                                                 if (writer.TryGetValue(key, out value) == false)
                                                 {
                                                     value = new List<int>();
                                                     writer.Add(key, value);
                                                 }

                                                 value.Add(messageId);
                                             });
        }

        private void RemoveSubscriptionMessageFromStorage(string type, Uri uri)
        {
            subscriptionMessageIds.Write(writer =>
                                             {
                                                 var key = new TypeAndUriKey
                                                               {
                                                                   TypeName = type,
                                                                   Uri = uri
                                                               };
                                                 IList<int> messageIds;
                                                 if (writer.TryGetValue(key, out messageIds) == false)
                                                     return;

                                                 storage.RemoveItems(SubscriptionsKey + localEndpoint, messageIds);

                                                 writer.Remove(key);
                                             });
        }

        public bool HandleAdministrativeMessage(CurrentMessageInformation msgInfo)
        {
            var addSubscription = msgInfo.Message as AddSubscription;
            if (addSubscription != null)
            {
                return ConsumeAddSubscription(addSubscription);
            }
            var removeSubscription = msgInfo.Message as RemoveSubscription;
            if (removeSubscription != null)
            {
                return ConsumeRemoveSubscription(removeSubscription);
            }
            var addInstanceSubscription = msgInfo.Message as AddInstanceSubscription;
            if (addInstanceSubscription != null)
            {
				logger.DebugFormat("Adding instance subscription for {0} on {1}.",addInstanceSubscription.Type, addInstanceSubscription.Endpoint);
                return ConsumeAddInstanceSubscription(addInstanceSubscription);
            }
            var removeInstanceSubscription = msgInfo.Message as RemoveInstanceSubscription;
            if (removeInstanceSubscription != null)
            {
                return ConsumeRemoveInstanceSubscription(removeInstanceSubscription);
            }
            return false;
        }

        public bool ConsumeRemoveInstanceSubscription(RemoveInstanceSubscription subscription)
        {
            int msgId;
            if (remoteInstanceSubscriptions.TryRemove(subscription.InstanceSubscriptionKey, out msgId))
            {
                storage.RemoveItems(SubscriptionsKey + localEndpoint, new []{msgId});

                RaiseSubscriptionChanged();
            }
            return true;
        }

        public bool ConsumeAddInstanceSubscription(AddInstanceSubscription subscription)
        {
			if (logger.IsDebugEnabled)
			{
				logger.DebugFormat("Adding instance subscription for {0} on {1}.",subscription.InstanceSubscriptionKey,subscription.Endpoint);
			}

            int itemId = storage.AddItem(SubscriptionsKey + localEndpoint, subscription, SerializeMessage);

            remoteInstanceSubscriptions.Add(
                subscription.InstanceSubscriptionKey,
                subscription.Type,
                new Uri(subscription.Endpoint),
                itemId);

            RaiseSubscriptionChanged();
            return true;
        }

        private MemoryStream SerializeMessage<T>(T subscription) where T : class
        {
            var message = new MemoryStream();
            messageSerializer.Serialize(new[] { subscription }, message);
            return message;
        }

        public bool ConsumeRemoveSubscription(RemoveSubscription removeSubscription)
        {
            RemoveSubscription(removeSubscription.Type, removeSubscription.Endpoint.Uri.ToString());
            return true;
        }

        public bool ConsumeAddSubscription(AddSubscription addSubscription)
        {
            var newSubscription = AddSubscription(addSubscription.Type, addSubscription.Endpoint.Uri.ToString());

            if (newSubscription && currentlyLoadingPersistentData == false)
            {
                int itemId = storage.AddItem(SubscriptionsKey+localEndpoint,addSubscription,SerializeMessage);
                
                AddMessageIdentifierForTracking(
                    itemId,
                    addSubscription.Type,
                    addSubscription.Endpoint.Uri);

                return true;
            }

            return false;
        }

        private void RaiseSubscriptionChanged()
        {
            var copy = SubscriptionChanged;
            if (copy != null)
                copy();
        }
    }
}