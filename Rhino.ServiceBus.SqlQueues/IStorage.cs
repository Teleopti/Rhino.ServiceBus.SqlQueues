using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.IO;
using Rhino.ServiceBus.Impl;

namespace Rhino.ServiceBus.SqlQueues
{
    public interface IStorage
    {
        IEnumerable<CurrentMessageInformation> GetItemsByKey(string key, Func<SqlBinary,object[]> deserializeMessages);
        void RemoveItems(string key, IEnumerable<int> messageIds);
        int AddItem<T>(string key, T subscription, Func<T,MemoryStream> serializeMessage);
    }
}