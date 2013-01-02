using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using Rhino.ServiceBus.Exceptions;
using Rhino.ServiceBus.Impl;

namespace Rhino.ServiceBus.SqlQueues
{
    public class SqlStorage : IStorage
    {
        private readonly string connectionString;
    	
        public SqlStorage(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public IEnumerable<CurrentMessageInformation> GetItemsByKey(string key, Func<SqlBinary, object[]> deserializeMessages)
        {
            var messages = new List<CurrentMessageInformation>();
            using (var context = BeginTransaction())
            {
                var command = context.Connection.CreateCommand();
                command.CommandType = CommandType.StoredProcedure;
                command.CommandText = "Queue.GetItemsByKey";
                command.Transaction = context.Transaction;
                command.Parameters.AddWithValue("@Key", key);
                var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    var value = reader.GetSqlBinary(reader.GetOrdinal("Value"));

                    object[] msgs = deserializeMessages(value);

                    try
                    {
                        messages.AddRange(msgs.Select(msg => new CurrentMessageInformation
                                                                 {
                                                                     AllMessages = msgs,
                                                                     Message = msg,
                                                                 }));
                    }
                    catch (Exception e)
                    {
                        throw new SubscriptionException("Failed to process subscription records", e);
                    }
                }
                reader.Close();
                context.Transaction.Commit();
            }
            return messages;
        }

        public void RemoveItems(string key, IEnumerable<int> messageIds)
        {
            using (var context = BeginTransaction())
            {
                using (var command = context.Connection.CreateCommand())
                {
                    command.Transaction = context.Transaction;
                    command.CommandText = "Queue.RemoveItem";
                    command.CommandType = CommandType.StoredProcedure;
                	command.Parameters.AddWithValue("@Id", -1);
                    command.Parameters.AddWithValue("@Key", key);
                    foreach (var msgId in messageIds)
                    {
                        command.Parameters.RemoveAt("@Id");
                        command.Parameters.AddWithValue("@Id", msgId);
                        command.ExecuteNonQuery();
                    }
                }
                context.Transaction.Commit();
            }
        }

        public int AddItem<T>(string key, T subscription, Func<T,MemoryStream> serializeMessage)
        {
            int itemId;
            using (var context = BeginTransaction())
            {
                using (var command = context.Connection.CreateCommand())
                {
                    MemoryStream message = serializeMessage(subscription);

                    command.CommandText = "Queue.AddItem";
                    command.CommandType = CommandType.StoredProcedure;
                    command.Transaction = context.Transaction;
                    command.Parameters.AddWithValue("@Key", key);
                    command.Parameters.AddWithValue("@Value", message.ToArray());

                    itemId = (int) (decimal) command.ExecuteScalar();
                }
                context.Transaction.Commit();
            }
            return itemId;
        }

        public SqlTransactionContext BeginTransaction()
        {
            return new SqlTransactionContext(new SqlConnection(connectionString));
        }
    }
}