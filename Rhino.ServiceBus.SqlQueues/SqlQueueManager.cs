using System;
using System.Data;
using System.Data.SqlClient;
using Common.Logging;
using Rhino.ServiceBus.Util;

namespace Rhino.ServiceBus.SqlQueues
{
    public class SqlQueueManager : IDisposable
    {
        private readonly Uri _endpoint;
        private readonly string _connectionString;
    	private static readonly ILog Logger = LogManager.GetLogger(typeof (SqlQueueManager));

        public SqlQueueManager(Uri endpoint, string connectionString)
        {
            _endpoint = endpoint;
            _connectionString = connectionString;
        }

        public void DisposeRudely()
        {
            
        }

        public void Dispose()
        {
            
        }

        public int CreateQueue(string queueName)
        {
        	int queueId;
        	using (BeginTransaction())
            {
            	using (var command = SqlTransactionContext.Current.Connection.CreateCommand())
                {
                    command.CommandText = "Queue.CreateQueueIfMissing";
                    command.CommandType = CommandType.StoredProcedure;
                    command.Transaction = SqlTransactionContext.Current.Transaction;
                    command.Parameters.AddWithValue("@Endpoint", _endpoint.ToString());
                    command.Parameters.AddWithValue("@Queue", queueName);

                	queueId = (int) command.ExecuteScalar();
                }
                SqlTransactionContext.Current.Transaction.Commit();
			}
        	return queueId;
        }

    	public ISqlQueue GetQueue(string queueName)
        {
            return new SqlQueue(queueName, _connectionString, _endpoint);
        }

        public bool Peek(int queueId)
        {
            using (var command = SqlTransactionContext.Current.Connection.CreateCommand())
            {
                command.CommandText = "Queue.PeekMessage";
                command.CommandType = CommandType.StoredProcedure;
                command.Transaction = SqlTransactionContext.Current.Transaction;
                command.Parameters.AddWithValue("@QueueId", queueId);
                
                var reader = command.ExecuteReader();
                if (reader.HasRows)
                {
                    reader.Close();
                    return true;
                }
                reader.Close();
            }
            return false;
        }

        public Message Receive(int queueId, TimeSpan timeOut)
        {
            RawMessage raw = null;

            using (var command = SqlTransactionContext.Current.Connection.CreateCommand())
            {
                command.CommandTimeout = timeOut.Seconds;
                command.CommandText = "Queue.RecieveMessage";
                command.CommandType = CommandType.StoredProcedure;
                command.Transaction = SqlTransactionContext.Current.Transaction;
                command.Parameters.AddWithValue("@QueueId", queueId);

                var reader = command.ExecuteReader();
                var messageIdIndex = reader.GetOrdinal("MessageId");
                var createdAtIndex = reader.GetOrdinal("CreatedAt");
                var processingUntilIndex = reader.GetOrdinal("ProcessingUntil");
                var processedCountIndex = reader.GetOrdinal("ProcessedCount");
                var processedIndex = reader.GetOrdinal("Processed");
                var headersIndex = reader.GetOrdinal("Headers");
                var payloadIndex = reader.GetOrdinal("Payload");
                while (reader.Read())
                {
                    raw = new RawMessage
                              {
                                  CreatedAt = reader.GetDateTime(createdAtIndex),
                                  Headers = reader.GetString(headersIndex),
                                  MessageId = reader.GetInt64(messageIdIndex),
                                  Processed = reader.GetBoolean(processedIndex),
                                  ProcessingUntil = reader.GetDateTime(processingUntilIndex),
                                  ProcessedCount = reader.GetInt32(processedCountIndex),
                                  QueueId = queueId,
                                  SubQueueName = null
                              };
                    
                    if (!reader.IsDBNull(payloadIndex))
                        raw.Payload = reader.GetSqlBinary(payloadIndex).Value;
                }
                reader.Close();
            }
            
            if (raw==null) throw new TimeoutException();
            return raw.ToMessage();
        }

        public void Send(Uri uri, MessagePayload payload)
        {
	        using (new internalTransactionScope(this))
	        {
		        using (var command = SqlTransactionContext.Current.Connection.CreateCommand())
		        {
			        command.CommandText = "Queue.EnqueueMessage";
			        command.CommandType = CommandType.StoredProcedure;
			        command.Transaction = SqlTransactionContext.Current.Transaction;
			        command.Parameters.AddWithValue("@Endpoint", uri.ToString());
			        command.Parameters.AddWithValue("@Queue", uri.GetQueueName());
			        command.Parameters.AddWithValue("@SubQueue", DBNull.Value);

			        var contents = new RawMessage
				        {
					        CreatedAt = payload.SentAt,
					        Payload = payload.Data,
					        ProcessingUntil = payload.SentAt
				        };
			        contents.SetHeaders(payload.Headers);

			        command.Parameters.AddWithValue("@CreatedAt", contents.CreatedAt);
			        command.Parameters.AddWithValue("@Payload", contents.Payload);
			        command.Parameters.AddWithValue("@ExpiresAt", DBNull.Value);
			        command.Parameters.AddWithValue("@ProcessingUntil", contents.CreatedAt);
			        command.Parameters.AddWithValue("@Headers", contents.Headers);

			        if (Logger.IsDebugEnabled)
			        {
				        Logger.DebugFormat("Sending message to {0} on {1}. Headers are '{2}'.", uri, uri.GetQueueName(),
				                           contents.Headers);
			        }

			        command.ExecuteNonQuery();
		        }
	        }
        }

		private class internalTransactionScope : IDisposable
		{
			private SqlTransactionContext transactionToCommit;

			public internalTransactionScope(SqlQueueManager parent)
			{
				if (SqlTransactionContext.Current == null)
				{
					transactionToCommit = parent.BeginTransaction();
				}
			}

			public void Dispose()
			{
				if (transactionToCommit != null)
				{
					try
					{
						transactionToCommit.Transaction.Commit();
					}
					catch (Exception ex)
					{
						Logger.Warn("Exception encountered when trying to commit transaction.",ex);
					}
					transactionToCommit.Dispose();
					transactionToCommit = null;
				}
			}
		}

        public SqlTransactionContext BeginTransaction()
        {
            return new SqlTransactionContext(new SqlConnection(_connectionString));
        }

        public void ExtendMessageLease(Message message)
        {
            using (var command = SqlTransactionContext.Current.Connection.CreateCommand())
            {
                command.CommandText = "Queue.ExtendMessageLease";
                command.CommandType = CommandType.StoredProcedure;
                command.Transaction = SqlTransactionContext.Current.Transaction;
                command.Parameters.AddWithValue("@MessageId", message.Id);
                
                command.ExecuteNonQuery();
            }
        }

        public void MarkMessageAsReady(Message message)
        {
            using (var command = SqlTransactionContext.Current.Connection.CreateCommand())
            {
                command.CommandText = "Queue.MarkMessageAsReady";
                command.CommandType = CommandType.StoredProcedure;
                command.Transaction = SqlTransactionContext.Current.Transaction;
                command.Parameters.AddWithValue("@MessageId", message.Id);

                command.ExecuteNonQuery();
            }
        }
    }

    public class SqlTransactionContext : IDisposable
    {
        private readonly SqlConnection _connection;
        private readonly SqlTransaction _transaction;

        public SqlTransactionContext(SqlConnection connection)
        {
            if (Current!=null)
            {
                throw new InvalidOperationException("Just one open context per thread is allowed!");
            }
            _connection = connection;
            _connection.Open();
            _transaction = connection.BeginTransaction();
            Current = this;
        }

        [ThreadStatic]
        public static SqlTransactionContext Current;

        public SqlConnection Connection { get { return _connection; } }
        public SqlTransaction Transaction { get { return _transaction; } }

        public void Dispose()
        {
            Current = null;
            _transaction.Dispose();
            _connection.Dispose();
        }
    }
}