using System;
using Common.Logging;
using Rhino.ServiceBus.Impl;

namespace Rhino.ServiceBus.SqlQueues
{
	public class SqlMessageHandlingCompletion
	{
		private readonly Action sendMessageBackToQueue;
		private readonly Action<CurrentMessageInformation, Exception> messageCompleted;
        private readonly Action<CurrentMessageInformation> beforeTransactionCommit;
        private readonly Action<CurrentMessageInformation> beforeTransactionRollback;
		private readonly ILog logger;
		private readonly Action<CurrentMessageInformation, Exception> messageProcessingFailure;
        private readonly CurrentMessageInformation currentMessageInformation;
        private readonly SqlQueueManager sqlQueueManager;

		private Exception exception;

	    public SqlMessageHandlingCompletion(SqlQueueManager sqlQueueManager, Action sendMessageBackToQueue, Exception exception, Action<CurrentMessageInformation, Exception> messageCompleted, Action<CurrentMessageInformation> beforeTransactionCommit, Action<CurrentMessageInformation> beforeTransactionRollback, ILog logger, Action<CurrentMessageInformation, Exception> messageProcessingFailure, CurrentMessageInformation currentMessageInformation)
	    {
	        this.sqlQueueManager = sqlQueueManager;
			this.sendMessageBackToQueue = sendMessageBackToQueue;
			this.exception = exception;
			this.messageCompleted = messageCompleted;
			this.beforeTransactionCommit = beforeTransactionCommit;
            this.beforeTransactionRollback = beforeTransactionRollback;
			this.logger = logger;
			this.messageProcessingFailure = messageProcessingFailure;
			this.currentMessageInformation = currentMessageInformation;
		}

		public void HandleMessageCompletion()
		{
			try
			{
				if (SuccessfulCompletion())
					return;
			}
			finally
			{
				DisposeTransactionIfNotAlreadyDisposed();				
			}

			//error

			NotifyMessageCompleted();

			NotifyAboutMessageProcessingFailure();

			SendMessageBackToQueue();
		}

		private void SendMessageBackToQueue()
		{
			if (sendMessageBackToQueue != null)
				sendMessageBackToQueue();
		}

		private void NotifyMessageCompleted()
		{
			try
			{
				if (messageCompleted != null)
					messageCompleted(currentMessageInformation, exception);
			}
			catch (Exception e)
			{
				logger.Error("An error occured when raising the MessageCompleted event, the error will NOT affect the message processing", e);
			}
		}

		private void NotifyAboutMessageProcessingFailure()
		{
			try
			{
				if (messageProcessingFailure != null)
					messageProcessingFailure(currentMessageInformation, exception);
			}
			catch (Exception moduleException)
			{
				logger.Error("Module failed to process message failure: " + exception.Message,
				             moduleException);
			}
		}

		private void DisposeTransactionIfNotAlreadyDisposed()
		{
			try
			{
                    if (beforeTransactionRollback != null)
                        beforeTransactionRollback(currentMessageInformation);
			}
			catch (Exception e)
			{
				logger.Warn("Failed to dispose of transaction in error mode.", e);
			}
		}

        private bool SuccessfulCompletion()
        {
            if (exception != null)
                return false;
            try
            {
                if (beforeTransactionCommit != null)
                    beforeTransactionCommit(currentMessageInformation);

                using (var tx = sqlQueueManager.BeginTransaction())
                {
                    sqlQueueManager.MarkMessageAsReady(((SqlQueueCurrentMessageInformation) currentMessageInformation).TransportMessage);
                    tx.Transaction.Commit();
                }

                NotifyMessageCompleted();
                return true;
            }
            catch (Exception e)
            {
                logger.Warn("Failed to complete transaction, moving to error mode", e);
                exception = e;
            }
            return false;
        }
	}
}