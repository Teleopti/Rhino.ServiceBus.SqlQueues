using System;
using System.Data.SqlClient;
using System.Threading;
using Common.Logging;

namespace Rhino.ServiceBus.SqlQueues
{
    public class CleanAction : IDisposable
    {
        private ISqlQueue queue;
        private Timer timeoutTimer;
		private readonly ILog logger = LogManager.GetLogger(typeof(CleanAction));
	    private int numberOfItemsToDelete = DefaultNumberOfItemsToDelete;
	    private const int DefaultNumberOfItemsToDelete = 1000000;

        [CLSCompliant(false)]
        public CleanAction(ISqlQueue queue)
        {
            this.queue = queue;
            timeoutTimer = new Timer(OnTimeoutCallback, null, TimeSpan.FromSeconds(0), TimeSpan.FromHours(1));
        }

        private void OnTimeoutCallback(object state)
        {
			try
			{
				logger.DebugFormat("Performing the clean action with {0} limit.", numberOfItemsToDelete);
				queue.Clean(numberOfItemsToDelete);
				numberOfItemsToDelete = DefaultNumberOfItemsToDelete;
			}
			catch (InvalidOperationException exception)
			{
				HandleException(exception);
			}
			catch (SqlException exception)
			{
				HandleException(exception);
			}
        }

	    private void HandleException(Exception exception)
		{
			var newDeleteAttemptTarget = (int)Math.Sqrt(numberOfItemsToDelete);
			logger.WarnFormat("Failed to perform the clean action with {0} limit. Trying with {1} instead.", exception, numberOfItemsToDelete, newDeleteAttemptTarget);
		    numberOfItemsToDelete = newDeleteAttemptTarget;
	    }

	    public void Dispose()
        {
            if (timeoutTimer != null)
                timeoutTimer.Dispose();
        }
    }
}
