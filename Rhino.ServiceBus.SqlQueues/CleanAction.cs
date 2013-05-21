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
	    private int failCount;

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
				queue.Clean();
		        failCount = 0;
	        }
	        catch (SqlException exception)
	        {
		        failCount++;

				if (failCount > 3)
				{
					logger.Error("Failed to perform the clean action for three times or more.",exception);
				}
				else
				{
					logger.Warn("Failed to perform the clean action, verify that this doesn't happen regularly.", exception);
				}
	        }
        }

        public void Dispose()
        {
            if (timeoutTimer != null)
                timeoutTimer.Dispose();
        }
    }
}
