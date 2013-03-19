using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Rhino.ServiceBus.SqlQueues
{
    public class CleanAction : IDisposable
    {
        private ISqlQueue queue;
        private Timer timeoutTimer;

        [CLSCompliant(false)]
        public CleanAction(ISqlQueue queue)
        {
            this.queue = queue;
            timeoutTimer = new Timer(OnTimeoutCallback, null, TimeSpan.FromSeconds(0), TimeSpan.FromHours(1));
        }

        private void OnTimeoutCallback(object state)
        {
            queue.Clean();
        }

        public void Dispose()
        {
            if (timeoutTimer != null)
                timeoutTimer.Dispose();
        }
    }
}
