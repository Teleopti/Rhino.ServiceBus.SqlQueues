namespace Rhino.ServiceBus.SqlQueues
{
    public interface ISqlQueue : IQueue
    {
        SqlTransactionContext BeginTransaction();
    }
}