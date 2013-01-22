using System;
using System.Reflection;
using Rhino.ServiceBus.Hosting;
using Rhino.ServiceBus.SqlQueues.Config;

namespace Rhino.ServiceBus.SqlQueues.Hosting
{
	[Serializable]
	public class SqlRemoteAppDomainHost : RemoteAppDomainHost
	{
		private string _connectionString;

		public SqlRemoteAppDomainHost(Assembly assembly, string configuration) : base(assembly, configuration)
		{
		}

		public SqlRemoteAppDomainHost(string assemblyPath, Type boosterType) : base(assemblyPath, boosterType)
		{
		}

		public SqlRemoteAppDomainHost(Type boosterType) : base(boosterType)
		{
		}

		public SqlRemoteAppDomainHost(string assemblyPath, string configuration) : base(assemblyPath, configuration)
		{
		}

		public void SetConnectionString(string connectionString)
		{
			_connectionString = connectionString;
		}

		protected override HostedService CreateRemoteHost(AppDomain appDomain)
		{
			appDomain.SetData("ConnectionString",_connectionString);
			appDomain.DoCallBack(SetConnectionStringInAppDomain);
			var service = base.CreateRemoteHost(appDomain);
			return service;
		}

		private static void SetConnectionStringInAppDomain()
		{
			QueueConnectionStringContainer.ConnectionString = (string)AppDomain.CurrentDomain.GetData("ConnectionString");
		}
	}
}
