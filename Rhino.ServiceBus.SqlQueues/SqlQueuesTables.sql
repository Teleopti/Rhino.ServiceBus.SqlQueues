CREATE SCHEMA [Queue] AUTHORIZATION [dbo]
GO

IF  EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF_Messages_CreatedAt]') AND type = 'D')
BEGIN
ALTER TABLE [Queue].[Messages] DROP CONSTRAINT [DF_Messages_CreatedAt]
END

GO

IF  EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF_Messages_ProcessingUntil]') AND type = 'D')
BEGIN
ALTER TABLE [Queue].[Messages] DROP CONSTRAINT [DF_Messages_ProcessingUntil]
END

GO

IF  EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF_Messages_Processed]') AND type = 'D')
BEGIN
ALTER TABLE [Queue].[Messages] DROP CONSTRAINT [DF_Messages_Processed]
END

GO

IF  EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF_Messages_ProcessedCount]') AND type = 'D')
BEGIN
ALTER TABLE [Queue].[Messages] DROP CONSTRAINT [DF_Messages_ProcessedCount]
END

GO

IF  EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DF_Queues_Endpoint]') AND type = 'D')
BEGIN
ALTER TABLE [Queue].[Queues] DROP CONSTRAINT [DF_Queues_Endpoint]
END

GO

/****** Object:  Table [Queue].[Messages]    Script Date: 06/26/2012 08:45:11 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Queue].[Messages]') AND type in (N'U'))
DROP TABLE [Queue].[Messages]
GO

/****** Object:  Table [Queue].[Queues]    Script Date: 06/26/2012 08:45:11 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Queue].[Queues]') AND type in (N'U'))
DROP TABLE [Queue].[Queues]
GO

/****** Object:  Table [Queue].[SubscriptionStorage]    Script Date: 06/26/2012 08:45:11 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Queue].[SubscriptionStorage]') AND type in (N'U'))
DROP TABLE [Queue].[SubscriptionStorage]
GO

CREATE TABLE [Queue].[Messages](
	[MessageId] [int] IDENTITY(1,1) NOT NULL,
	[QueueId] [int] NOT NULL,
	[CreatedAt] [datetime] NOT NULL,
	[ProcessingUntil] [datetime] NOT NULL,
	[ExpiresAt] [datetime] NULL,
	[Processed] [bit] NOT NULL,
	[Headers] [nvarchar](2000) NULL,
	[Payload] [varbinary](max) NULL,
	[ProcessedCount] [int] NOT NULL,
 CONSTRAINT [PK_Messages] PRIMARY KEY CLUSTERED 
(
	[MessageId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

CREATE TABLE [Queue].[Queues](
	[QueueName] [nvarchar](50) NOT NULL,
	[QueueId] [int] IDENTITY(1,1) NOT NULL,
	[ParentQueueId] [int] NULL,
	[Endpoint] [nvarchar](250) NOT NULL,
 CONSTRAINT [PK_Queues_1] PRIMARY KEY CLUSTERED 
(
	[QueueId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

CREATE TABLE [Queue].[SubscriptionStorage](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Key] [nvarchar](250) NOT NULL,
	[Value] [varbinary](max) NOT NULL,
 CONSTRAINT [PK_SubscriptionStorage1] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

ALTER TABLE [Queue].[Messages] ADD  CONSTRAINT [DF_Messages_CreatedAt]  DEFAULT (getdate()) FOR [CreatedAt]
GO

ALTER TABLE [Queue].[Messages] ADD  CONSTRAINT [DF_Messages_ProcessingUntil]  DEFAULT (getdate()) FOR [ProcessingUntil]
GO

ALTER TABLE [Queue].[Messages] ADD  CONSTRAINT [DF_Messages_Processed]  DEFAULT ((0)) FOR [Processed]
GO

ALTER TABLE [Queue].[Messages] ADD  CONSTRAINT [DF_Messages_ProcessedCount]  DEFAULT ((1)) FOR [ProcessedCount]
GO

ALTER TABLE [Queue].[Queues] ADD  CONSTRAINT [DF_Queues_Endpoint]  DEFAULT ('') FOR [Endpoint]
GO
