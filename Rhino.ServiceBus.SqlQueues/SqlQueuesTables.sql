IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'Queue')
EXEC sys.sp_executesql N'CREATE SCHEMA [Queue]'
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Queue].[Messages]') AND type in (N'U'))
DROP TABLE [Queue].[Messages]
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Queue].[Queues]') AND type in (N'U'))
DROP TABLE [Queue].[Queues]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Queue].[SubscriptionStorage]') AND type in (N'U'))
DROP TABLE [Queue].[SubscriptionStorage]
GO

CREATE TABLE [Queue].[Messages](
	[MessageId] [bigint] IDENTITY(1,1) NOT NULL,
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
 CONSTRAINT [PK_Queues] PRIMARY KEY CLUSTERED 
(
	[QueueId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

--Add index to support PeekMessage and ReciveMessage
CREATE NONCLUSTERED INDEX [IX_Message_QueueId_Processed_ProcessingUntil] ON [Queue].[Messages] 
(
	[QueueId] ASC,
	[Processed] ASC,
	[ProcessingUntil] ASC
)
INCLUDE (
	[CreatedAt],
	[ExpiresAt]
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
ALTER TABLE [Queue].[Messages] ADD  CONSTRAINT [DF_Messages_ProcessingUntil]  DEFAULT (getdate()) FOR [ProcessingUntil]
ALTER TABLE [Queue].[Messages] ADD  CONSTRAINT [DF_Messages_Processed]  DEFAULT ((0)) FOR [Processed]
ALTER TABLE [Queue].[Messages] ADD  CONSTRAINT [DF_Messages_ProcessedCount]  DEFAULT ((1)) FOR [ProcessedCount]
GO

ALTER TABLE [Queue].[Queues] ADD  CONSTRAINT [DF_Queues_Endpoint]  DEFAULT ('') FOR [Endpoint]
