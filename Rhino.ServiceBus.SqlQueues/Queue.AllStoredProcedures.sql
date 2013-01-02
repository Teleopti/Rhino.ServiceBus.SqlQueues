----
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Queue].[AddItem]') AND type in (N'P', N'PC'))
DROP PROCEDURE [Queue].[AddItem]
GO
CREATE PROCEDURE [Queue].[AddItem]
	@Key nvarchar(250),
	@Value varbinary(max)
AS
BEGIN
	SET NOCOUNT ON;

    INSERT INTO Queue.SubscriptionStorage ([Key],Value) VALUES (@Key,@Value)
    SELECT SCOPE_IDENTITY();
END
GO
----
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Queue].[CreateQueueIfMissing]') AND type in (N'P', N'PC'))
DROP PROCEDURE [Queue].[CreateQueueIfMissing]
GO
CREATE PROCEDURE [Queue].[CreateQueueIfMissing]
	@Queue nvarchar(50),
	@Endpoint nvarchar(250)
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @QueueId int
	
			SELECT @QueueId = QueueId
			FROM Queue.Queues
			WHERE QueueName = @Queue
			AND Endpoint=@Endpoint;
			
			if (@QueueId is null)
				BEGIN
					INSERT INTO Queue.Queues (QueueName,Endpoint) VALUES (@Queue,@Endpoint)
					SELECT @QueueId = SCOPE_IDENTITY()
				END

		SELECT @QueueId
END
GO
----
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Queue].[EnqueueMessage]') AND type in (N'P', N'PC'))
DROP PROCEDURE [Queue].[EnqueueMessage]
GO
CREATE PROCEDURE [Queue].[EnqueueMessage]
	@Endpoint nvarchar(250),
	@Queue nvarchar(50),
	@Subqueue nvarchar(50),
	@Payload varbinary(MAX),
	@Headers nvarchar(2000),
	@ProcessingUntil datetime,
	@CreatedAt datetime,
	@ExpiresAt datetime
AS
BEGIN
	SET NOCOUNT ON;

    DECLARE @QueueId int;
    
    EXEC Queue.GetAndAddQueue @Endpoint,@Queue,@Subqueue,@QueueId=@QueueId OUTPUT;
		
	INSERT INTO Queue.Messages (QueueId,Payload,ProcessingUntil,ExpiresAt,Processed,Headers,CreatedAt) VALUES (@QueueId,@Payload,ISNULL(@ProcessingUntil,GetDate()),@ExpiresAt,0,@Headers,@CreatedAt)
END
GO
----
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Queue].[ExtendMessageLease]') AND type in (N'P', N'PC'))
DROP PROCEDURE [Queue].[ExtendMessageLease]
GO
CREATE PROCEDURE [Queue].[ExtendMessageLease]
	@MessageId int
AS
BEGIN
	SET NOCOUNT ON;

	UPDATE Queue.Messages
	SET ProcessingUntil = DateAdd(mi,10,GetDate())
	WHERE MessageId=@MessageId
END
GO
----
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Queue].[GetAndAddQueue]') AND type in (N'P', N'PC'))
DROP PROCEDURE [Queue].[GetAndAddQueue]
GO
CREATE PROCEDURE [Queue].[GetAndAddQueue]
	@Endpoint nvarchar(250),
	@Queue nvarchar(50),
	@Subqueue nvarchar(50),
	@QueueId int OUTPUT
AS
BEGIN
	SET NOCOUNT ON;

	SELECT @QueueId = QueueId FROM Queue.Queues WHERE QueueName = @Queue AND Endpoint = @Endpoint;
	if (@QueueId is null)
		BEGIN
			INSERT INTO Queue.Queues (QueueName,Endpoint) VALUES (@Queue,@Endpoint)
			SELECT @QueueId = SCOPE_IDENTITY()
		END
		
	IF (@Subqueue is not null)
		BEGIN
			DECLARE @SubqueueId AS int
			
			SELECT @SubqueueId = s.QueueID FROM Queue.Queues p INNER JOIN Queue.Queues s ON p.QueueId = s.ParentQueueId WHERE p.QueueName = @Queue AND p.Endpoint = @Endpoint AND s.QueueName = @Subqueue
			if (@SubqueueId is null)
				BEGIN
					INSERT INTO Queue.Queues (QueueName,ParentQueueId,Endpoint) VALUES (@Subqueue,@QueueId,@Endpoint)
					SELECT @SubqueueId = SCOPE_IDENTITY()
				END
				
			SET @QueueId = @SubqueueId
		END

	SET NOCOUNT OFF;
	RETURN @QueueId
END
GO
----
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Queue].[GetItemsByKey]') AND type in (N'P', N'PC'))
DROP PROCEDURE [Queue].[GetItemsByKey]
GO
CREATE PROCEDURE [Queue].[GetItemsByKey]
	@Key nvarchar(250)
AS
BEGIN
	SET NOCOUNT ON;

    SELECT *
    FROM Queue.SubscriptionStorage
    WHERE ([Key]=@Key)
    
    DELETE FROM Queue.SubscriptionStorage
    WHERE ([Key]=@Key)
END
GO
----
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Queue].[MarkMessageAsReady]') AND type in (N'P', N'PC'))
DROP PROCEDURE [Queue].[MarkMessageAsReady]
GO
CREATE PROCEDURE [Queue].[MarkMessageAsReady]
	@MessageId int
AS
BEGIN
	SET NOCOUNT ON;

--Debug
	/*
	UPDATE Queue.Messages
	SET Processed = 1
	WHERE MessageId = @MessageId
	*/
	DELETE Queue.Messages
	WHERE MessageId = @MessageId
END
GO
----
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Queue].[MoveMessage]') AND type in (N'P', N'PC'))
DROP PROCEDURE [Queue].[MoveMessage]
GO
CREATE PROCEDURE [Queue].[MoveMessage]
	@Endpoint nvarchar(250),
	@Queue nvarchar(50),
	@Subqueue nvarchar(50),
	@MessageId int
AS
BEGIN
	SET NOCOUNT ON;

    DECLARE @QueueId int;
        
    EXEC Queue.GetAndAddQueue @Endpoint,@Queue,@Subqueue,@QueueId=@QueueId OUTPUT;

	UPDATE Queue.Messages
	SET QueueId = @QueueId
	WHERE MessageId=@MessageId
END
GO
----
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Queue].[PeekMessage]') AND type in (N'P', N'PC'))
DROP PROCEDURE [Queue].[PeekMessage]
GO
CREATE PROCEDURE [Queue].[PeekMessage]
	@QueueId int
AS
BEGIN
	SET NOCOUNT ON;
	
	SELECT TOP 1 *
	FROM Queue.Messages
	WHERE isnull(ExpiresAt,DATEADD(mi,1,GetDate())) > GetDate()
	AND Processed=0
	AND ProcessingUntil<GetDate()
	AND QueueId = @QueueId
	ORDER BY CreatedAt ASC
END
GO
----
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Queue].[PeekMessageById]') AND type in (N'P', N'PC'))
DROP PROCEDURE [Queue].[PeekMessageById]
GO
CREATE PROCEDURE [Queue].[PeekMessageById]
	@MessageId int
AS
BEGIN
	SET NOCOUNT ON;

    SELECT
		m.*,
		q.QueueName SubQueueName
    FROM Queue.Messages m
    LEFT JOIN Queue.Queues q
		ON m.QueueId=q.QueueId
		AND q.ParentQueueId IS NOT NULL
	WHERE isnull(m.ExpiresAt,DATEADD(mi,1,GetDate())) > GetDate()
	AND m.Processed=0
	AND m.ProcessingUntil<GetDate()
	AND m.MessageId=@MessageId
	ORDER BY CreatedAt ASC
END
GO
----
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Queue].[RecieveMessage]') AND type in (N'P', N'PC'))
DROP PROCEDURE [Queue].[RecieveMessage]
GO
CREATE PROCEDURE [Queue].[RecieveMessage]
	@QueueId int
AS
BEGIN
	SET NOCOUNT ON;
	
	DECLARE @MessageId int;
	SELECT TOP 1 @MessageId = MessageId
	FROM Queue.Messages
	WHERE isnull(ExpiresAt,DATEADD(mi,1,GetDate())) > GetDate()
	AND Processed=0
	AND ProcessingUntil<GetDate()
	AND QueueId=@QueueId
	ORDER BY CreatedAt ASC
	
	if (@MessageId is not null)
		BEGIN
			UPDATE Queue.Messages
			SET ProcessingUntil = DateAdd(mi,1,GetDate()),
			ProcessedCount=ProcessedCount+1
			WHERE MessageId=@MessageId
			
			SELECT *
			FROM Queue.Messages
			WHERE MessageId=@MessageId
		END
	else
		BEGIN
			SELECT TOP 0 *
			FROM Queue.Messages;
		END
END
GO
----
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Queue].[RecieveMessages]') AND type in (N'P', N'PC'))
DROP PROCEDURE [Queue].[RecieveMessages]
GO
CREATE PROCEDURE [Queue].[RecieveMessages]
	@Endpoint nvarchar(250),
	@Queue nvarchar(50),
	@Subqueue nvarchar(50)
AS
BEGIN
	SET NOCOUNT ON;

    DECLARE @QueueId int;
        
    EXEC Queue.GetAndAddQueue @Endpoint,@Queue,@Subqueue,@QueueId=@QueueId OUTPUT;
	
			UPDATE Queue.Messages
			SET ProcessingUntil = DateAdd(mi,10,GetDate()),
			ProcessedCount=ProcessedCount+1
			WHERE QueueId = @QueueId
			
			SELECT *
			FROM Queue.Messages
			WHERE QueueId = @QueueId
END
GO
----
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Queue].[RemoveItem]') AND type in (N'P', N'PC'))
DROP PROCEDURE [Queue].[RemoveItem]
GO
CREATE PROCEDURE [Queue].[RemoveItem]
	@Key nvarchar(250),
	@Id int
AS
BEGIN
	SET NOCOUNT ON;

    DELETE FROM Queue.SubscriptionStorage
    WHERE ([Key]=@Key)
    AND (Id=@Id)
END
GO
----