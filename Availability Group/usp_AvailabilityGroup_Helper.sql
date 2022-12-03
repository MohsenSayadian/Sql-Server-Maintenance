USE [master]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author: Mohsen Sayadian
-- Create date: 2022-12-03
-- Description:	Availability Group Dashboard
-- Version : 1.00
-- =============================================

IF EXISTS(SELECT 1 FROM sys.procedures 
          WHERE Name = 'usp_AvailabilityGroup_Helper')
BEGIN
    DROP PROCEDURE dbo.usp_AvailabilityGroup_Helper
END

GO

USE [master]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[usp_AvailabilityGroup_Helper]
	@Dashboard bit,
	@DashboardCondition NVARCHAR(2000),
	@Health bit,
	@HealthCondition NVARCHAR(2000),
	@Log bit,
	@LogCondition NVARCHAR(2000),
	@performance NVARCHAR(200) = '00:00:00',
	@help bit = 0
	
AS
BEGIN
	SET NOCOUNT ON;

	IF(@help = 1)
	BEGIN
		PRINT 
		'
		EXEC	[dbo].[usp_AvailabilityGroup_Helper]
				@Dashboard = 1,
				@DashboardCondition = N'''',
				@Health = 1,
				@HealthCondition = N'''',
				@Log = 1,
				@LogCondition = N'''',
				@performance = N''00:00:00''
		'
	END
	
	IF(@Dashboard = 1)
	BEGIN
	
		-- shows endpoint url and sync state for ag, and dag
		SELECT ag.name AS group_name
			,ag.is_distributed
			,ar.replica_server_name AS replica_name
			,ar_state.role_desc
			,ar_state.synchronization_health
			,ar_state.recovery_health_desc
			,ar.endpoint_url
			,ar.availability_mode_desc
			,ar.failover_mode_desc
			,ar.primary_role_allow_connections_desc AS allow_connections_primary
			,ar.secondary_role_allow_connections_desc AS allow_connections_secondary
			,ar.seeding_mode_desc AS seeding_mode
		FROM sys.availability_replicas AS ar
		JOIN sys.availability_groups AS ag ON ar.group_id = ag.group_id
		JOIN sys.dm_hadr_availability_replica_states AS ar_state ON ar_state.replica_id = ar.replica_id

		IF(LEN(@DashboardCondition) > 0)
		BEGIN
			SET @DashboardCondition = ' AND ' + @DashboardCondition
		END
		BEGIN TRY

			EXEC(' SELECT * FROM (
			SELECT ag.name AS ag_name
				,ar.replica_server_name AS ag_replica_server
				,DB_NAME(dr_state.database_id) DatabaseName
				,is_ag_replica_local = CASE 
					WHEN ar_state.is_local = 1
						THEN N''LOCAL''
					ELSE ''REMOTE''
					END
				,ag_replica_role = CASE 
					WHEN ar_state.role_desc IS NULL
						THEN N''DISCONNECTED''
					ELSE ar_state.role_desc
					END
				,ar_state.synchronization_health_desc AS [Sync Status]
				,ar_state.connected_state_desc
				,ar.availability_mode_desc
				,dr_state.synchronization_state_desc
				,ar.replica_server_name
				,ar.endpoint_url
				,ar_state.last_connect_error_description
				,ar_state.last_connect_error_number
				,ar_state.last_connect_error_timestamp
				,dr_state.log_send_queue_size
				,dr_state.log_send_rate
				,dr_state.redo_queue_size
				,dr_state.redo_rate
				,dr_state.suspend_reason_desc
				,dr_state.last_sent_time
				,dr_state.last_received_time
				,dr_state.last_hardened_time
				,dr_state.last_redone_time
				,dr_state.last_commit_time
				,dr_state.secondary_lag_seconds
				,dr_state.last_commit_lsn
				,dr_state.last_hardened_lsn
				,dr_state.last_sent_lsn
			FROM (
				(
					sys.availability_groups AS ag JOIN sys.availability_replicas AS ar ON ag.group_id = ar.group_id
					) JOIN sys.dm_hadr_availability_replica_states AS ar_state ON ar.replica_id = ar_state.replica_id
				)
			JOIN sys.dm_hadr_database_replica_states dr_state ON ag.group_id = dr_state.group_id
				AND dr_state.replica_id = ar_state.replica_id 
				) AS Result
				WHERE 1 = 1 ' + @DashboardCondition)

		END TRY 
		BEGIN CATCH
			SELECT  
				ERROR_NUMBER() AS ErrorNumber  
				,ERROR_SEVERITY() AS ErrorSeverity  
				,ERROR_STATE() AS ErrorState  
				,ERROR_PROCEDURE() AS ErrorProcedure  
				,ERROR_LINE() AS ErrorLine  
				,ERROR_MESSAGE() AS ErrorMessage;  
		END CATCH


	END

	IF(@Health = 1)
	BEGIN

		IF(LEN(@HealthCondition) > 0)
		BEGIN
			SET @HealthCondition = ' AND ' + @HealthCondition
		END

		BEGIN TRY

			EXEC('
			DECLARE @FileName NVARCHAR(4000)

			SELECT @FileName = target_data.value(''(EventFileTarget/File/@name)[1]'', ''nvarchar(4000)'')
			FROM (
				SELECT TOP (100) CAST(target_data AS XML) target_data
				FROM sys.dm_xe_sessions s
				JOIN sys.dm_xe_session_targets t ON s.address = t.event_session_address
				WHERE s.name = N''AlwaysOn_health''
				) ft

			SELECT * FROM (
			SELECT XEData.value(''(event/@timestamp)[1]'', ''datetime2(3)'') AS event_timestamp
				,XEData.value(''(event/data[@name="error_number"]/value)[1]'', ''int'') AS error_number
				,XEData.value(''(event/data[@name="severity"]/value)[1]'', ''int'') AS severity
				,XEData.value(''(event/data[@name="message"]/value)[1]'', ''varchar(max)'') AS message
			FROM (
				SELECT CAST(event_data AS XML) XEData
					,*
				FROM sys.fn_xe_file_target_read_file(@FileName, NULL, NULL, NULL)
				WHERE object_name = ''error_reported''
				) event_data
			) AS Result
			WHERE 1 = 1 '+@HealthCondition+'
			ORDER BY event_timestamp DESC
			')

		END TRY 
		BEGIN CATCH
			SELECT  
				ERROR_NUMBER() AS ErrorNumber  
				,ERROR_SEVERITY() AS ErrorSeverity  
				,ERROR_STATE() AS ErrorState  
				,ERROR_PROCEDURE() AS ErrorProcedure  
				,ERROR_LINE() AS ErrorLine  
				,ERROR_MESSAGE() AS ErrorMessage;  
		END CATCH

	END

	IF(@Log = 1)
	BEGIN

		BEGIN TRY

			EXEC('
			DECLARE @start DATETIME;
			DECLARE @end DATETIME;

			SET @start = DATEADD(DAY,-5,GETDATE());
			SET @end = GETDATE();
			EXEC xp_ReadErrorLog 0, 1, N''availability'',N'''+@LogCondition+''', @start, @end, ''desc''
			')

		END TRY 
		BEGIN CATCH
			SELECT  
				ERROR_NUMBER() AS ErrorNumber  
				,ERROR_SEVERITY() AS ErrorSeverity  
				,ERROR_STATE() AS ErrorState  
				,ERROR_PROCEDURE() AS ErrorProcedure  
				,ERROR_LINE() AS ErrorLine  
				,ERROR_MESSAGE() AS ErrorMessage;  
		END CATCH

	END

	IF(@performance != '00:00:00')
	BEGIN
		Exec ('

		SELECT
		   ag.name AS group_name,
		   ag.is_distributed,
		   ar.replica_server_name AS replica_name,
		 counters.*
		 INTO #Temp
		FROM sys.availability_replicas AS ar
		JOIN sys.availability_groups AS ag
		   ON ar.group_id = ag.group_id
		CROSS APPLY (SELECT * FROM sys.dm_os_performance_counters WHERE instance_name LIKE ''%''+ar.replica_server_name+''%'') AS counters
		WAITFOR DELAY '''+@performance+'''

		SELECT
		   ag.name AS group_name,
		   ag.is_distributed,
		   ar.replica_server_name AS replica_name,
		counters.object_name ,
		counters.counter_name ,
		counters.instance_name,
		counters.cntr_value - Temp.cntr_value AS cntr_value
		FROM sys.availability_replicas AS ar
		JOIN sys.availability_groups AS ag
		   ON ar.group_id = ag.group_id
		CROSS APPLY (SELECT * FROM sys.dm_os_performance_counters WHERE instance_name LIKE ''%''+ar.replica_server_name+''%'') AS counters
		INNER JOIN #Temp Temp ON
		ag.name = Temp.group_name AND
		ag.is_distributed = Temp.is_distributed AND
		ar.replica_server_name = Temp.replica_name AND
		counters.object_name = Temp.object_name AND
		counters.counter_name = Temp.counter_name AND
		counters.instance_name = Temp.instance_name 
		')
	END

END
