SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author: Mohsen Sayadian
-- Create date: 2022-06-05
-- Description:	Restore From Directories
-- Version : 1.00
-- =============================================

IF EXISTS(SELECT 1 FROM sys.procedures 
          WHERE Name = 'usp_RestoreFromDirectories')
BEGIN
    DROP PROCEDURE dbo.usp_RestoreFromDirectories
END

GO

USE [master]
GO
/****** Object:  StoredProcedure [dbo].[usp_RestoreFromDirectories]    Script Date: 6/5/2022 1:35:05 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[usp_RestoreFromDirectories]
	@DBName sysname,
	@RestorePointTime Datetime,
	@BackupPath NVARCHAR(2000),
	@RestoreDBName sysname,
	@RestoreDataPath NVARCHAR(2000),
	@RestoreLogPath NVARCHAR(2000),
	@RestoreExtraParmeters NVARCHAR(2000)  = '',
	@Execute bit  = 0,
	@SingleUser bit  = 0
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @Cmd NVARCHAR(2000) 
	DECLARE @FileList TABLE (Id int NOT NULL IDENTITY(1,1),BackupFile NVARCHAR(2000)) 

	SET @Cmd = 'DIR /a:-D /S /b "' + @backupPath + '"'

	INSERT INTO @FileList(BackupFile) 
	EXEC master.sys.xp_cmdshell @cmd 

	DECLARE @BackupFiles TABLE ( [LogicalName] NVARCHAR(128), [PhysicalName] NVARCHAR(260), [Type] CHAR(1), [FileGroupName] NVARCHAR(128), [Size] NUMERIC(20, 0), [MaxSize] NUMERIC(20, 0), [FileID] BIGINT, [CreateLSN] NUMERIC(25, 0), [DropLSN] NUMERIC(25, 0), [UniqueID] UNIQUEIDENTIFIER, [ReadOnlyLSN] NUMERIC(25, 0), [ReadWriteLSN] NUMERIC(25, 0), [BackupSizeInBytes] BIGINT, [SourceBlockSize] INT, [FileGroupID] INT, [LogGroupGUID] UNIQUEIDENTIFIER, [DifferentialBaseLSN] NUMERIC(25, 0), [DifferentialBaseGUID] UNIQUEIDENTIFIER, [IsReadOnly] BIT, [IsPresent] BIT, [TDEThumbprint] VARBINARY(32), [SnapshotURL] NVARCHAR(360))

	DECLARE @BackupInfo TABLE  (Id INT NOT NULL IDENTITY(1, 1), BackupName NVARCHAR(128), BackupDescription NVARCHAR(255), BackupType SMALLINT, ExpirationDate DATETIME, Compressed BIT, Position SMALLINT, DeviceType TINYINT, UserName NVARCHAR(128), ServerName NVARCHAR(128), DatabaseName NVARCHAR(128), DatabaseVersion INT, DatabaseCreationDate DATETIME, BackupSize NUMERIC(20, 0), FirstLSN NUMERIC(25, 0), LastLSN NUMERIC(25, 0), CheckpointLSN NUMERIC(25, 0), DatabaseBackupLSN NUMERIC(25, 0), BackupStartDate DATETIME, BackupFinishDate DATETIME, SortOrder SMALLINT, [CodePage] SMALLINT, UnicodeLocaleId INT, UnicodeComparisonStyle INT, CompatibilityLevel TINYINT, SoftwareVendorId INT, SoftwareVersionMajor INT, SoftwareVersionMinor INT, SoftwareVersionBuild INT, MachineName NVARCHAR(128), Flags INT, BindingId UNIQUEIDENTIFIER, RecoveryForkId UNIQUEIDENTIFIER, Collation NVARCHAR(128), FamilyGUID UNIQUEIDENTIFIER, HasBulkLoggedData BIT, IsSnapshot BIT, IsReadOnly BIT, IsSingleUser BIT, HasBackupChecksums BIT, IsDamaged BIT, BeginsLogChain BIT, HasIncompleteMetaData BIT, IsForceOffline BIT, IsCopyOnly BIT, 
		FirstRecoveryForkID UNIQUEIDENTIFIER, ForkPointLSN NUMERIC(25, 0), RecoveryModel NVARCHAR(60), DifferentialBaseLSN NUMERIC(25, 0), DifferentialBaseGUID UNIQUEIDENTIFIER, BackupTypeDescription NVARCHAR(60), BackupSetGUID UNIQUEIDENTIFIER, CompressedBackupSize BIGINT, Containment TINYINT, KeyAlgorithm NVARCHAR(32), EncryptorThumbprint VARBINARY(20), EncryptorType NVARCHAR(32), BackupFile NVARCHAR(2000))
	
	DECLARE @Index int = (SELECT COUNT(*) FROM @FileList);
	DECLARE @FilePath NVARCHAR(2000) ;
	DECLARE @SqlHeader NVARCHAR(4000)
	
	WHILE (@Index > 0)
	BEGIN
		 BEGIN TRY
			
			SET @FilePath = (SELECT BackupFile FROM @FileList WHERE Id = @Index)
			SET @SqlHeader = N'RESTORE HEADERONLY FROM DISK = ''' + @FilePath + ''''
			
			INSERT INTO @BackupInfo (BackupName, BackupDescription, BackupType, ExpirationDate, Compressed, Position, DeviceType, UserName, ServerName, DatabaseName, DatabaseVersion, DatabaseCreationDate, BackupSize, FirstLSN, LastLSN, CheckpointLSN, DatabaseBackupLSN, BackupStartDate, BackupFinishDate, SortOrder, [CodePage], UnicodeLocaleId, UnicodeComparisonStyle, CompatibilityLevel, SoftwareVendorId, SoftwareVersionMajor, SoftwareVersionMinor, SoftwareVersionBuild, MachineName, Flags, BindingId, RecoveryForkId, Collation, FamilyGUID, HasBulkLoggedData, IsSnapshot, IsReadOnly, IsSingleUser, HasBackupChecksums, IsDamaged, BeginsLogChain, HasIncompleteMetaData, IsForceOffline, IsCopyOnly, FirstRecoveryForkID, ForkPointLSN, RecoveryModel, DifferentialBaseLSN, DifferentialBaseGUID, BackupTypeDescription, BackupSetGUID, CompressedBackupSize, Containment, KeyAlgorithm, EncryptorThumbprint, EncryptorType)
			EXEC (@SqlHeader)

			UPDATE @BackupInfo SET BackupFile = @FilePath WHERE Id = SCOPE_IDENTITY()

		END TRY
		BEGIN CATCH
			
			SET @SqlHeader = (SELECT BackupFile FROM @FileList WHERE Id = @Index)
			Print '-- Error File > ' + @SqlHeader

		END CATCH

		SET @Index = @Index - 1
	END

	IF(@Execute = 1 AND @SingleUser = 1) EXEC('ALTER DATABASE ['+@DBName+'] SET SINGLE_USER WITH ROLLBACK IMMEDIATE');

	Print CHAR(13) + '--- Restore List -------------------------------------------------------------------'

	--Firts Full Backup
	DECLARE @FisrtFullBackupDateTime Datetime 
	DECLARE @BackupSetGUID UNIQUEIDENTIFIER

	SELECT TOP(1) @FisrtFullBackupDateTime = BackupStartDate ,@BackupSetGUID = BackupSetGUID  FROM @BackupInfo 
	WHERE BackupType = 1 AND DatabaseName = @DBName AND BackupFinishDate <= @RestorePointTime AND BackupTypeDescription IN ('DATABASE')
	ORDER BY BackupStartDate DESC

	DELETE  FROM @BackupInfo 
	WHERE DatabaseName = @DBName 
	AND BackupFinishDate <= @RestorePointTime 
	AND BackupTypeDescription IN ('DATABASE DIFFERENTIAL','TRANSACTION LOG')
	AND  BackupStartDate = @FisrtFullBackupDateTime

	DECLARE @BackupFile Nvarchar(2000) = ''
	DECLARE @BackupTypeDescription varchar(50) = ''
	DECLARE @SqlBackupHeader Nvarchar(2000) = ''

	SELECT  @SqlBackupHeader = @SqlBackupHeader + 'DISK = '''+BackupFile+''' ,' ,@BackupFile = BackupFile ,@BackupTypeDescription = BackupTypeDescription FROM @BackupInfo 
	WHERE BackupSetGUID = @BackupSetGUID

	SET @SqlBackupHeader =  
	'RESTORE '+CASE WHEN @BackupTypeDescription = 'TRANSACTION LOG' THEN 'LOG' ELSE 'DATABASE' END +' ['+@RestoreDBName+'] FROM '+ substring(@SqlBackupHeader, 1, (len(@SqlBackupHeader) - 1)) + ' WITH ' 

	INSERT INTO @BackupFiles ([LogicalName], [PhysicalName], [Type], [FileGroupName], [Size], [MaxSize], [FileID], [CreateLSN], [DropLSN], [UniqueId], [ReadonlyLSN], [ReadWriteLSN], [BackupSizeInBytes], [SourceBlockSize], [FileGroupId], [LogGroupGUID], [DifferentialBaseLSN], [DifferentialBaseGUID], [IsReadOnly], [IsPresent],[TDEThumbprint],[SnapshotURL])
	EXEC ('RESTORE FILELISTONLY FROM DISK = ''' + @BackupFile + '''')	

	Declare @SqlBackupMove Nvarchar(2000) = ''
	SELECT  @SqlBackupMove = 
			@SqlBackupMove + 'MOVE N'''+LogicalName+''' TO N'''+
			CASE WHEN Type = 'D' 
			THEN @RestoreDataPath + RIGHT([PhysicalName], CHARINDEX('\', REVERSE([PhysicalName])) -1)  
			ELSE @RestoreLogPath + RIGHT([PhysicalName], CHARINDEX('\', REVERSE([PhysicalName])) -1)   END 
			+''' ,'  
	FROM @BackupFiles
	DELETE @BackupFiles

	SET @SqlBackupMove = @SqlBackupMove + ' NORECOVERY ,NOUNLOAD,  STATS = 20 {Parameters} ' 
	SET @SqlBackupMove = REPLACE(@SqlBackupMove ,'{Parameters}' , @RestoreExtraParmeters)

	Print @SqlBackupHeader + @SqlBackupMove
	IF(@Execute = 1) EXEC( @SqlBackupHeader + @SqlBackupMove)

	--Last Diff Backup and log chain

	SELECT TOP(1) @FisrtFullBackupDateTime = BackupStartDate  FROM @BackupInfo 
	WHERE BackupType = 5 
	AND DatabaseName = @DBName 
	AND BackupFinishDate <= @RestorePointTime 
	AND BackupStartDate >= @FisrtFullBackupDateTime
	AND BackupTypeDescription IN ('DATABASE DIFFERENTIAL')
	ORDER BY BackupStartDate DESC

	DELETE FROM @BackupInfo 
	WHERE DatabaseName = @DBName 
	AND BackupFinishDate <= @RestorePointTime 
	AND BackupTypeDescription IN ('TRANSACTION LOG')
	AND BackupStartDate = @FisrtFullBackupDateTime


	DECLARE contact_cursor CURSOR FOR  
	SELECT BackupSetGUID     
	FROM @BackupInfo 
	WHERE BackupFinishDate <= @RestorePointTime 
		AND BackupTypeDescription IN ('DATABASE DIFFERENTIAL','TRANSACTION LOG')
		AND BackupStartDate >= @FisrtFullBackupDateTime
		AND DatabaseName = @DBName
	GROUP BY BackupStartDate ,BackupSetGUID
	ORDER BY BackupStartDate ,BackupSetGUID  
  
	OPEN contact_cursor;  
  
	FETCH NEXT FROM contact_cursor INTO @BackupSetGUID ;  
 
	WHILE @@FETCH_STATUS = 0  
	BEGIN  
		
		SET @BackupFile  = ''
		SET @BackupTypeDescription  = ''
		SET @SqlBackupHeader  = ''

		SELECT  @SqlBackupHeader = @SqlBackupHeader + 'DISK = '''+BackupFile+''' ,' ,@BackupFile = BackupFile ,@BackupTypeDescription = BackupTypeDescription FROM @BackupInfo 
		WHERE BackupSetGUID = @BackupSetGUID

		SET @SqlBackupHeader =  
		'RESTORE '+CASE WHEN @BackupTypeDescription = 'TRANSACTION LOG' THEN 'LOG' ELSE 'DATABASE' END +' ['+@RestoreDBName+'] FROM '+ substring(@SqlBackupHeader, 1, (len(@SqlBackupHeader) - 1)) + ' WITH ' 

		INSERT INTO @BackupFiles ([LogicalName], [PhysicalName], [Type], [FileGroupName], [Size], [MaxSize], [FileID], [CreateLSN], [DropLSN], [UniqueId], [ReadonlyLSN], [ReadWriteLSN], [BackupSizeInBytes], [SourceBlockSize], [FileGroupId], [LogGroupGUID], [DifferentialBaseLSN], [DifferentialBaseGUID], [IsReadOnly], [IsPresent],[TDEThumbprint],[SnapshotURL])
		EXEC ('RESTORE FILELISTONLY FROM DISK = ''' + @BackupFile + '''')	

		SET @SqlBackupMove  = ''
		SELECT  @SqlBackupMove = 
				@SqlBackupMove + 'MOVE N'''+LogicalName+''' TO N'''+
				CASE WHEN Type = 'D' 
				THEN @RestoreDataPath + RIGHT([PhysicalName], CHARINDEX('\', REVERSE([PhysicalName])) -1)  
				ELSE @RestoreLogPath + RIGHT([PhysicalName], CHARINDEX('\', REVERSE([PhysicalName])) -1)   END 
				+''' ,'  
		FROM @BackupFiles
		DELETE @BackupFiles

		SET @SqlBackupMove = @SqlBackupMove + ' NORECOVERY ,NOUNLOAD,  STATS = 20 {Parameters} ' 
		SET @SqlBackupMove = REPLACE(@SqlBackupMove ,'{Parameters}' , @RestoreExtraParmeters)

		Print @SqlBackupHeader + @SqlBackupMove
		IF(@Execute = 1) EXEC( @SqlBackupHeader + @SqlBackupMove)

	   FETCH NEXT FROM contact_cursor INTO @BackupSetGUID;  
	END  
  
	CLOSE contact_cursor;  
	DEALLOCATE contact_cursor;  

	Print 'RESTORE DATABASE ['+@RestoreDBName+'] WITH RECOVERY; '

	IF(@Execute = 1) EXEC( 'RESTORE DATABASE ['+@RestoreDBName+'] WITH RECOVERY;')
END
GO
