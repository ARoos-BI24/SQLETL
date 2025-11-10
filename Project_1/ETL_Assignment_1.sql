
-- ETL Stored Procedure for Monthly Auto Loan Profit Estimates
CREATE OR ALTER PROCEDURE sp_ProcessMonthlyLoanEstimates
/* 
    This stored procedure calculates monthly auto loan profit estimates.
    It extracts and processes loan data, then applies latest interest rates 
    and risk adjustments before generating final profit estimates.
*/
AS
BEGIN
    SET NOCOUNT ON

	DECLARE
		@BaseInterestRate FLOAT
	,	@EffectiveDate NVARCHAR(10)
	,	@DateFull NVARCHAR(8)
	,	@DateSuffix NVARCHAR(6)
	,	@SourceTableName NVARCHAR(128)
	,	@StagingTableName NVARCHAR(128)
	,	@TargetTableName NVARCHAR(128)
	,	@SQL NVARCHAR(MAX)
	,	@ErrorMessage NVARCHAR(255)

	-- Format the date as YYYYMMDD
	SET @DateFull = CONVERT(NVARCHAR(8), GETDATE(), 112)
	SET @DateSuffix = CONVERT(NVARCHAR(6), GETDATE(), 112)
	SET @SourceTableName = 'CarInformation_' + @DateSuffix
	SET @StagingTableName = 'CarInformation_Staging_' + @DateSuffix
	SET @TargetTableName = 'LoanProfitEstimates_' + @DateSuffix

	BEGIN TRY
		IF OBJECT_ID(@SourceTableName, 'U') IS NULL
        BEGIN
            RAISERROR('
			Source table %s does not exist.
			', 10, 1, @SourceTableName)
            RETURN
        END

		PRINT 'Starting ETL process for ' + @DateSuffix;

		-- STEP 1: Get the most recent interest rate (Deduplicate Using ROW_NUMBER())
		WITH InterestRateHistory AS (
			SELECT
				BaseInterestRate,
				CONVERT(NVARCHAR(8), EffectiveDate, 112) AS EffectiveDate,
				ROW_NUMBER() OVER (ORDER BY EffectiveDate DESC) AS rn
			FROM InterestRates
			WHERE EffectiveDate <= @DateFull
		)
		SELECT
			@BaseInterestRate = BaseInterestRate,
			@EffectiveDate = EffectiveDate
		FROM InterestRateHistory
		WHERE rn = 1

		IF CONVERT(NVARCHAR(6), @EffectiveDate, 112) <> @DateSuffix
			BEGIN
				RAISERROR('
				Base interest rate for the current month (%s) is missing.
				Value from the previous month was used.
				', 16, 1, @DateSuffix)
			END

		IF @BaseInterestRate IS NULL
			BEGIN
				RAISERROR('
				No valid interest rate found.', 16, 1)
				RETURN
			END
		ELSE
			PRINT 'Using interest rate: ' +  + CAST(@BaseInterestRate AS NVARCHAR(10))
			PRINT 'With effective date: ' +  + CAST(@EffectiveDate AS NVARCHAR(10))

		-- STEP 2: Create staging table and validate data
		SET @SQL = N'
		-- Check if staging table exists
		IF OBJECT_ID(''' + @StagingTableName + ''', ''U'') IS NULL
		BEGIN
			-- Create staging table with data validation
			SELECT
				RecordID,
				LTRIM(RTRIM(CarModel)) AS CarModel,
				UPPER(LTRIM(RTRIM(EnergyClass))) AS EnergyClass,
				CASE
					WHEN ManufactureYear < 2015 OR ManufactureYear > YEAR(GETDATE())
					THEN NULL
					ELSE ManufactureYear
				END AS ManufactureYear,
				CASE
					WHEN BasePrice <= 0 THEN NULL
					ELSE BasePrice
				END AS BasePrice,
				UPPER(LTRIM(RTRIM(CustomerRiskTier))) AS CustomerRiskTier,
				FileMonth,
				GETDATE() AS ProcessedDate
			INTO ' + @StagingTableName + '
			FROM ' + @SourceTableName + '
			WHERE RecordID IS NOT NULL
				AND CarModel IS NOT NULL
				AND EnergyClass IS NOT NULL
				AND BasePrice > 0
				AND CustomerRiskTier IS NOT NULL
		END
		ELSE
			BEGIN
				RAISERROR(''Table %s already exists, dropping table. Run the SP again!'', 16, 1, ''' + @StagingTableName + ''')
			END'

		EXEC sp_executesql @SQL

		-- STEP 3: Transform and load into target table
		SET @SQL = N'
		-- Check if target table exists
		IF OBJECT_ID(''' + @TargetTableName + ''', ''U'') IS NULL
			BEGIN
				-- Create final results table
				SELECT
				c.RecordID,
				c.CarModel,
				c.EnergyClass,
				c.ManufactureYear,
				c.BasePrice,
				c.CustomerRiskTier,
				-- Calculate Final Interest Rate
				' + CAST(@BaseInterestRate AS NVARCHAR) + '  + ecm.MarginRate + crt.RiskAdjustment AS FinalInterestRate,
				-- Calculate Estimated Monthly Payment (simple interest-only formula)
				(c.BasePrice * (' + CAST(@BaseInterestRate AS NVARCHAR) + '  + ecm.MarginRate + crt.RiskAdjustment) / 100) / 12 AS EstimatedMonthlyPayment,
				-- Calculate Depreciated Value based on vehicle age and matching depreciation rate
				c.BasePrice * (1 - dr.DepreciationRate) AS DepreciatedValue,
				-- Calculate Estimated Profit (basic model)
				(
					((c.BasePrice * (' + CAST(@BaseInterestRate AS NVARCHAR) + ' + ecm.MarginRate + crt.RiskAdjustment) / 100)) -
					(c.BasePrice - (c.BasePrice * (1 - dr.DepreciationRate)))
				) AS EstimatedProfit,
				c.FileMonth,
				c.ProcessedDate
				INTO ' + @TargetTableName + '
				FROM ' + @StagingTableName + ' c
				JOIN EnergyClassMargin ecm ON c.EnergyClass = UPPER(LTRIM(RTRIM(ecm.EnergyClass)))
				JOIN CreditRiskTier crt ON c.CustomerRiskTier = UPPER(LTRIM(RTRIM(crt.RiskTier)))
				JOIN DepreciationRates dr
					ON (YEAR(c.FileMonth) - c.ManufactureYear) BETWEEN dr.MinYear AND dr.MaxYear
			END
		ELSE
			BEGIN
				RAISERROR(''Table %s already exists, dropping table. Run the SP again!'', 16, 1, ''' + @TargetTableName + ''')
			END'

		EXEC sp_executesql @SQL

		-- STEP 4: Add indexes for performance
		SET @SQL = N'CREATE INDEX IX_' + @TargetTableName 
			+ '_RecordID ON ' + @TargetTableName + '(RecordID);'
		EXEC sp_executesql @SQL

		SET @SQL = N'CREATE INDEX IX_' + @TargetTableName 
			+ '_EnergyClass ON ' + @TargetTableName + '(EnergyClass);'
		EXEC sp_executesql @SQL

		-- STEP 5: Clean up staging table
		SET @SQL = N'DROP TABLE ' + @StagingTableName + ';'
		EXEC sp_executesql @SQL

		PRINT 'ETL process completed successfully for ' + @TargetTableName
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE()
        PRINT '
		ETL process failed: ' + @ErrorMessage

        -- Clean up on error
        IF OBJECT_ID(@StagingTableName, 'U') IS NOT NULL
        BEGIN
            SET @SQL = N'DROP TABLE ' + @StagingTableName + ';'
            EXEC sp_executesql @SQL
        END

		IF OBJECT_ID(@TargetTableName, 'U') IS NOT NULL
        BEGIN
            SET @SQL = N'DROP TABLE ' + @TargetTableName + ';'
            EXEC sp_executesql @SQL
        END
        --RAISERROR('ETL process failed: %s', 16, 1, @ErrorMessage)
    END CATCH
END
GO

-- STEP 6: Create SQL Server Agent Job for automated execution
CREATE OR ALTER PROCEDURE sp_CreateAgentJob_LoanProfitEstimates
AS
	BEGIN
	-- Create the job
	EXEC msdb.dbo.sp_add_job
		@job_name = N'Monthly Auto Loan ETL Process'

	-- Add job step
	EXEC msdb.dbo.sp_add_jobstep
		@job_name = N'Monthly Auto Loan ETL Process',
		@step_name = N'Process Monthly Loan Estimates',
		@subsystem = N'TSQL',
		@database_name = N'ETL_Assignment1',
		@command = N'EXEC sp_ProcessMonthlyLoanEstimates;',
		@on_success_action = 1,
		@on_fail_action = 2

	-- Create schedule (runs on 1st day of each month at 6 AM)
	EXEC msdb.dbo.sp_add_schedule
		@schedule_name = N'Monthly ETL Schedule',
		@freq_type = 16,
		@freq_interval = 1,
		@freq_recurrence_factor = 1,
		@active_start_time = 100000

	-- Attach schedule to job
	EXEC msdb.dbo.sp_attach_schedule
		@job_name = N'Monthly Auto Loan ETL Process',
		@schedule_name = N'Monthly ETL Schedule'

	-- Add job to server
	EXEC msdb.dbo.sp_add_jobserver
		@job_name = N'Monthly Auto Loan ETL Process'
END
GO

EXEC sp_ProcessMonthlyLoanEstimates
-- DROP PROCEDURE sp_ProcessMonthlyLoanEstimates

IF OBJECT_ID('sp_CreateAgentJob_LoanProfitEstimates', 'P') IS NULL
BEGIN
    EXEC sp_CreateAgentJob_LoanProfitEstimates
END
-- DROP PROCEDURE sp_CreateAgentJob_LoanProfitEstimates





/*
-- Check job history
SELECT
    j.name AS JobName,
    h.run_date,
    h.run_time,
    msdb.dbo.agent_datetime(h.run_date, h.run_time) AS RunDateTime,
    CASE h.run_status
        WHEN 0 THEN 'Failed'
        WHEN 1 THEN 'Succeeded'
        WHEN 2 THEN 'Retry'
        WHEN 3 THEN 'Canceled'
        WHEN 4 THEN 'In Progress'
    END AS RunStatus,
    h.run_duration AS RunDuration -- format is HHMMSS
FROM msdb.dbo.sysjobhistory h
JOIN msdb.dbo.sysjobs j ON h.job_id = j.job_id
WHERE h.step_id = 0 -- step_id = 0 means it's the outcome of the entire job
ORDER BY h.run_date DESC, h.run_time DESC;

SELECT * FROM msdb.dbo.sysjobs_view;
*/



/*
--  Identify All Existing Schedules
SELECT schedule_id, name 
FROM msdb.dbo.sysschedules 
WHERE name = N'Monthly ETL Schedule';

-- Delete All Previous Schedules
DECLARE @ScheduleID INT;

WHILE EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'Monthly ETL Schedule')
BEGIN
    SELECT TOP 1 @ScheduleID = schedule_id FROM msdb.dbo.sysschedules WHERE name = N'Monthly ETL Schedule';
    
    EXEC msdb.dbo.sp_delete_schedule @schedule_id = @ScheduleID;
END;

*/