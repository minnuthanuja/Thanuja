cls
$SourceServer = 'edwdev'
$SourceDatabase = 'DMSData'
$DestinationServer = 'edwdev'
$DestinationDatabase = 'DMSData'
#$ProgressErrorOutput = @()
#$ProgressSuccessOutput = @()
$mergedDataSet = new-object system.data.dataset;

# ==============================================================================
# FUNCTION CREATED TO RUN SQL QUERIES
# ==============================================================================
function SqlQuery($server, $database, $query)
{
 $connection = New-Object System.Data.SqlClient.SqlConnection
 $connection.ConnectionString = "Server=$server;Database=$database;Integrated Security=True;"
 $connection.Open()
 $command = $connection.CreateCommand()
 $command.CommandText = $query
 $result = $command.ExecuteReader()
 $table = new-object “System.Data.DataTable”
 $table.Load($result)
 $connection.Close()
 return $table
}

# ==============================================================================
# TRUNCATE OUTPUT TABLE
# ==============================================================================
$EmptyOutputTable= "Truncate table [DMSData].[dbo].[SQLJobMonitor]"
try{
SqlQuery $DestinationServer $DestinationDatabase $EmptyOutputTable
}
catch
{
Write-Error 'Failed to truncate output table'
return
}

# ==============================================================================
# GET SERVER LIST
# ==============================================================================
$ServerListQuery = "Select [Server_Name] as Server_Name  from [dbo].[Blitz_Config_AU] where Is_Active=1"
#$Priority
#$FindingsGroup
#$Finding
#$DatabaseName
#$URL
#$Details
#$QueryPlan
#$QueryPlanFiltered
#$CheckID
#$Server_Name
#$Server_Version
#$Server_Edition
#$Server_Product_Level
#$Server_Product_Version

$Sql_Instances = SqlQuery $SourceServer $SourceDatabase $ServerListQuery
#$Sql_Instances
$SuccessCount = 0
$FailureCount = 0
ForEach ($Instance in $Sql_Instances.Server_Name)
{


try{

$spQuery = "DECLARE @PreviousDate datetime 
DECLARE @Year VARCHAR(4) 
DECLARE @Month VARCHAR(2) 
DECLARE @MonthPre VARCHAR(2) 
DECLARE @Day VARCHAR(2) 
DECLARE @DayPre VARCHAR(2) 
DECLARE @FinalDate INT 

-- Initialize Variables 
SET @PreviousDate = DATEADD(dd, -7, GETDATE()) -- Last 7 days  
SET @Year = DATEPART(yyyy, @PreviousDate)  
SELECT @MonthPre = CONVERT(VARCHAR(2), DATEPART(mm, @PreviousDate)) 
SELECT @Month = RIGHT(CONVERT(VARCHAR, (@MonthPre + 1000000000)),2) 
SELECT @DayPre = CONVERT(VARCHAR(2), DATEPART(dd, @PreviousDate)) 
SELECT @Day = RIGHT(CONVERT(VARCHAR, (@DayPre + 1000000000)),2) 
SET @FinalDate = CAST(@Year + @Month + @Day AS INT) 

-- Final Logic 
SELECT   h.server as Server_Name ,
         j.[name] AS Job_Name, 
         --s.step_name, 
       --  h.step_id, 
         h.step_name AS Job_Step_Name,
       CONVERT(DATETIME, RTRIM(run_date) + ' '
        + STUFF(STUFF(REPLACE(STR(RTRIM(h.run_time),6,0),
        ' ','0'),3,0,':'),6,0,':')) as Job_Runtime, 
        -- h.sql_severity, 
         h.message As [Error_Message]
         
FROM     msdb.dbo.sysjobhistory h 
         INNER JOIN msdb.dbo.sysjobs j 
           ON h.job_id = j.job_id 
         INNER JOIN msdb.dbo.sysjobsteps s 
           ON j.job_id = s.job_id
           AND h.step_id = s.step_id
WHERE    j.enabled = 1
and h.run_status = 0 -- Failure 
         AND h.run_date > @FinalDate AND j.name <> 'syspolicy_purge_history'
         AND (j.name like '%Database%'
         or j.name like '%index%')
ORDER BY h.instance_id DESC"

$mergedDataSet = new-object system.data.dataset;

$sourceServerConnection = new-object Microsoft.SqlServer.Management.Common.ServerConnection 
    
$sourceServerConnection.ServerInstance=$Instance
$SqlConnection = New-Object System.Data.SqlClient.SqlConnection
$SqlConnection.ConnectionString = "$($sourceServerConnection) ; Integrated Security = true ; MultiSubnetFailover=True;ApplicationIntent=ReadOnly"
$SqlCmd = New-Object System.Data.SqlClient.SqlCommand
$SqlCmd.CommandText = $spQuery
$SqlCmd.Connection = $SqlConnection
$SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
$SqlAdapter.SelectCommand = $SqlCmd
$DataSet = New-Object System.Data.DataSet
$counter = $SqlAdapter.Fill($mergedDataSet) |  out-null
$SqlConnection.Close()
$SqlConnection.Dispose()


$connectionString = "Data Source=$DestinationServer; Integrated Security=True;Initial Catalog=DMSDATA;"
#Bulk copy object instantiation
$bulkCopy = new-object ("Data.SqlClient.SqlBulkCopy") $connectionString
#Define the destination table 
$bulkCopy.DestinationTableName = "[dbo].[SQLJobMonitor]"
#load the data into the target
$bulkCopy.WriteToServer($mergedDataSet.Tables[0])

$SuccessCount = $SuccessCount + 1
Write-Host 'Success Count = ' $SuccessCount 'Success For ' $Instance -BackgroundColor DarkGreen
}
catch
{
$FailureCount = $FailureCount + 1
Write-Host 'Failure Count = ' $FailureCount 'Failure On :-' $Instance -BackgroundColor Red
$FailureCategory = 'AUS Monday Job Monitoring'
$failureInfoQuery =  "INSERT INTO [dbo].[Failure_logging] VALUES('$Instance','$FailureCategory',getdate())"
SqlQuery $DestinationServer $DestinationDatabase $failureInfoQuery
}
}

# ==============================================================================
# SEND MAIL FOR FAILED JOBS
# ==============================================================================
$SendMailQuery = "
--Send mails to the user with Failed job list

DECLARE @tableHTML  NVARCHAR(MAX)   
DECLARE @body NVARCHAR(MAX)  
Declare @errorbody Nvarchar(100)  
Declare @errorbody1 Nvarchar(max)  
SET @errorbody=' Hi Team,<br> <br> The below are the list of all jobs that failed on '+CONVERT(VARCHAR(12),GETDATE(),107)  
SET @tableHTML = CAST(( select [Server_Name]  AS 'td','' ,[Job_Name] AS 'td',''  ,[Job_Step_Name] AS 'td',''  , [Job_Runtime] As 'td',''  , [Error_Message] As 'td','' FROM  [dbo].[SQLJobMonitor]


FOR XML PATH('tr'), ELEMENTS ) AS NVARCHAR(MAX))    

SET @body = N'<H4>List of failed jobs: <br>  <br>' +      
N'<html><body><table border = 1> <FONT SIZE=2>     
<tr bgcolor=#F8F8FD>      
<th> <FONT SIZE=2> Server_Name </FONT></th>   
<th> <FONT SIZE=2> Job_Name  </FONT></th>   
<th> <FONT SIZE=2> Job_Step_Name  </FONT></th>   
<th> <FONT SIZE=2> Job_Runtime  </FONT></th>   
<th> <FONT SIZE=2> Error_Message  </FONT></th>'      

SET @body = @body+@tableHTML +'</FONT></table> <br> <br> 

Thanks,<br> SIMPLOT DBA Team</body></html>' 

--Select @body

SET @errorbody1=@errorbody+@body  
declare @TABCOUNT int  
set @TABCOUNT = (select count(1) from [dbo].[SQLJobMonitor])  
if(@TABCOUNT>0)  

EXEC msdb.dbo.sp_send_dbmail  

@profile_name = 'DBMaintenanceMail',  
@recipients='lijith.lakshmanan@simplot.com@simplot.com ; lijith.lakshmanan@intimetec.com ; Sudhir.Kumar@intimetec.com',    
@subject = 'SQL Job Moitoring Report AUS',  
@body = @errorbody1,  
@body_format = 'HTML',  
@importance='High'  

Else  
Print 'No Failed Jobs' 
"
try
{
SqlQuery $DestinationServer $DestinationDatabase $SendMailQuery
Write-Host 'Mail send successfully ' -BackgroundColor Cyan
}
catch
{
Write-Host 'Send mail part failed'  -BackgroundColor Red
}



#$ProgressSuccessOutput | out-file -filepath C:\Devraj\Test\NoSP_Blitz.txt -append 
#$ProgressErrorOutput | out-file -filepath C:\Devraj\Test\ErrorConnectSp_Blitz.txt -append 
#$FoundSP | out-file -filepath C:\Devraj\Test\FoundSp_Blitz.txt -append 

#$ProgressSuccessOutput
#$ProgressErrorOutput 

