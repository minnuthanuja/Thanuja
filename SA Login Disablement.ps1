
$SourceServer = 'processprod'
$SourceDatabase = 'SQL_Audit'
$DestinationServer = 'edwprod'
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

#$EmptyOutputTable= "Truncate table [DMSData].[dbo].[Blitz_Result_Autogrowth_TestDG12162021]"
#try{
#SqlQuery $DestinationServer $DestinationDatabase $EmptyOutputTable
#}
#catch
#{
#}


# ==============================================================================
# SA enablement Check for NA SERVERS
# ==============================================================================
Write-Host 'SA enablement Task for NA Servers Started' -BackgroundColor DarkMagenta

$ServerListQuery="select distinct [Server_Name] from [views].[SQL_Instance_Audit_LL] where Server_name not in ('OCC02DV096','OCC00DB039','OCC00AP002','LTH01DB001')"

#,'OCC00DB039','OCC02DV108','LTH01DB001'
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

$mergedDataSet = new-object system.data.dataset;


try{

$spQuery = "EXEC [dbo].[Custom_SP_Blitz_SA_Disablement]   WITH RESULT SETS 
(
( 
Priority TINYINT,
Findings_Group VARCHAR(50),
Finding VARCHAR(200),
Database_Name NVARCHAR(128),
URL VARCHAR(200),
Details NVARCHAR(4000),
Query_Plan XML  NULL,
Query_Plan_Filtered [NVARCHAR](MAX) NULL,
Check_ID INT,
Server_Name sql_variant,
[Server_Version] varchar(128),
[Server_Edition] sql_variant,
[Server_Product_Level] sql_variant,
[Server_Product_Version] sql_variant,
[Run_datetime] [datetime] NULL
)
);;"


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
$bulkCopy.DestinationTableName = "[dbo].[Blitz_Result]"
#load the data into the target
$bulkCopy.WriteToServer($mergedDataSet.Tables[0])

$SuccessCount = $SuccessCount + 1
Write-Host 'Success Count = ' $SuccessCount 'Success For ' $Instance -BackgroundColor DarkGreen
}
catch
{
Write-Host $_
$FailureCount = $FailureCount + 1
Write-Host 'Failure Count = ' $FailureCount 'Failure On :-' $Instance -BackgroundColor Red
$FailureCategory = 'SA Disablement'
$failureInfoQuery =  "INSERT INTO [dbo].[Failure_logging] VALUES('$Instance','$FailureCategory', getdate())"
SqlQuery 'edwdev' $DestinationDatabase $failureInfoQuery
}
}

$SendMailQuery = "
--Send mails to the user with Failed job list

DECLARE @tableHTML  NVARCHAR(MAX)   
DECLARE @body NVARCHAR(MAX)  
Declare @errorbody Nvarchar(100)  
Declare @errorbody1 Nvarchar(max)  
SET @errorbody=' Hi Team,<br> <br> The below are the list of all the servers that has SA login enabled in them.'
SET @tableHTML = CAST(( select Distinct [Server_Name]  AS 'td','' ,[Findings_Group] AS 'td',''  ,[Finding] AS 'td',''  , [Details] As 'td','' FROM  Blitz_Result where Findings_Group = 'SA Login Enablement'


FOR XML PATH('tr'), ELEMENTS ) AS NVARCHAR(MAX))    

SET @body = N'<H4>List of SA enabled Servers: <br>  <br>' +      
N'<html><body><table border = 1> <FONT SIZE=2>     
<tr bgcolor=#F8F8FD>      
<th> <FONT SIZE=2> Server_Name </FONT></th>   
<th> <FONT SIZE=2> Findings_Group  </FONT></th>   
<th> <FONT SIZE=2> Finding  </FONT></th>   
<th> <FONT SIZE=2> Details  </FONT></th>'      

SET @body = @body+@tableHTML +'</FONT></table> <br> <br> 

Thanks,<br> SIMPLOT DBA Team</body></html>' 

--Select @body

SET @errorbody1=@errorbody+@body  
declare @TABCOUNT int  
set @TABCOUNT = (SELECT count(1) FROM  Blitz_Result where Findings_Group = 'SA Login Enablement')  
if(@TABCOUNT>0)  

EXEC msdb.dbo.sp_send_dbmail  

@profile_name = 'DBMaintenanceMail',
--@recipients='devraj.gurjar@simplot.com',
@recipients='IT.dba.group@simplot.com',  
@copy_recipients = 'it.dba.intimetech.notifications@simplot.com',  
@subject = 'SA Login Disablement Check',  
@body = @errorbody1,  
@body_format = 'HTML',  
@importance='High'  

Else  
Print 'SA Login Disablement on all servers' 
"
try
{
SqlQuery $DestinationServer $DestinationDatabase $SendMailQuery
Write-Host 'Mail send successfully for NA  Servers ' -BackgroundColor Cyan
}
catch
{
Write-Host 'NA Servers Send mail part failed'  -BackgroundColor Red
}


Write-Host 'SA enablement Task for NA Servers Completed' -BackgroundColor DarkMagenta