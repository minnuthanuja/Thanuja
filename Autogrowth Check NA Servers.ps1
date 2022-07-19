CLS 

$SourceServer = 'processprod'
$SourceDatabase = 'SQL_Audit'
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

$EmptyOutputTable= "Truncate table [DMSData].[dbo].[Blitz_Result_Autogrowth]"
try{
SqlQuery $DestinationServer $DestinationDatabase $EmptyOutputTable
}
catch
{
Write-error 'error while truncating output table' -BackgroundColor DarkMagenta
return
}


# ==============================================================================
# AUTOGROWTH FOR DEVELOPMENT SERVERS
# ==============================================================================
Write-Host 'Autogrowth Task for NA Dev Servers Started' -BackgroundColor DarkMagenta

$ServerListQuery = "select distinct [Server_Name] from [views].[SQL_Instance_Audit_LL] where [Region_V3] ='NORAM'and Disposition_Name != 'Production'"
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

$spQuery = "EXEC [dbo].[Custom_SP_Blitz_Autogrowth_Tracking]  WITH RESULT SETS 
(
( 
Priority TINYINT ,
FindingsGroup VARCHAR(50),
Finding VARCHAR(200),
DatabaseName NVARCHAR(128),
URL VARCHAR(200),
Details NVARCHAR(4000),
QueryPlan XML  NULL,
QueryPlanFiltered [NVARCHAR](MAX) NULL,
CheckID INT,
Server_Name sql_variant,
[Server_Version] varchar(128),
[Server_Edition] sql_variant,
[Server_Product_Level] sql_variant,
[Server_Product_Version] sql_variant,
[run_datetime] [datetime] NULL,
[Size] [int] NOT NULL,
[Growth] [int] NOT NULL,
[is_percent_growth] bit NOT NULL,
[Database_ID] [smallint] NULL,
[logical_name] [sysname] NOT NULL
)
);"

$mergedDataSet = new-object system.data.dataset;

$sourceServerConnection = new-object Microsoft.SqlServer.Management.Common.ServerConnection 
    
$sourceServerConnection.ServerInstance=$Instance
$SqlConnection = New-Object System.Data.SqlClient.SqlConnection
$SqlConnection.ConnectionString = "$($sourceServerConnection) ; Integrated Security = true ; MultiSubnetFailover=True;ApplicationIntent=ReadOnly"
$SqlCmd = New-Object System.Data.SqlClient.SqlCommand
$SqlCmd.CommandTimeout = 100
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
$bulkCopy.DestinationTableName = "[dbo].[Blitz_Result_Autogrowth]"
#load the data into the target
$bulkCopy.WriteToServer($mergedDataSet.Tables[0])

$SuccessCount = $SuccessCount + 1
Write-Host 'Success Count = ' $SuccessCount 'Success For ' $Instance -BackgroundColor DarkGreen
}
catch
{
$FailureCount = $FailureCount + 1
Write-Host 'Failure Count = ' $FailureCount 'Failure On :-' $Instance -BackgroundColor Red
$FailureCategory = 'Autogrowth Check NA Servers'
$failureInfoQuery =  "INSERT INTO [dbo].[Failure_logging] VALUES('$Instance','$FailureCategory', Getdate())"
SqlQuery 'edwdev' $DestinationDatabase $failureInfoQuery
}
}

$SendMailQuery = "EXEC [SP_Autogrowth_Tracking_SendMail]"
try
{
SqlQuery $DestinationServer $DestinationDatabase $SendMailQuery
Write-Host 'Mail send successfully for NA Dev Servers ' -BackgroundColor Cyan
}
catch
{
Write-Host 'NA Dev Servers Send mail part failed'  -BackgroundColor Red
}


Write-Host 'Autogrowth Task for NA Dev Servers Completed' -BackgroundColor DarkMagenta
Write-Host '     '
Write-Host 'Autogrowth Task for NA Prod Servers Started' -BackgroundColor DarkMagenta
# ==============================================================================
# AUTOGROWTH FOR PRODUCTION NA SERVERS
# ==============================================================================

$ServerListQuery = "select distinct [Server_Name] from [views].[SQL_Instance_Audit_LL] where [Region_V3] ='NORAM' and Disposition_Name = 'Production'"
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

$spQuery = "EXEC [dbo].[Custom_SP_Blitz_Autogrowth_Tracking]  WITH RESULT SETS 
(
( 
Priority TINYINT ,
FindingsGroup VARCHAR(50),
Finding VARCHAR(200),
DatabaseName NVARCHAR(128),
URL VARCHAR(200),
Details NVARCHAR(4000),
QueryPlan XML  NULL,
QueryPlanFiltered [NVARCHAR](MAX) NULL,
CheckID INT,
Server_Name sql_variant,
[Server_Version] varchar(128),
[Server_Edition] sql_variant,
[Server_Product_Level] sql_variant,
[Server_Product_Version] sql_variant,
[run_datetime] [datetime] NULL,
[Size] [int] NOT NULL,
[Growth] [int] NOT NULL,
[is_percent_growth] bit NOT NULL,
[Database_ID] [smallint] NULL,
[logical_name] [sysname] NOT NULL
)
);"

$mergedDataSet = new-object system.data.dataset;

$sourceServerConnection = new-object Microsoft.SqlServer.Management.Common.ServerConnection 
    
$sourceServerConnection.ServerInstance=$Instance
$SqlConnection = New-Object System.Data.SqlClient.SqlConnection
$SqlConnection.ConnectionString = "$($sourceServerConnection) ; Integrated Security = true ; MultiSubnetFailover=True;ApplicationIntent=ReadOnly"
$SqlCmd = New-Object System.Data.SqlClient.SqlCommand
$SqlCmd.CommandTimeout = 100
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
$bulkCopy.DestinationTableName = "[dbo].[Blitz_Result_Autogrowth]"
#load the data into the target
$bulkCopy.WriteToServer($mergedDataSet.Tables[0])

$SuccessCount = $SuccessCount + 1
Write-Host 'Success Count = ' $SuccessCount 'Success For ' $Instance -BackgroundColor DarkGreen
}
catch
{
$FailureCount = $FailureCount + 1
Write-Host 'Failure Count = ' $FailureCount 'Failure On :-' $Instance -BackgroundColor Red
$FailureCategory = 'Autogrowth Check NA Servers'
$failureInfoQuery =  "INSERT INTO [dbo].[Failure_logging] VALUES('$Instance','$FailureCategory', Getdate())"
SqlQuery 'edwdev' $DestinationDatabase $failureInfoQuery
}
}

$SendMailQuery = "EXEC [SP_Autogrowth_Tracking_SendMailPrd]"
try
{
SqlQuery $DestinationServer $DestinationDatabase $SendMailQuery
Write-Host 'Mail send For NA Prod Servers ' -BackgroundColor Cyan
}
catch
{
Write-Host 'Send mail part failed for NA Prod Servers'  -BackgroundColor Red
}



#$ProgressSuccessOutput | out-file -filepath C:\Devraj\Test\NoSP_Blitz.txt -append 
#$ProgressErrorOutput | out-file -filepath C:\Devraj\Test\ErrorConnectSp_Blitz.txt -append 
#$FoundSP | out-file -filepath C:\Devraj\Test\FoundSp_Blitz.txt -append 

#$ProgressSuccessOutput
#$ProgressErrorOutput 


Write-Host 'Autogrowth Task for NA Prod Servers Completed' -BackgroundColor DarkMagenta