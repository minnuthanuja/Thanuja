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

$EmptyOutputTable= "Truncate table [DMSData].[dbo].[Blitz_Result]"
try{
SqlQuery $DestinationServer $DestinationDatabase $EmptyOutputTable
}
catch
{
Write-Error 'Error in truncating output table'
return
}


# ==============================================================================
# SP_Blitz FOR AUS SERVERS
# ==============================================================================
Write-Host 'SP_Blitz Task for AUS Servers Started' -BackgroundColor DarkMagenta

$ServerListQuery = "Select [Server_Name] as Server_Name from [dbo].[Blitz_Config_AU]"
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

$spQuery = "EXEC sp_Blitz WITH RESULT SETS 
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
[Server_Product_Version] sql_variant
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
$bulkCopy.DestinationTableName = "[dbo].[Blitz_Result]"
#load the data into the target
$bulkCopy.WriteToServer($mergedDataSet.Tables[0])

$SuccessCount = $SuccessCount + 1
Write-Host 'Success Count = ' $SuccessCount 'Success For ' $Instance -BackgroundColor DarkGreen
}
catch
{
$FailureCount = $FailureCount + 1
Write-Host 'Failure Count = ' $FailureCount 'Failure On :-' $Instance -BackgroundColor Red
$FailureCategory = 'SP_Blitz AUS Servers'
$failureInfoQuery =  "INSERT INTO [dbo].[Failure_logging] VALUES('$Instance','$FailureCategory',getdate())"
SqlQuery 'edwdev' $DestinationDatabase $failureInfoQuery
}
}


Write-Host 'Sp_Blitz Task for AUS Servers Completed' -BackgroundColor DarkMagenta
Write-Host '     '
