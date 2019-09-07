$invocation = (Get-Variable MyInvocation).Value
$directorypath = Split-Path $invocation.MyCommand.Path
. "$directorypath\Processing.ps1"
############################################## –§—É–Ω–∫—Ü–∏—è —Ä–∞–±–æ—Ç—ã —Å SQL #######################################################
 function Invoke-DatabaseQuery 
    {
	[CmdletBinding()]
	param (
		[string]$connectionString,
		[string]$query
	)
	$connection = New-Object System.Data.SqlClient.SqlConnection
	$connection.ConnectionString = $connectionString
	$command = New-Object System.Data.SqlClient.SqlCommand
	$command.CommandText = $query
	$command.Connection = $Connection
	$adapter = New-Object System.Data.SqlClient.SqlDataAdapter
	$adapter.SelectCommand = $command
	$DataSet = New-Object System.Data.DataSet
	$adapter.Fill($DataSet)
	$connection.close()
	$DataSet.Tables[0]
    }
####################################################################################################################################

function AddHungtoSql
{
    param(
        [PSCustomObject]$DBrow,
        [string]$table
    )
    $DBres = "" | Select-Object  Type, Client,Status, date, Duration, Start_Time, End_Time, Policy_Name,Media_Server
    $DBres.Type = $SelectServer.Type
    $DBres.Client = $SelectServer.Client
    $DBres.date = $SelectServer.date
    $DBres.Duration = $SelectServer.Duration
    $DBres.Start_Time = $SelectServer.Start_Time
    $DBres.Policy_Name = $SelectServer.Policy_Name
    $DBres.Media_Server = $SelectServer.Media_Server
    $DBres.Status = "0"
    $DBres.End_Time =  "01.01.1970 04:00:00"       
    AddToSQL -DBrow $DBres  -table $SQLTable[$cod]
    $DBresult = $null
}



function AddToSQL
{
    param(
        [PSCustomObject]$DBrow,
        
        [string]$table
    )
    $newRow = $false
    if ($DBrow.End_Time -eq "01.01.1970 03:00:00")
    {
        $Duration = "0"
    }
    elseif ($DBrow.End_Time -eq "01.01.1970 04:00:00")
    {
        $Duration = "Á‡‚ËÒÎ‡"
    }
    else
    {
        $time = $DBrow.End_Time - $DBrow.Start_Time
       
        
        $hours = [math]::Floor($time.TotalHours)
        [string]$Duration = [string]$hours + "˜ " + [string]$time.Minutes + "ÏËÌ " + [string]$time.Seconds + "ÒÂÍ."

    }
    [datetime]$date = $DBrow.Start_Time
    $Start_Time = $DBrow.Start_Time 
    $End_Time = $DBrow.End_Time

    $Client = $DBrow.Client
    $Media_Server = $DBrow.Media_Server
    $Policy_Name = $DBrow.Policy_Name
    $type = $DBrow.Type
    $MasterServerID = $DBrow.MasterServerID
    $size = $DBrow.size
    
    $status = $DBrow.status
    
    
    if ($Duration -eq "Á‡‚ËÒÎ‡")
    {
         $SqlSelect = "SELECT top 1 [Type],[Client], [date], [Duration],[Start_Time],[End_Time],[Policy_Name],[Delay],[TimeStart]  FROM [$table] WHERE ([Client ] = '$Client' AND [Policy_Name] = '$Policy_Name' AND [Start_Time] = '$Start_Time') ORDER BY [Start_Time] DESC"
    }
    else
    {   
        $SqlSelect = "SELECT top 1 [Type],[Client], [date], [Duration],[Start_Time],[End_Time],[Policy_Name],[Delay],[TimeStart]  FROM [$table] WHERE ([Client ] = '$Client' AND [Policy_Name] = '$Policy_Name') ORDER BY [Start_Time] DESC"
    }   
    $SelectServer = Invoke-DatabaseQuery ñquery "$SqlSelect" ñconnectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True" 
    try{
        $delay =  $SelectServer.Delay
        $startTime = $SelectServer.TimeStart
    }
    catch{
        $delay =  ""
        $startTime = ""
    }
    if ($SelectServer.End_Time -eq "01.01.1970 03:00:00" -AND $Start_Time -eq $SelectServer.Start_Time)
    {
        $Sqlupdate = "update [dbo].[$table] SET [End_Time] = '$End_Time',[Duration] = '$Duration',[size] = '$size', [status] = '$status' WHERE ([Policy_Name] = '$Policy_Name' AND [Client ] = '$Client' AND [Start_Time] = '$Start_Time' AND [type] = '$type')"
        
        Invoke-DatabaseQuery ñquery "$Sqlupdate" ñconnectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True"
        
        ErrorToSQL -DBrow $DBRow
    }
    elseif ((($SelectServer.Start_Time -ne $Start_Time)) -and ($Start_Time -gt "01.01.2018 03:00:00") -and ($Media_Server -ne ""))
    #elseif ((($SelectServer -eq "") -OR ($SelectServer.Start_Time -ne $Start_Time)) -and ($SelectServer.Start_Time -gt "01.01.2018 03:00:00") -and ($Media_Server -ne ""))
    {
        # Ï‡ÒÚÂ ÒÂ‚Â‡ ‰Ó·‡‚Îˇ˛ÚÒˇ ÚÓÎ¸ÍÓ ‚ NBU_Catalog
        if (($Policy_Name -eq "NBU_Catalog" -and $MasterServers.Contains($Client)) -OR ($Policy_Name -ne "NBU_Catalog" -and !$MasterServers.Contains($Client)))
        {
            $SqlInsert = "insert into [dbo].[$table] values (N'$type',N'$Client', N'$date', N'$Duration',N'$size', N'$Start_Time', N'$End_Time', N'$Status', N'$Media_Server', N'$Policy_Name','$MasterServerID',N'$delay',N'$startTime')"
            Invoke-DatabaseQuery ñquery "$SqlInsert" ñconnectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True" 

            ErrorToSQL -DBrow $DBRow

            $newRow = $true  
        }
        
    }

    $SqlSelect = "SELECT [Client] ,[Policy_Name], [MasterServerID]  FROM [$table] WHERE ([Policy_Name] = '$Policy_Name') ORDER BY [Client]"
    $SelectServer = Invoke-DatabaseQuery ñquery "$SqlSelect" ñconnectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True" 
    
    if ($SelectServer.MasterServerID -ne $MasterServerID)
    {
        $Sqlupdate = "update [dbo].[$table] SET [MasterServerID] = '$MasterServerID' WHERE ([Policy_Name] = '$Policy_Name')"
        Invoke-DatabaseQuery ñquery "$Sqlupdate" ñconnectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True" 
   }

}

function ErrorToSQL{
    param(
        [PSCustomObject]$DBrow
    )
    if ($DBrow.End_Time -ne "01.01.1970 03:00:00")
    {
        $Start_Time = $DBrow.Start_Time 
        $End_Time = $DBrow.End_Time

        $Policy_Name = $DBrow.Policy_Name
        
        $status = $DBrow.status
        $segment = getSegment -policyError $DBrow

        $SqlSelect = "SELECT [Policy_Name],[Start_time], [End_time]  FROM [dbo].[Errors] WHERE ([Policy_Name] = '$Policy_Name')"
        $SelectPolicy = Invoke-DatabaseQuery ñquery "$SqlSelect" ñconnectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True" 

        if ($DBrow.status -ne "0" -and $DBrow.Policy_Name -notlike "*Archlog*" -and !($SelectPolicy.start_time.Contains($Start_Time) -and $SelectPolicy.End_time.Contains($End_time)))
        {
                
                $SqlInsert = "insert into [dbo].[Errors] values (N'$Policy_Name',N'$Start_Time',N'$End_Time',N'$segment',N'$status', '')"
                Invoke-DatabaseQuery ñquery "$SqlInsert" ñconnectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True" 
        }
        
        elseif($DBrow.status -eq "0" -and $DBrow.Policy_Name -like "*Archlog*")
        {
               $SqlErrorSelect = "SELECT [Policy_Name] FROM [dbo].[Errors] WHERE ([Policy_Name] = '$Policy_Name')"
               $SqlErrors =  Invoke-DatabaseQuery ñquery "$SqlErrorSelect" ñconnectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True"  
               if ($SqlErrors)
               {
                $SqlDelete = "delete from [dbo].[Errors] where [Policy_Name] = '$policy_name'"
                #$SqlInsert = "insert into [dbo].[Errors] values (N'$Policy_Name',N'$Start_Time',N'$End_Time',N'$segment',N'$status', 'delete')"
                Invoke-DatabaseQuery ñquery "$SqlDelete" ñconnectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True"  
               }
        } 
    }
}

function deleteSQLrunningPolicy {
        $SqlDeleteString = "DELETE FROM [dbo].[Moscow] WHERE ([End_Time] = '1970-01-01 03:00:00.000')"
        Invoke-DatabaseQuery ñquery "$SqlDeleteString" ñconnectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True" 
        $SqlDeleteString = "DELETE FROM [dbo].[Dubna] WHERE ([End_Time] = '1970-01-01 03:00:00.000')"
        Invoke-DatabaseQuery ñquery "$SqlDeleteString" ñconnectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True" 
        $SqlDeleteString = "DELETE FROM [dbo].[Gorodec] WHERE ([End_Time] = '1970-01-01 03:00:00.000')"
        Invoke-DatabaseQuery ñquery "$SqlDeleteString" ñconnectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True" 


        $SqlDeleteString = "DELETE FROM [dbo].[Moscow] WHERE ([End_Time] = '1970-01-01 04:00:00.000')"
        Invoke-DatabaseQuery ñquery "$SqlDeleteString" ñconnectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True" 
        $SqlDeleteString = "DELETE FROM [dbo].[Dubna] WHERE ([End_Time] = '1970-01-01 04:00:00.000')"
        Invoke-DatabaseQuery ñquery "$SqlDeleteString" ñconnectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True" 
        $SqlDeleteString = "DELETE FROM [dbo].[Gorodec] WHERE ([End_Time] = '1970-01-01 04:00:00.000')"
        Invoke-DatabaseQuery ñquery "$SqlDeleteString" ñconnectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True" 
}
