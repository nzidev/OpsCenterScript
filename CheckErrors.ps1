$invocation = (Get-Variable MyInvocation).Value
$directorypath = Split-Path $invocation.MyCommand.Path
. "$directorypath\IncomingData.ps1"
. "$directorypath\SQLFunctions.ps1"
. "$directorypath\Mail.ps1"
. "$directorypath\Processing.ps1"
$table = "Errors"

for ($cod =0; $cod -lt $AnySQLInfo.count ; $cod++){ #по всем цодам
    $tbl = $SQLTable[$cod]
    $SqlSelect = "SELECT [Start_Time],[End_Time],[Policy_Name],[Media_Server],[MasterServerID],status  FROM [$tbl] WHERE [status] <>'0' and End_time <> '1970-01-01 03:00:00.000' ORDER BY [Start_Time] DESC"
    [array]$SelectPolicyWithErrors = Invoke-DatabaseQuery –query "$SqlSelect" –connectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True" 

    foreach ($policyError in $SelectPolicyWithErrors)
    {
        $segment = ""
        if ($policyError.MasterServerID -eq "193898" -or $policyError.MasterServerID -eq "62033" -or $policyError.Media_Server -like "*idmz*")
        {
            $segment = "iDMZ"
        }

        if ($policyError.MasterServerID -eq "61" -and $policyError.Media_Server -notlike "*idmz*") 
        {
            $segment = "inside"
        }

        if ($policyError.MasterServerID -eq "559" -or $policyError.MasterServerID -eq "12692" -or $policyError.MasterServerID -eq "127829")
        {
            $segment = "rDMZ"
        }
        $Policy_Name = $policyError.Policy_Name
        $Start_Time = $policyError.Start_Time
        $End_Time =$policyError.End_Time
        
        $status = $policyError.status
        
        if ($Policy_Name -ne $null)
        {
         $SqlInsert = "insert into [dbo].[$table] values (N'$Policy_Name',N'$Start_Time',N'$End_Time',N'$segment',N'$status', '')"
         Invoke-DatabaseQuery –query "$SqlInsert" –connectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True" 
        }
    }

   
}