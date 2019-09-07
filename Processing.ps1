function ProcessingTable{
    param (
		[array]$allBackups
	)
    [array]$eachBackup = $null
    
    if ($allBackups.End_Time -eq "01.01.1970 03:00:00")
    {
        $noFinishedBackups = @()

        $eachBackup = $allBackups | Where-Object {$_.End_Time -eq "01.01.1970 03:00:00"}
        
    }
    else
    {
                  
        ############################################
        ######Обрабатываем политику#################

        $DBrow=$null
        $size = 0
        [array]$uniqclient = @()
        [array]$uniqArray = @()
        $eachBackup = @()
        for($i = $allBackups.Count; $i -gt 0; $i--)
        {
    
            $res = "" | Select-Object MasterServerID, Type, size, End_Time, Client, Media_Server, Status, Start_Time, Policy_Name
            [array]$allres = ""
            $size= 0 
            $x = $allBackups.Count - 1
                $groupres = ($allBackups.Start_time | sort | group)
            #if (($allBackups.Start_time | sort | group)[0].Count -gt 1)   # Если бэкапится несколько клиентов в одной политике
            # Если бэкапится несколько клиентов в одной политике
            if ($groupres[0].Count -gt 1 -and ($allBackups | Where-Object {$_.Start_time -eq [datetime]::ParseExact($groupres[0].name, "dd.MM.yyyy HH:mm:ss", $null)}).type -ne 2)   # Если бэкапится несколько клиентов в одной политике
                        
            {
                [array]$uniqArray = @()
                [array]$uniqclient = @()
                $uniqclient = ($allBackups.client | sort | Get-Unique)

                for ($j = 0; $j -lt $uniqclient.count; $j++)
                {
                    if (($allBackups[$x].size -eq 0 -AND $allBackups[$x].Status -eq 0) -OR ($allBackups[$x].size -eq 0 -AND $allBackups[$x].Status -ne 0 -AND $allBackups[$x].Type -ne 0 ) -OR ($allBackups[$x].size -eq 0 -AND $allBackups[$x].Status -eq 1 -AND $allBackups[$x].type -eq 0) -OR ($allBackups[$x].size -ne 0 -AND $allBackups[$x].Status -eq 0 -AND $allBackups[$x].type -eq 0))
                    {
                        $size = 0 
                        $res = "" | Select-Object MasterServerID, Type, size, End_Time, Client, Media_Server, Status, Start_Time, Policy_Name
                        $allres = $allBackups | Where-Object {($_.Start_Time -ge $allBackups[$x].Start_Time) -and ($_.end_Time -le $allBackups[$x].end_Time.AddSeconds(1)) -and ($_.client -eq $allBackups[$x].client)}
                        $res.MasterServerID = $allres[$allres.count-1].MasterServerID
                        $res.End_Time = $allres[$allres.count-1].End_Time
                        $res.Start_Time = $allres[$allres.count-1].Start_Time
                        $res.Media_Server = $allres[$allres.count-1].Media_Server
                        $res.Type = $allres[$allres.count-1].Type

                        $wrongstatus = $allres.Status | sort | Get-Unique
                        $status = $wrongstatus -join ", "
                        $res.Status = $status
                
                        try{
                        $res.Client = ($allres | Where-Object {($_.size -ne "0")})[0].client
                        }
                        catch
                        {
                        $res.Client = $allres[$allres.count - 1].client 
                        }
                        $res.Policy_Name = $allres[$allres.count-1].Policy_Name
                        $allres | foreach {$size += $_.size}
                        $res.size = $size
        
                        $eachBackup = $eachBackup + $res

                        $allBackups = $allBackups | Where-Object {!(($_.Start_Time -ge $allBackups[$x].Start_Time) -and ($_.end_Time -le $allBackups[$x].end_Time.AddSeconds(1)) -and ($_.client -eq $allBackups[$x].client))}
                        $i = $allBackups.count + 1
                        $x = $allBackups.Count - 1
                    }
                    else
                    {
                        $eachBackup = $eachBackup + $allBackups[$x]
                        $allBackups = $allBackups | Where-Object {!(($_ -eq $allBackups[$x]))}
                        $x = $allBackups.Count - 1

                    }

                }


            }
            else{
                            
                            




                if (($allBackups[$x].size -eq 0 -AND $allBackups[$x].Status -eq 0) -OR ($allBackups[$x].size -eq 0 -AND $allBackups[$x].Status -ne 0 -AND $allBackups[$x].Type -ne 0)  -OR ($allBackups[$x].size -eq 0 -AND $allBackups[$x].Status -eq 1 -AND $allBackups[$x].type -eq 0))
                {
                    $size = 0 
                    $res = "" | Select-Object MasterServerID, Type, size, End_Time, Client, Media_Server, Status, Start_Time, Policy_Name
                    $allres = $allBackups | Where-Object {($_.Start_Time -ge $allBackups[$x].Start_Time) -and ($_.end_Time -le $allBackups[$x].end_Time.AddSeconds(1))}
                    $res.MasterServerID = $allres[$allres.count-1].MasterServerID
                    $res.End_Time = $allres[$allres.count-1].End_Time
                    $res.Start_Time = $allres[$allres.count-1].Start_Time
                    $res.Media_Server = $allres[$allres.count-1].Media_Server
                    $res.Type = $allres[$allres.count-1].Type

                    $wrongstatus = $allres.Status | sort | Get-Unique
                    $status = $wrongstatus -join ", "
                    $res.Status = $status
        
                    try{
                        $res.Client = ($allres | Where-Object {($_.size -ne "0")})[0].client
                    }
                    catch
                    {
                        try
                        {
                                $res.Client = ($allres | Where-Object {($_.Speed -ne "0")})[0].client
                        }
                        catch
                        {
                        $res.Client = $allres[0].client 
                        }
                    }
                    $res.Policy_Name = $allres[$allres.count-1].Policy_Name
                    $allres | foreach {$size += $_.size}
                    $res.size = $size
        
                    $eachBackup = $eachBackup + $res

                    $allBackups = $allBackups | Where-Object {!(($_.Start_Time -ge $allBackups[$x].Start_Time) -and ($_.end_Time -le $allBackups[$x].end_Time.AddSeconds(1)))}
                    $i = $allBackups.count + 1

                }

                elseif(($allBackups[$x].size -eq 0 -AND ($allBackups[$x].Status -ne 0 -or $allBackups[$x].Status -ne 1) -AND $allBackups[$x].Type -eq 0))
                {
                    [array]$allres = $allBackups | Where-Object {($_.Start_Time -ge $allBackups[$x].Start_Time) -and ($_.end_Time -le $allBackups[$x].end_Time.AddSeconds(1))}
                    if ($allres.count -gt 1 -and ($allres.type).contains(0) -and ($allres.type).contains(2)) # ушли в ошибку родительский и дочерние потоки
                    {
                        $size = 0 
                        $res = "" | Select-Object MasterServerID, Type, size, End_Time, Client, Media_Server, Status, Start_Time, Policy_Name
                        $allres = $allBackups | Where-Object {($_.Start_Time -ge $allBackups[$x].Start_Time) -and ($_.end_Time -le $allBackups[$x].end_Time.AddSeconds(1))}
                        $res.MasterServerID = $allres[$allres.count-1].MasterServerID
                        $res.End_Time = $allres[$allres.count-1].End_Time
                        $res.Start_Time = $allres[$allres.count-1].Start_Time
                        $res.Media_Server = $allres[$allres.count-1].Media_Server
                        $res.Type = $allres[$allres.count-1].Type

                        $wrongstatus = $allres.Status | sort | Get-Unique
                        $status = $wrongstatus -join ", "
                        $res.Status = $status
        
                        try{
                            $res.Client = ($allres | Where-Object {($_.size -ne "0")})[0].client
                        }
                        catch
                        {
                            try
                            {
                                    $res.Client = ($allres | Where-Object {($_.Speed -ne "0")})[0].client
                            }
                            catch
                            {
                            $res.Client = $allres[0].client 
                            }
                        }
                        $res.Policy_Name = $allres[$allres.count-1].Policy_Name
                        $allres | foreach {$size += $_.size}
                        $res.size = $size
        
                        $eachBackup = $eachBackup + $res

                        $allBackups = $allBackups | Where-Object {!(($_.Start_Time -ge $allBackups[$x].Start_Time) -and ($_.end_Time -le $allBackups[$x].end_Time.AddSeconds(1)))}
                        $i = $allBackups.count + 1
                    }
                    else
                    {
                        $eachBackup = $eachBackup + $allBackups[$x]
                        $allBackups = $allBackups | Where-Object {!(($_ -eq $allBackups[$x]))}

                    }

                }

                else
                {
                    if ($allBackups[$x].type -ne 2)
                    {
                    $eachBackup = $eachBackup + $allBackups[$x]
                    $allBackups = $allBackups | Where-Object {!(($_ -eq $allBackups[$x]))}
                    }
                    elseif($allBackups.count -gt 1 -and ($allBackups | Where-Object { ($_.type -ne 2)}).count -gt 0)
                    {
                        $allBackups = $allBackups | Where-Object {!(($_ -eq $allBackups[$x]))}
                        $x = $allBackups.Count - 1
                    }
                    else
                    {
                        $eachBackup = $eachBackup + $allBackups[$x]
                        $allBackups = $allBackups | Where-Object {!(($_ -eq $allBackups[$x]))}
                    }


                }
            }
        }
            #### Все бэкапы в массиве $eachBackup



                    

}
     
     return $eachBackup 
}

function noFinished {
    param (
		[array]$allBackups,
        [string]$client
	)
    $size = 0
    $DBres = "" | Select-Object  Type, Client,size, date, Duration, Start_Time, End_Time, Policy_Name,Media_Server, MasterServerID
    $DBres.Type = $allBackups[$allBackups.Count - 1].Type
    $DBres.MasterServerID = $allBackups[$allBackups.Count - 1].MasterServerID
    $DBres.date = $allBackups[$allBackups.Count - 1].date
    $DBres.Duration = $allBackups[$allBackups.Count - 1].Duration
    $DBres.Start_Time = $allBackups[$allBackups.Count - 1].Start_Time
    $DBres.Policy_Name = $allBackups[$allBackups.Count - 1].Policy_Name
    $DBres.Media_Server = $allBackups[$allBackups.Count - 1].Media_Server
    $DBres.End_Time =  $allBackups[$allBackups.Count - 1].End_Time    

    $allBackups | foreach {$size += $_.size}
    $DBres.size = $size




    $eachBackup = $allBackups[$allBackups.Count - 1]
    if ($client)
    {
        $DBres.Client = $client
    }
    else
    {
        try{
            $DBres.Client = ($allBackups | Where-Object {($_.size -ne "0")})[0].client
        }
        catch
        {
           if ($Policy_Name -ne "NBU_Catalog" -and $MasterServers.Contains($allBackups[$allBackups.count - 1].client))
           {
            $DBres.Client = ($allBackups.client -ne $allBackups[$allBackups.Count - 1].Client)[0]
           } 
           else
           {
            $DBres.Client = $allBackups[$allBackups.count - 1].client 
           }
        }
    }
   
    return $DBres 
}



function getSegment {
    param (
		    [array]$policyError
	    )
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

    return $segment
    
}

# проверка ошибок на предмет необходимо ли откправлять письмо
# если в имени политики есть слово Archlog то после 3х подряд -> письмо
# если Full, Diff или Cum, то сразу письмо
function checkErrorsForMail { 
     
     $SqlSelect = "SELECT  [Start_Time],[End_Time],[Policy_Name],[Segment],[Status],[Mail]  FROM [Errors] WHERE [Mail] = '' ORDER BY [Start_Time] DESC"
     $SelectServerWithErrors = Invoke-DatabaseQuery –query "$SqlSelect" –connectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True" 

     $ErrorsArchlog = $SelectServerWithErrors | Where-Object {$_.Policy_Name -Like "*Archlog*"}

     $ErrorsArchlog | group Policy_Name | foreach {if ($_.count -ge 2) {$ErrorsArchlog}}

}

