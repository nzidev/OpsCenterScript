$invocation = (Get-Variable MyInvocation).Value
$directorypath = Split-Path $invocation.MyCommand.Path
. "$directorypath\IncomingData.ps1"
. "$directorypath\SQLFunctions.ps1"
. "$directorypath\DBFunctions.ps1"
. "$directorypath\Mail.ps1"
. "$directorypath\Processing.ps1"

for ($cod =0; $cod -lt $AnySQLInfo.count ; $cod++){ #по всем цодам
#for ($cod =0; $cod -lt 1 ; $cod++){ #по всем цодам
    for ($seg =0; $seg -lt $AnySQLSegment.Count; $seg++) #по всем сегментам
    {
        $Error.Clear()
        $segmentForQuery = $AnySQLSegment[$seg]
        $connection = "Driver=SQL Anywhere 16;Host="+$AnySQLInfo[$cod].$segmentForQuery + ";Server=OPSCENTER_"+$AnySQLInfo[$cod].$segmentForQuery + ";Database=vxpmdb;Uid=$us;Pwd=" + [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR) + ";Port=13786"
        $query="SELECT policyName FROM domain_Job GROUP BY policyName ORDER BY policyName;"
        [array]$AllPolicy = $null
        $AllPolicy = Invoke-AnywhereQueryDPC -query $query -connectionString $connection
        if ($Error[$error.count-1] -like "*Сервер*базы*данных*не*найден*") #отвелился OpsCenter
        {
            $segmentError = $AnySQLSegment[$seg]
            
            ErrorMassage -server $AnySQLInfo[$cod].$segmentError -segment $AnySQLSegment[$seg]
            # TODO: если отвалился, надо пробовать его вернуть обратно
        }
        else {
            $policy = $null
            foreach($policy in $AllPolicy.policyName){ #по всем политикам
            if ($policy -ne "" -and $policy -notlike "SLP*")
            {
               # $policy = "EX_X5-8_n5001d2db03_Full"  #для теста
                write-host $policy -ForegroundColor Green
                $tbl = $SQLTable[$cod]
                [array]$eachBackup = $null
                [boolean]$newTask = $false
                #узнаем что есть в нашей базе
                $SqlSelect = "SELECT top 1 [Type],[Client], [date], [Duration],[Start_Time],[End_Time],[Policy_Name],[Media_Server]  FROM [$tbl] WHERE [Policy_Name] = '$policy' ORDER BY [Start_Time] DESC"
                $SelectServer = Invoke-DatabaseQuery –query "$SqlSelect" –connectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True" 
                try{
                   [string]$tim = ($SelectServer.End_Time.AddSeconds(1)).ToString("yy\/MM\/dd HH:mm:ss") 
                   [string]$starttim = ($SelectServer.Start_Time).ToString("yy\/MM\/dd HH:mm:ss") 
                }
                catch{
                [string]$tim = '2000/01/01 00:00:00'
                [string]$starttim = '2000/01/01 00:00:00'
                [boolean]$newTask = $true
                }

                $client = $SelectServer.client
                $zerotim = '1970/01/01 03:00:00'
                $idAfterHung = ""
                $id = ""
                if ($tim -eq '70/01/01 03:00:01') #в нашей базе она еще в процессе
                {
                    if ($SelectServer.type -eq 5)
                    {
                        $query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and Start_time >= '$starttim' ORDER BY id DESC;" 
                    }
                    else
                    {
                        $query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and Start_time >= '$starttim' and Client = '$client' ORDER BY id DESC;" 
                    }
                    [array]$DBresult = $null
                    $DBresult = Invoke-AnywhereQueryDPC -query $query -connectionString $connection
                    if (($DBresult.End_Time[$DBresult.count-1]).ToString("yy\/MM\/dd HH:mm:ss") -eq '70/01/01 03:00:00')
                    {
                        $alltasks = $DBresult | Where-Object {$_.type -ne 2 -and $_.client -eq $client -and $_.End_Time.ToString("yy\/MM\/dd HH:mm:ss") -ne '70/01/01 03:00:00'}
                        if (([array]$alltasks).count -ge 1 ) # зависла
                        {
                            AddHungtoSql  -DBrow $SelectServer -table $SQLTable[$cod] # добавляем зависший бэкап
                            #$timZ = ($alltasks[$alltasks.count-2].Start_time).ToString("yy\/MM\/dd HH:mm:ss") 
                            #$query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and Start_time >= '$timZ'  ORDER BY id DESC;" 
                            $query=""
                        }
                        else
                        {
                            if ($DBresult.Type -eq 5 -and ($DBresult | Where-Object {$_.type -eq 5 -and $_.client -ne $client -and $_.start_time -gt $SelectServer.Start_Time.AddDays(1)}))
                            {
                                AddHungtoSql  -DBrow $SelectServer -table $SQLTable[$cod]
                            }
                            else
                            {
                                $query=""
                            }

                        }
                    }
                    else #закончился
                    {
                         $query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and Start_time >= '$starttim'  ORDER BY id DESC;" 
                    }
                }
                elseif ($tim -eq '70/01/01 04:00:01') #в нашей базе она зависла
                {
                        
                        $type = $SelectServer.Type
                        $query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and Start_time >= '$starttim' and Type = '$type' and Client = '$client'  ORDER BY id DESC;" 
                        [array]$DBresult = $null
                        $DBresult = Invoke-AnywhereQueryDPC -query $query -connectionString $connection
                        if (!$DBresult)
                        {
                            $query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and End_time >= '$starttim' and Type = '$type'  ORDER BY id DESC;" 
                            [array]$DBresult = $null
                            $DBresult = Invoke-AnywhereQueryDPC -query $query -connectionString $connection
                        }

                        $afterHungNumber = 2
                        for ($i = $DBresult.count-2; $i -gt 0;$i--)
                        {
                        if ($DBresult[$i].End_Time -ne "01.01.1970 03:00:00")
                            {
                                [array]$afterHung =  $DBresult[$i]
                                $i = 0
                            }
                            $afterHungNumber++
                        }
                        $starttim = $afterHung.start_time.ToString("yy\/MM\/dd HH:mm:ss")
                        $tim = ($afterHung.end_time).ToString("yy\/MM\/dd HH:mm:ss")
                        $idAfterHung = $afterHung.Job_ID
                        
                        for($i = $DBresult.count-$afterHungNumber; $i -gt 0; $i-- )
                        {
                            if ($DBresult[$i].End_Time -ne "01.01.1970 03:00:00")
                            {
                                $idAfterAfterHung = $DBresult[$i].Job_ID
                                $i = 0
                            }
                            
                        }
                        $query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and Start_time >= '$starttim'  ORDER BY id DESC;" 
                }
                
              
                else 
                {
                        
                    $query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and Start_time >= '$starttim'  ORDER BY id DESC;" 
                        
                    
                }                
                [array]$DBresult = $null
                try{
                    $DBresult = Invoke-AnywhereQueryDPC -query $query -connectionString $connection  
                }
                catch
                {
                    [array]$DBresult = $null
                }


                if($DBresult) #если завершенная, то анализируем её
                {
                    if ($newTask)
                    {
                        $query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and Start_time > '$starttim' and End_Time = '$zerotim' and size = 0 and speed = 0 ORDER BY id DESC;" 

                        [array]$DBresult = $null
                        $DBresult = Invoke-AnywhereQueryDPC -query $query -connectionString $connection
                        $endtime = ($DBresult[$DBresult.count-1].Start_Time).ToString("yy\/MM\/dd HH:mm:ss")
                        $query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and Start_time > '$starttim' and Start_time < '$endtime' ORDER BY id DESC;" 

                        [array]$DBresult = $null
                        $DBresult = Invoke-AnywhereQueryDPC -query $query -connectionString $connection
                        if ($DBresult)
                        {
                            $eachBackup = ProcessingTable -allBackups $DBresult
                        }
                        else #такой политики еще не было в базе и первый бэкап в процессе
                        {
                             $query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and Start_time > '$starttim' and End_Time = '$zerotim' and size = 0 and speed = 0  ORDER BY id DESC;" 

                            [array]$DBresult = $null
                            $DBresult = Invoke-AnywhereQueryDPC -query $query -connectionString $connection
                        
                             $eachBackup = noFinished -allBackups $DBresult
                        }

                    }
                    elseif ($DBresult.End_Time -eq "01.01.1970 03:00:00") # новая в процессе
                    {
                        if ( $SelectServer.End_Time -ne "01.01.1970 03:00:00")
                        {
                            try{
                                [array]$allInprogress = ($DBResult | Where-Object {(($_.End_Time) -eq "01.01.1970 03:00:00" -and $_.status -eq 0)})
                                $id = $allInprogress[$allInprogress.Count-1].Job_ID
                                $endtime = ($SelectServer.End_Time.AddSeconds(1)).ToString("yy\/MM\/dd HH:mm:ss")
                                if ($SelectServer.type -eq 5)
                                {
                                    if ($idAfterHung -eq "")
                                    {
                                        $query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and Start_time > '$endtime' and Job_ID < '$id'  ORDER BY id DESC;" 
                                    }
                                    else
                                    {
                                        $query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and Job_ID >= '$idAfterHung' and Job_ID < '$id' ORDER BY id DESC;" 
                                    }
                                }
                                else{    
                                    if ($idAfterHung -eq "")
                                    {
                                        $query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and Start_time > '$endtime' and Job_ID < '$id'  and size = 0 and speed = 0 ORDER BY id DESC;" 
                                    }
                                    else
                                    {
                                        $query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and Job_ID >= '$idAfterHung' and Job_ID < '$id'  and size = 0 and speed = 0 ORDER BY id DESC;" 
                                    }
                                }
                                [array]$DBresult = $null
                                $DBresult = Invoke-AnywhereQueryDPC -query $query -connectionString $connection

                                if ($DBresult -and $SelectServer.type -eq 5)
                                {
                                    $eachBackup = ProcessingTable -allBackups $DBresult
                                    [array]$DBresult = $null
                                }

                            }
                            catch{ # новая политика, такой еще нет в базе и она еще идет, добавляем ее в базу
                                [array]$DBresult = $null
                            }
                        }
                        else
                        {
                                $starttime = ($SelectServer.Start_time).ToString("yy\/MM\/dd HH:mm:ss")
                                $query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and Start_time >= '$starttime' and End_Time = '$zerotim'  and Client = '$client'  ORDER BY id DESC;" 
                                [array]$DBresult = $null
                                $DBresult = Invoke-AnywhereQueryDPC -query $query -connectionString $connection
                                $id = $DBresult[$DBresult.count-1].Job_ID
                        }
                        
                        if ($DBresult) #новая началась, добавляем предыдущие 
                        {
                            $endttime = $DBresult.Start_time[$DBresult.count-1].ToString("yy\/MM\/dd HH:mm:ss")  # время начала нового
                            if ($DBresult.type -eq 5)
                            {
                                
                                $id = $DBresult.Job_ID[$DBresult.count-1]
                            }
                            if ($SelectServer.End_Time -eq "01.01.1970 04:00:00")
                            {
                               $starttime = $tim
                               
                               $query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and Client = '$client' and id >= '$idAfterHung' and id < '$idAfterAfterHung' ORDER BY id DESC;"
                            }
                            else
                            {
                                $starttime = $SelectServer.End_time.ToString("yy\/MM\/dd HH:mm:ss") 
                                if ($starttime -eq "70/01/01 03:00:00")
                                {
                                    $starttime = $SelectServer.Start_time.ToString("yy\/MM\/dd HH:mm:ss")
                                }
                                $query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and Start_time >= '$starttime' and id < '$id' ORDER BY id DESC;"
                            }
                            
                            
                            [array]$DBresult = $null
                            $DBresult = Invoke-AnywhereQueryDPC -query $query -connectionString $connection
                            if ($DBresult.End_Time -eq "01.01.1970 03:00:00" -and $DBresult.type -eq 5) #началась новая, а текущая зависла (в Тран логах)
                            {
                                AddHungtoSql  -DBrow $SelectServer -table $SQLTable[$cod]
                            }
                            elseif ($DBresult) #закончилась, выбираем ее и добавляем
                            {
                                $eachBackup = ProcessingTable -allBackups $DBresult
                            }
                            
                            else # ищем предыдущие и добавляем текущую
                            {
                                $query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and Start_time > '$tim' and End_time > '$starttim' and Client = '$client' and id < '$id' ORDER BY id DESC;"
                                [array]$DBresult = $null
                                $DBresult = Invoke-AnywhereQueryDPC -query $query -connectionString $connection
                                if ($DBresult) #есть промежуточные
                                {
                                    #Добавляем сразу их
                                    $eachBackup = ProcessingTable -allBackups $DBresult
                                }
                                else
                                {
                                    $query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and Start_time > '$endtime' and End_Time = '$zerotim'  and Client = '$client' and size = 0 and speed = 0 ORDER BY id DESC;" 
                                    [array]$DBresult = $null
                                    $DBresult = Invoke-AnywhereQueryDPC -query $query -connectionString $connection
                                    if ($DBresult)
                                    {
                                        $eachBackup = noFinished -allBackups $DBresult
                                        
                                    }
                                }
                                
                            }
                        }
                        elseif($eachBackup -eq $null) { #текущая в процессе
                            $query="SELECT  id as 'Job_ID', clientName as 'Client', mediaServerName as 'Media_Server',MasterServerID as 'MasterServerID',scheduletype as 'Type', UTCBigIntToNomTime(startTime) as 'Start_Time', UTCBigIntToNomTime(endTime) as 'End_Time', statusCode as 'Status' ,policyName as 'Policy_Name', throughput as 'speed', bytesWritten as 'size' FROM domain_Job WHERE policyName='$Policy' and Start_time > '$tim'  ORDER BY id DESC;" 
                            [array]$DBresult = $null
                            $DBresult = Invoke-AnywhereQueryDPC -query $query -connectionString $connection
                            if ($DBresult.type -eq 5)
                            {
                                    $eachBackup = noFinished -allBackups $DBresult[$DBresult.Count - 1] -client $SelectServer.client
                                   
                            }
                            else
                            {
                                $eachBackup = $DBresult[$DBresult.Count - 1]
                            }
                        }
                    }

                    else{ #Обработка не зависших и текущих
                        $eachBackup = ProcessingTable -allBackups $DBresult
                        #$eachBackup | Out-GridView
                    }

                    foreach($DBrow in $eachBackup)
                    {
                        $newRow = AddToSQL -DBrow $DBrow  -table $SQLTable[$cod]
                        if($DBrow | Where-Object {($_.status -ne "0")})
                                    {
                                        $wrongstatus = ($eachBackup | Where-Object {($_.status -ne "0")})
                                        foreach ($wrongRow in $wrongstatus)
                                        {
                                            $res = "" | Select-Object Client,status,Policy_Name,Start_Time,End_Time,type,Segment
                                            $res.Client = $wrongRow.Client
                                            switch($wrongRow.status)
                                                {
                                                    "2" { $res.status = "[2]Файлы не скопированы"}
                                                    "6" { $res.status = "[6]Файлы не скопированы"}
                                                    "13" { $res.status = "[13]Файлы не прочитаны"}
                                                    "41" { $res.status = "[41]Connection timed out"}
                                                    "50" { $res.status = "[50]Процесс прерван"}
                                                    "54" { $res.status = "[54]Timed out connecting"}
                                                    "58" { $res.status = "[58]Не удалось подсоедениться"}
                                                    "84" { $res.status = "[84]Media write error"}
                                                    "86" { $res.status = "[86]Media position error"}
                                                    "150" { $res.status = "[150]Завершено администратором"}
                                                    "174" { $res.status = "[174]Системная ошибка"}
                                                    "247" { $res.status = "[247]Политика не активна"}
                                                    "2074" { $res.status = "[2074]Объем диска"}
                                                    "2106" { $res.status = "[2106]Сервер дискового хранилища отключен"}
                                                    default {$res.status = $wrongRow.status}
                                                }
                                            $res.Policy_Name = $wrongRow.Policy_Name
                                            $res.Start_Time = $wrongRow.Start_Time
                                            $res.End_Time = $wrongRow.End_Time   
                                            $res.Segment = $AnySQLSegment[$seg]
                                            $wrongResult = $wrongResult + $res
                                        }
                                    }
                    }
                   

                }

            }
          }
        }
    }
}

#SendErrorsPolicy
          
