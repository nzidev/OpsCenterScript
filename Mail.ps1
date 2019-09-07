$invocation = (Get-Variable MyInvocation).Value
$directorypath = Split-Path $invocation.MyCommand.Path
. "$directorypath\IncomingData.ps1"
. "$directorypath\SQLFunctions.ps1"
function ErrorMassage{
    param (
		[string]$server,
		[string]$segment
	)
    $body = "<p><span style='font-size:18px;'><strong><span style='color:#FF0000;'><span style='font-family:courier new,courier,monospace;'>Отвалился OPsCenter:</span></span></strong></span></p>" +
    "<br>" +
    $server + " / "+  $segment + "<br> <br>"  + 
    "<p><span style='font-size:11px;'><em>Скрипт выполняется автоматически на сервере. Ответственный Нестеровский И.С.</em></span></p>"
    
    $operator = 
    $smtpserver = 
    $enc = New-Object System.Text.utf8encoding
    $Topic = 'Ошибка OPsCenter'
    $priority = 'High'
    Send-MailMessage -from '' -to $operator -subject $Topic -Priority $priority -body $body -BodyAsHtml -smtpserver $smtpserver -Encoding $enc
}


function SendMail{
    param (
		[string]$body
	)
    
    #
    $operator = ''
    $smtpserver = ''
    $enc = New-Object System.Text.utf8encoding
    $Topic = 'Ошибка BackUP'
    $priority = 'Normal'
    Send-MailMessage -from '' -to $operator -subject $Topic -Priority $priority -body $body -BodyAsHtml -smtpserver $smtpserver -Encoding $enc
}


function SendErrorsPolicy {
    $SqlErrorSelect = "SELECT [Policy_Name],[Start_Time],[End_Time],[Segment],[Status] FROM [dbo].[Errors] WHERE ([Mail] = '')"
    $SqlErrors =  Invoke-DatabaseQuery –query "$SqlErrorSelect" –connectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True"  
    
    
        $wrongResult = @()
        foreach ($wrongRow in $SqlErrors)
        {

            

            if ($wrongRow.Policy_Name -like "*Archlog*")
            {
                $policy_name = $wrongRow.Policy_Name
                $SqlErrorSelect = "SELECT [Policy_Name],[Start_Time],[End_Time],[Segment],[Status] FROM [dbo].[Errors] WHERE ([Mail] = '' and [Policy_Name] = '$policy_name')"
                $SqlErrors =  Invoke-DatabaseQuery –query "$SqlErrorSelect" –connectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True"  
                if ($SqlErrors[0] -ge 3)
                {
                    $res = "" | Select-Object status,Policy_Name,Start_Time,End_Time,Segment
                    
                    $res.status = $wrongRow.status 
                    $res.Policy_Name = $wrongRow.Policy_Name
                    $res.Start_Time = $wrongRow.Start_Time
                    $res.End_Time = $wrongRow.End_Time   
                    $res.Segment = $wrongRow.Segment
             
                    $policy_name = $wrongRow.Policy_Name
                    $Start_time = $wrongRow.Start_Time
                    $end_time = $wrongRow.End_Time 
                    $wrongResult = $wrongResult + $res
                
                    $SqlErrorUpdate = "Update [dbo].[Errors] SET [Mail] = 'True'  WHERE ([Policy_Name] = '$Policy_Name')"
                    Invoke-DatabaseQuery –query "$SqlErrorUpdate" –connectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True" 
                }
            }
            elseif ($wrongRow.Policy_Name -ne $null)
            {
                $res = "" | Select-Object status,Policy_Name,Start_Time,End_Time,Segment
                    
                $res.status = $wrongRow.status 
                $res.Policy_Name = $wrongRow.Policy_Name
                $res.Start_Time = $wrongRow.Start_Time
                $res.End_Time = $wrongRow.End_Time   
                $res.Segment = $wrongRow.Segment
             
                $policy_name = $wrongRow.Policy_Name
                $Start_time = $wrongRow.Start_Time
                $end_time = $wrongRow.End_Time 
                $wrongResult = $wrongResult + $res
                
                $SqlErrorUpdate = "Update [dbo].[Errors] SET [Mail] = 'True'  WHERE ([Policy_Name] = '$Policy_Name' and [Start_Time] = '$Start_Time' and [End_Time] = '$End_Time')"
                Invoke-DatabaseQuery –query "$SqlErrorUpdate" –connectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True"  
            }

        }



        $a = "<style>"
        $a = $a + "TABLE{border-width: 1px;border-style: solid;border-color: black;border-collapse: collapse;}"
        $a = $a + "TH{border-width: 1px;padding: 3px;border-style: solid;border-color: black;}"
        $a = $a + "TD{border-width: 1px;padding: 3px;border-style: solid;border-color: black;}"
        $a = $a + "</style>"
    
        $wrongResult_html = $wrongResult | ConvertTo-Html -Head $a

        $body = "<p><span style='font-size:18px;'><strong><span style='color:#0000FF;'><span style='font-family:courier new,courier,monospace;'>Были ошибки при выполнении политик резервного копирования:</span></span></strong></span></p>" +

        "<br>" +

        $wrongResult_html  + "<br> <br>"  + 
        "<p><span style='font-size:11px;'><em>Скрипт выполняется автоматически на сервере. Ответственный Нестеровский И.С.</em></span></p>"

        if ($wrongResult)
        {
            SendMail -body $body
        }
}

