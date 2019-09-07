############################################## Р¤СѓРЅРєС†РёСЏ СЂР°Р±РѕС‚С‹ СЃ SQL #######################################################
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


$SQLServerAtlas = "" 
$SQLDBNameAtlas = "ServersDB"

$SQLServer = "" 
$SQLDBName = ""
$SQLSelect = "SELECT Client,Policy_Name FROM  UNION  select client,Policy_Name from  UNION  select client,Policy_Name from  order by Client"
$SelectServer = Invoke-DatabaseQuery –query "$SqlSelect" –connectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True" 
foreach ($Obj in $SelectServer)
{
    $policyObj = $Obj.Policy_Name
    $clientName = ($obj.Client -split "\.")[0]

    $clientName

  

    $SQLSelect = "SELECT ID FROM .[Network]  where name = '$clientName' and deleted = '0'"
    $SelectServer = Invoke-DatabaseQuery –query "$SqlSelect" –connectionString "Server = $SQLServerAtlas; Database = $SQLDBNameAtlas; Integrated Security = True" 
    $networkIP = $SelectServer.id



    $SQLSelect = "SELECT * FROM .[Servers]  where [NetworkID] = '$networkIP'"
    $SelectServer = Invoke-DatabaseQuery –query "$SqlSelect" –connectionString "Server = $SQLServerAtlas; Database = $SQLDBNameAtlas; Integrated Security = True" 
    $Role = $SelectServer.Description

    $domainID = $SelectServer.DomainID

    $SQLSelect = "SELECT * FROM .[Domains]  where [ID] = '$domainID'"
    $SelectServer = Invoke-DatabaseQuery –query "$SqlSelect" –connectionString "Server = $SQLServerAtlas; Database = $SQLDBNameAtlas; Integrated Security = True" 
    $domain = $SelectServer.Domain_name

    $domain


    $SQLSelect = "SELECT * FROM .[Servers]  where [NetworkID] = '$networkIP'"
    $SelectServer = Invoke-DatabaseQuery –query "$SqlSelect" –connectionString "Server = $SQLServerAtlas; Database = $SQLDBNameAtlas; Integrated Security = True" 
    $Role = $SelectServer.Description


    $Role

    $ComplexID = $SelectServer.ComplexID
    $SubsystemID = $SelectServer.SubsystemID

    $SQLSelect = "SELECT * FROM .[Complex]  where Id = '$ComplexID'"
    $SelectServer = Invoke-DatabaseQuery –query "$SqlSelect" –connectionString "Server = $SQLServerAtlas; Database = $SQLDBNameAtlas; Integrated Security = True" 
    $Complex = $SelectServer.Title

    $Complex

    $SQLSelect = "SELECT * FROM .[Subsystem]  where Id = '$SubsystemID'"
    $SelectServer = Invoke-DatabaseQuery –query "$SqlSelect" –connectionString "Server = $SQLServerAtlas; Database = $SQLDBNameAtlas; Integrated Security = True" 
    $Subsystem = $SelectServer.Title
    $Subsystem

    $nameFQDN = $clientName + "." + $domain
    $nameFQDN
    



    $SQLSelect = "SELECT Policy FROM .[Descriptions]  where [Client] = '$nameFQDN'"
    $SelectServer = Invoke-DatabaseQuery –query "$SqlSelect" –connectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True" 
    if ($SelectServer)
    {
        if (!(Invoke-DatabaseQuery –query "SELECT Policy FROM .[Descriptions]  where [Policy] = '$policyObj'" –connectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True"))
        {
            $Sqlinsert = "insert into .[Descriptions] values (N'$policyObj',N'',N'',N'','','',N'$nameFQDN')"
            Invoke-DatabaseQuery –query "$Sqlinsert" –connectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True"
        }
        $Sqlupdate = "update .[Descriptions] SET [Kompleks] = '$Complex', [Podsistema] = '$Subsystem', [Resurs] = '$Role' WHERE ([Client] = '$nameFQDN')"
        Invoke-DatabaseQuery –query "$Sqlupdate" –connectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True"   
    }
   elseif (Invoke-DatabaseQuery –query "SELECT Policy FROM .[Descriptions]  where [Client] = '$clientName'" –connectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True")
   {
       if (!(Invoke-DatabaseQuery –query "SELECT Policy FROM .[Descriptions]  where [Policy] = '$policyObj'" –connectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True"))
            {
                $Sqlinsert = "insert into .[Descriptions] values (N'$policyObj',N'',N'',N'','','',N'$clientName')"
                Invoke-DatabaseQuery –query "$Sqlinsert" –connectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True"
            }
        $Sqlupdate = "update .[Descriptions] SET [Kompleks] = '$Complex', [Podsistema] = '$Subsystem', [Resurs] = '$Role' WHERE ([Client] = '$clientName')"
        Invoke-DatabaseQuery –query "$Sqlupdate" –connectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True"   
    }
    elseif ($policyObj -ne $null -and ($role -OR $Complex -or $Subsystem))
    {

        $Sqlinsert = "insert into .[Descriptions] values (N'$policyObj',N'$Role',N'$Complex',N'$Subsystem','','',N'$nameFQDN')"
        Invoke-DatabaseQuery –query "$Sqlinsert" –connectionString "Server = $SQLServer; Database = $SQLDBName; Integrated Security = True"  
    }
    
    
   
}