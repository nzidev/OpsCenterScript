$invocation = (Get-Variable MyInvocation).Value
$directorypath = Split-Path $invocation.MyCommand.Path
. "$directorypath\SQLFunctions.ps1"
. "$directorypath\IncomingData.ps1"

deleteSQLrunningPolicy