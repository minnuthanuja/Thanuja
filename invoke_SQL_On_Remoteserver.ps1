import-module SqlServer
Install-Module -Name SqlServer -AllowClobber -Scope CurrentUser

set-executionpolicy remotesigned -Scope CurrentUser

Invoke-Sqlcmd




