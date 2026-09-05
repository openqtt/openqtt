:: The batch file for 'openqtt ctl' command

@set args=%*

:: Discover the release root directory from the directory
:: of this script
@set script_dir=%~dp0
@for %%A in ("%script_dir%\..") do @(
  set rel_root_dir=%%~fA
)
@%rel_root_dir%\bin\openqtt.cmd ctl %args%
