@echo off
title *******************************************************dlls files copying script***********************************************************************
color 0A
echo.
xcopy /y C:\work\tips\pts\C462\common\02-Web\Tips.Pts.C462.Common.Web\bin\Debug\Tips.Pts.C462.Common.Web.dll C:\debug\dbgshr
xcopy /y C:\work\tips\pts\C462\common\02-Web\Tips.Pts.C462.Common.Web\bin\Debug\Tips.Pts.C462.Common.Web.dll.config C:\debug\dbgshr
xcopy /y C:\work\tips\pts\C462\common\02-Web\Tips.Pts.C462.Common.Web\bin\Debug\Tips.Pts.C462.Common.Web.xml C:\debug\dbgshr
xcopy /y C:\work\tips\pts\C462\common\02-Web\Tips.Pts.C462.Common.Web\bin\Debug\Tips.Pts.C462.Common.Web.pdb C:\debug\dbgshr
echo.
echo --------------------------------------------------------------------
echo all C462.common.web dlls files has been copied to C:\debug\dbgshr
echo --------------------------------------------------------------------
echo.
pause