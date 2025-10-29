@echo off
setlocal enabledelayedexpansion

rem Change directory to where your files are located
cd /d "C:\Users\DhivakarJeganathan\Desktop\Work\Quick\Cursor\TNCP R\SAP Tables\corning_aupf_acpf\ACPF-AUPF\VCU95"

for %%f in (*_VCU_*) do (
    set "newname=%%f"
    set "newname=!newname:_VCU_=_ACPF_VCU_!"
    echo Renaming "%%f" to "!newname!"
    ren "%%f" "!newname!"
)

echo Done!
pause
