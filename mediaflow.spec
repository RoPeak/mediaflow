# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path

from PyInstaller.utils.hooks import collect_data_files
from PyInstaller.utils.hooks import collect_submodules

datas = []
hiddenimports = []
datas += collect_data_files("babelfish")
datas += collect_data_files("guessit")
hiddenimports += collect_submodules("mediashrink")
hiddenimports += collect_submodules("plexify")
hiddenimports += collect_submodules("babelfish")

entrypoint = str(Path("packaging") / "pyinstaller_entry.py")
sibling_paths = [str(Path("..") / "mediashrink"), str(Path("..") / "plexify")]


a = Analysis(
    [entrypoint],
    pathex=sibling_paths,
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="mediaflow",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="mediaflow",
)