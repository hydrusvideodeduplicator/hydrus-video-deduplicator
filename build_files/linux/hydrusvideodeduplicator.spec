# -*- mode: python ; coding: utf-8 -*-

import os

# Use __main__.py rather than the CLI exe entrypoint. The CLI exe entrypoint waits for ENTER before
# exiting, which is helpful when Windows closes the console window but just gets in the way here.
a = Analysis(
    [os.path.join(SPECPATH, '..', '..', 'src', 'hydrusvideodeduplicator', '__main__.py')],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[],
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
    a.binaries,
    a.datas,
    [],
    name='hydrusvideodeduplicator-linux-x86_64',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
