# -*- mode: python ; coding: utf-8 -*-

import subprocess
from pathlib import Path

block_cipher = None

tag = subprocess.check_output(["git", "describe", "--abbrev=0", "--tags"]).decode('utf-8').strip()
module_path = str(Path().absolute())
module_name = Path().absolute().parts[-1]

a = Analysis([f'launch_{module_name}.py'],
             pathex=[module_path],
             binaries=[],
             datas=[],
             hiddenimports=['pkg_resources.py2_warn'],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)

exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          [],
          name=f'{module_name}-{tag}',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          upx_exclude=[],
          runtime_tmpdir=None,
          console=True )
