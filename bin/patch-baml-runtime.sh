#!/usr/bin/env bash
set -euo pipefail

python3 <<'PY'
from pathlib import Path

path = Path("runtime.go")
text = path.read_text()

import_line = '\t"github.com/beam-cloud/airstore/pkg/bamlregistry"\n'
target_import = '\tbaml "github.com/boundaryml/baml/engine/language_client_go/pkg"\n'

if import_line not in text:
    text = text.replace(target_import, import_line + target_import)

text = text.replace("baml.SetTypeMap(typeMap)", "bamlregistry.RegisterTypeMap(typeMap)")

path.write_text(text)
PY
