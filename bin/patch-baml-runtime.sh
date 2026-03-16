#!/usr/bin/env bash
set -euo pipefail

python3 <<'PY'
from pathlib import Path
import re

path = Path("runtime.go")
text = path.read_text()

import_line = '\t"github.com/beam-cloud/airstore/pkg/bamlregistry"\n'
target_import = '\tbaml "github.com/boundaryml/baml/engine/language_client_go/pkg"\n'

if import_line not in text:
    text = text.replace(target_import, import_line + target_import)

text = text.replace("baml.SetTypeMap(typeMap)", "bamlregistry.RegisterTypeMap(typeMap)")

path.write_text(text)

enums_path = Path("types/enums.go")
if enums_path.exists():
    enums_text = enums_path.read_text()
    enums_text = re.sub(
        r'(if name\.Name != "[^"]+" )&& (name\.Namespace != cffi\.CFFITypeNamespace_TYPES \{)',
        r"\1|| \2",
        enums_text,
    )
    enums_path.write_text(enums_text)

resolve_client_block = '''\n\t// Resolve client option to clientRegistry (client takes precedence)\n\tif callOpts.client != nil {\n\t\tif callOpts.clientRegistry == nil {\n\t\t\tcallOpts.clientRegistry = baml.NewClientRegistry()\n\t\t}\n\t\tcallOpts.clientRegistry.SetPrimaryClient(*callOpts.client)\n\t}\n'''

for parse_path in (Path("functions_parse.go"), Path("functions_parse_stream.go")):
    if not parse_path.exists():
        continue
    parse_text = parse_path.read_text()
    if resolve_client_block not in parse_text:
        parse_text = re.sub(
            r'(\tfor _, opt := range opts \{\n\t\topt\(&callOpts\)\n\t\}\n)',
            r'\1' + resolve_client_block,
            parse_text,
            count=1,
        )
    parse_path.write_text(parse_text)
PY
