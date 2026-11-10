#!/usr/bin/env bash
# Corre notebooks/02_pipeline.ipynb de punta a punta en el emulador de Colab, con perfil cloud (D22).
# Se ejecuta dentro de la imagen de infra/colab/Dockerfile; lo lanza `make colab-emu`.
#
#   /repo   el repo, montado solo lectura (incluye .env y el bundle, que no salen del contenedor)
#   /out    evidence/entrega2/cloud/, donde queda el registro de corridas
#
# Copia el repo a /content/big-data como haría el `git clone` de la celda de setup, instala lo mismo
# que esa celda y ejecuta el notebook con CLEAN = True. El notebook ejecutado no se guarda: sus
# salidas muestran rutas del entorno (plan 2.9).
set -euo pipefail

CLONE=/content/big-data
mkdir -p "$CLONE"
tar -C /repo --exclude='./data/datalake/bronze' --exclude='./data/datalake/silver' \
    --exclude='./data/datalake/gold' --exclude='./data/datalake/quarantine' \
    --exclude='./data/datalake/_checkpoints' --exclude='./data/datalake/_metadata' \
    --exclude='./data/sandbox' --exclude='./.git' -cf - . | tar -C "$CLONE" -xf -

# Igual que la celda de setup: solo los paquetes del pipeline, fijados, más setuptools (distutils).
cd "$CLONE"
pip install -q --no-cache-dir $(grep -E '^(pyspark|cassandra-driver|pyyaml)==' requirements.txt) setuptools

[ -f "$ASTRA_BUNDLE_PATH" ] || { echo "falta el bundle en $ASTRA_BUNDLE_PATH"; exit 1; }
[ -n "${ASTRA_DB_TOKEN:-}" ] || { echo "falta ASTRA_DB_TOKEN (.env)"; exit 1; }

echo "Python $(python -V | cut -d' ' -f2) · $(java -version 2>&1 | head -1) · $(pip list 2>/dev/null | grep -iE '^(pyspark|numpy|pandas) ' | tr -s ' ' | tr '\n' ' ')"

cd notebooks
python - <<'EOF'
import json
nb = json.load(open("02_pipeline.ipynb"))
params = next(c for c in nb["cells"] if "CLEAN = False" in "".join(c["source"]))
params["source"] = ["".join(params["source"]).replace("CLEAN = False", "CLEAN = True")]
json.dump(nb, open("_run.ipynb", "w"))   # en notebooks/: el kernel arranca en la carpeta del notebook
EOF
start=$(date +%s)
jupyter nbconvert --to notebook --execute _run.ipynb --output-dir /tmp --output executed \
    --ExecutePreprocessor.timeout=3600
echo "notebook ejecutado sin errores en $(( $(date +%s) - start )) s"

cp "$CLONE/data/datalake/_metadata/runs/runs.jsonl" /out/runs_colab_emulado.jsonl
cp "$CLONE/evidence/entrega2/serving/queries_astradb.md" /out/queries_colab_emulado.md
echo "evidencia en evidence/entrega2/cloud/"
