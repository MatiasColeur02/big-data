# Emulador del runtime de Google Colab para validar el perfil cloud sin abrir Colab (D22).
#
# Colab trae Python 3.12+, numpy 2, pandas 2.x y Jupyter; NO trae Java 17 ni el connector ya resuelto.
# Esta imagen reproduce eso. El resto (Java 17 por apt ya incluido acá, los tres paquetes fijados y
# setuptools) lo instala run.sh igual que la celda "Setup de Colab" de notebooks/02_pipeline.ipynb.
#
#   make colab-emu      # ver infra/colab/run.sh
#
# Base bookworm: Debian 13 (trixie) ya no trae OpenJDK 17; Colab (Ubuntu 22.04) sí.
FROM python:3.12-slim-bookworm
RUN apt-get update -qq \
 && apt-get install -y -qq --no-install-recommends openjdk-17-jre-headless procps > /dev/null \
 && rm -rf /var/lib/apt/lists/*
RUN pip install -q --no-cache-dir "numpy>=2,<3" "pandas>=2.2,<3" tabulate nbconvert ipykernel
WORKDIR /content
