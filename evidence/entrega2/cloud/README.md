# Evidencia · perfil cloud (fase 2: Colab + AstraDB)

Plan 2.9, decisiones D17 y D22. Mismo código que el perfil local: solo cambian
`serving.provider: astradb` y la ruta del secure connect bundle (`config/pipeline.cloud.example.yaml`).

| Prueba | Cómo se regenera | Resultado | Archivo |
|---|---|---|---|
| B3 en AstraDB: escritura y lectura con el connector, upsert sin duplicar, `TRUNCATE` con el driver | `make check-astra` | OK | [`check_astra.txt`](check_astra.txt) |
| Carga de Gold y consultas P1/P2 en Astra | `make serving PROFILE=cloud` | 11.050 y 262 filas, igual a Gold; P1 = 34 filas, P2 = 5 | [`../serving/queries_astradb.md`](../serving/queries_astradb.md) |
| P1/P2: Astra contra Cassandra local | `diff` de los dos archivos sin la cabecera | Mismas filas y valores | [`../serving/queries_cassandra.md`](../serving/queries_cassandra.md) |
| Idempotencia O6 con serving en Astra (3 corridas) | `make idempotency PROFILE=cloud` | Conteos y sumas idénticos, 0 duplicados | [`../idempotencia/astradb.md`](../idempotencia/astradb.md) |
| `02_pipeline.ipynb` de punta a punta en un runtime tipo Colab (Python 3.12, numpy 2, Java 17 por `apt`), desde cero y contra Astra | `make colab-emu` | 6 etapas OK, sin errores | [`runs_colab_emulado.jsonl`](runs_colab_emulado.jsonl), [`queries_colab_emulado.md`](queries_colab_emulado.md) |
| Notebook en Google Colab real y P1/P2 desde la consola de Astra | Manual (ver README, "Perfil cloud") | **Pendiente** | `colab_run.png`, `astra_console_p1.png`, `astra_console_p2.png` |

## Emulador de Colab

`infra/colab/` levanta una imagen con lo que trae Colab (Python 3.12, numpy 2, pandas 2.x, Jupyter) y
corre el notebook con `nbconvert` igual que *Run all*, con `CLEAN = True` y el perfil cloud. Instala
lo mismo que la celda de setup del notebook, que en Colab real se ejecuta y en el emulador se saltea
porque no hay `google.colab`. Lo que el emulador **no** prueba: el panel de Secrets, el selector de
archivos para subir el bundle y el `git clone` (copia el repo local).

Encontró un problema que sin él habría aparecido recién en Colab: PySpark 3.5 importa `distutils` en
`toPandas()` y Python 3.12 ya no lo trae (`ModuleNotFoundError`). La celda de setup ahora instala
`setuptools` cuando el Python es 3.12 o mayor (D22).

**Tiempo:** el pipeline tarda ~1 minuto (la etapa más lenta es el serving: ~35 s de ida y vuelta a
Astra). La primera corrida, además, descarga el connector y sus 16 dependencias desde Maven Central,
y eso puede tardar bastante más. El emulador guarda esa descarga en el volumen `colab-emu-ivy`; en Colab
se repite en cada sesión nueva.

## Capturas pendientes (Colab real)

1. Correr `notebooks/02_pipeline.ipynb` en Colab según el README y capturar la tabla de resumen
   (estado de cada etapa) → `colab_run.png`.
2. En la consola de Astra (*CQL Console*), `USE cloud_analytics;` y pegar P1 y P2 de
   [`src/serving/cql/03_queries.cql`](../../../src/serving/cql/03_queries.cql) → `astra_console_p1.png`,
   `astra_console_p2.png`.
3. Revisar que ninguna captura muestre el token.
