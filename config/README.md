# config/

`pipeline.example.yaml` es la plantilla versionada. Para correr:

```bash
cp config/pipeline.example.yaml config/pipeline.local.yaml
```

`pipeline.local.yaml` está en `.gitignore`. **Nunca commitear** credenciales, tokens ni el
secure connect bundle de AstraDB: la consigna lo prohíbe explícitamente (§8.2) y es un ítem
del criterio de aceptación final.
