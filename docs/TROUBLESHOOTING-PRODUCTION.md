# Guía de Resolución de Problemas - Producción

Cuando **jobs** o **sync** fallan en producción, sigue esta guía paso a paso.

## Diagnóstico Rápido

### 1. Verificar el endpoint de salud

Visita (sin autenticación):
```
https://gmc-mobility-api.vercel.app/api/health
```

Interpreta la respuesta:
- `status: "healthy"` → Todo OK
- `status: "degraded"` → Revisa qué `checks` tienen `status: "error"`

**Checks críticos:**
| Check | Si falla → |
|-------|------------|
| `environment` | Faltan variables en Vercel. Ver sección Variables de Entorno. |
| `s3Config` | AWS credentials mal configuradas o faltantes. |
| `storage` | No puede leer jobs.json en S3. Verifica bucket y permisos. |
| `verasetApi` | VERASET_API_KEY faltante o inválida. |
| `verasetS3Access` | **CRÍTICO para sync**: Las credenciales AWS no tienen acceso al bucket de Veraset. |

### 2. Ejecutar script de diagnóstico local

Con las mismas variables que usa producción (copia de .env de Vercel):

```bash
npx tsx scripts/diagnose-production.ts
```

Esto prueba:
- Variables de entorno
- Acceso a nuestro bucket S3
- **Acceso al bucket de Veraset** (si falla aquí, el sync nunca funcionará)
- Conectividad con la API de Veraset

---

## Problemas Comunes

### "Jobs no se crean" / Error al crear job

**Causas probables:**
1. **VERASET_API_KEY** no configurada en Vercel
2. **AUTH_SECRET** no configurada → el middleware rechaza requests
3. Límite de uso mensual alcanzado
4. Error de validación (POIs, fechas, etc.)

**Solución:**
1. Vercel Dashboard → Settings → Environment Variables
2. Verifica: `VERASET_API_KEY`, `AUTH_SECRET`, `AWS_*`
3. **Redeploy** después de cambiar variables

### "Sync falla" / 500 en /sync/status o /sync/stream

**Causa más común:** Las credenciales AWS **no tienen acceso al bucket de Veraset**.

El sync copia desde:
- **Origen:** `s3://veraset-prd-platform-us-west-2/output/garritz/{jobId}/`
- **Destino:** `s3://garritz-veraset-data-us-west-2/...`

Tus credenciales AWS necesitan:
1. **Lectura** en `veraset-prd-platform-us-west-2` (bucket de Veraset)
2. **Lectura/Escritura** en `garritz-veraset-data-us-west-2` (tu bucket)

**Si `verasetS3Access` falla en /api/health:**
- Veraset debe otorgar acceso cross-account a tu cuenta AWS
- Contacta a Veraset para que configuren la política IAM/bucket policy
- Sin esto, el sync **nunca** funcionará

### "405 Method Not Allowed" en /sync

Ya corregido en el código. Si persiste, verifica que el cliente use **POST** para iniciar sync (no GET).

### Variables de entorno no se aplican

**Importante:** En Vercel, al agregar o modificar variables:
1. Debes hacer **Redeploy** del proyecto
2. Las variables no se aplican a deployments existentes
3. Verifica que estén marcadas para **Production** (no solo Preview)

---

## Checklist de Variables en Vercel

| Variable | Obligatoria | Uso |
|----------|-------------|-----|
| AUTH_SECRET | ✅ | Middleware de autenticación |
| VERASET_API_KEY | ✅ | Crear jobs, consultar estado |
| AWS_ACCESS_KEY_ID | ✅ | S3 config + sync |
| AWS_SECRET_ACCESS_KEY | ✅ | S3 config + sync |
| AWS_REGION | ✅ | us-west-2 |
| S3_BUCKET | ✅ | garritz-veraset-data-us-west-2 |
| ALLOWED_ORIGINS | Recomendado | https://gmc-mobility-api.vercel.app |

---

## Logs en Vercel

Para ver errores específicos:

1. Vercel Dashboard → Tu proyecto → **Deployments**
2. Clic en el deployment activo
3. Pestaña **Logs** o **Functions**
4. Busca: `[JOBS POST]`, `[SYNC]`, `Error`, `Failed`

---

## Contacto con Veraset

Si el problema es **acceso al bucket S3 de Veraset**:
- Tu cuenta AWS necesita permisos cross-account
- Solo Veraset puede configurar esto en su bucket
- Proporciónales tu AWS Account ID y la región (us-west-2)
