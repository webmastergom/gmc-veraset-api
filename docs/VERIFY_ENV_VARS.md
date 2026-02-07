# Verificación de Variables de Entorno en Vercel

## Problema: Error 405 Method Not Allowed

Si estás viendo un error `405 (Method Not Allowed)` en producción pero funciona en local, es muy probable que falte alguna variable de entorno crítica en Vercel.

## Variables de Entorno Críticas

### 1. AUTH_SECRET (OBLIGATORIA)

**Por qué es crítica**: El middleware usa `process.env.AUTH_SECRET` para validar cookies de autenticación. Si falta, puede causar problemas de routing.

**Cómo verificar en Vercel**:
1. Ve a [Vercel Dashboard](https://vercel.com)
2. Selecciona tu proyecto: `gmc-mobility-api`
3. Ve a **Settings** → **Environment Variables**
4. Busca `AUTH_SECRET`
5. Verifica que esté configurada para **Production** (no solo Preview/Development)

**Cómo generar si falta**:
```bash
openssl rand -base64 32
```

**Valor esperado**: Una cadena de 32+ caracteres en base64

---

### 2. VERASET_API_KEY (OBLIGATORIA)

**Por qué es crítica**: Se usa para todas las llamadas a la API de Veraset.

**Cómo verificar**: Mismo proceso que arriba, busca `VERASET_API_KEY`

**Valor esperado**: Tu API key de Veraset (sin espacios al inicio/final)

---

### 3. AWS Credentials (OBLIGATORIAS para análisis de datasets)

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION` (debe ser `us-west-2`)
- `S3_BUCKET` (debe ser `garritz-veraset-data-us-west-2`)

**Por qué son críticas**: El endpoint `/api/datasets/[name]/analyze` usa AWS Athena, que requiere estas credenciales.

---

### 4. ALLOWED_ORIGINS (RECOMENDADA)

**Por qué es importante**: Controla qué orígenes pueden hacer requests CORS.

**Valor recomendado**: `https://gmc-mobility-api.vercel.app`

---

## Checklist de Verificación Rápida

Ejecuta este checklist en Vercel Dashboard → Settings → Environment Variables:

- [ ] `AUTH_SECRET` existe y está configurada para **Production**
- [ ] `VERASET_API_KEY` existe y está configurada para **Production**
- [ ] `AWS_ACCESS_KEY_ID` existe y está configurada para **Production**
- [ ] `AWS_SECRET_ACCESS_KEY` existe y está configurada para **Production**
- [ ] `AWS_REGION` existe y tiene valor `us-west-2`
- [ ] `S3_BUCKET` existe y tiene valor `garritz-veraset-data-us-west-2`
- [ ] `ALLOWED_ORIGINS` existe (opcional pero recomendado)

---

## Cómo Verificar que las Variables Están Disponibles

### Opción 1: Verificar en los Logs de Vercel

1. Ve a Vercel Dashboard → Tu Proyecto → **Deployments**
2. Haz clic en el deployment más reciente
3. Ve a la pestaña **Logs**
4. Busca errores como:
   - `AUTH_SECRET not configured`
   - `AWS credentials not configured`
   - `VERASET_API_KEY not configured`

### Opción 2: Crear un Endpoint de Debug Temporal

Puedes crear temporalmente un endpoint para verificar las variables:

```typescript
// app/api/debug/env/route.ts (TEMPORAL - ELIMINAR DESPUÉS)
import { NextResponse } from 'next/server';

export async function GET() {
  // SOLO PARA DEBUG - ELIMINAR EN PRODUCCIÓN
  return NextResponse.json({
    hasAuthSecret: !!process.env.AUTH_SECRET,
    hasVerasetKey: !!process.env.VERASET_API_KEY,
    hasAwsKey: !!process.env.AWS_ACCESS_KEY_ID,
    hasAwsSecret: !!process.env.AWS_SECRET_ACCESS_KEY,
    nodeEnv: process.env.NODE_ENV,
  });
}
```

Luego visita: `https://gmc-mobility-api.vercel.app/api/debug/env`

**⚠️ IMPORTANTE**: Elimina este endpoint después de verificar, ya que expone información sensible.

---

## Pasos para Resolver el Problema

1. **Verifica todas las variables** usando el checklist arriba
2. **Genera `AUTH_SECRET`** si falta:
   ```bash
   openssl rand -base64 32
   ```
3. **Agrega las variables faltantes** en Vercel Dashboard
4. **Asegúrate de seleccionar "Production"** al agregar cada variable
5. **Redespliega** el proyecto después de agregar/modificar variables:
   - Ve a **Deployments**
   - Haz clic en los tres puntos del deployment más reciente
   - Selecciona **Redeploy**

---

## Notas Importantes

- **Las variables de entorno NO se aplican automáticamente**: Después de agregar/modificar variables, debes hacer un **Redeploy**
- **Verifica el entorno correcto**: Asegúrate de que las variables estén configuradas para **Production**, no solo Preview/Development
- **Sin espacios**: Al copiar/pegar valores, asegúrate de no agregar espacios al inicio/final
- **Case sensitive**: Los nombres de las variables son case-sensitive (`AUTH_SECRET` no es lo mismo que `auth_secret`)

---

## Si el Problema Persiste

Si después de verificar todas las variables y redesplegar el problema persiste:

1. Revisa los logs de Vercel para ver errores específicos
2. Verifica que el deployment más reciente se haya completado exitosamente
3. Intenta hacer un **Redeploy** manual desde Vercel Dashboard
4. Verifica que no haya errores de compilación en el build
