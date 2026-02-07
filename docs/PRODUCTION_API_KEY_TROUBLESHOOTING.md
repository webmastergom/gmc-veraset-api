# Troubleshooting: Veraset API Key en Producción

## Problema
La API key funciona en local pero falla en producción en Vercel.

## Posibles Causas y Soluciones

### 1. Variable de Entorno no Configurada en Vercel

**Síntoma**: Error "VERASET_API_KEY not configured"

**Solución**:
1. Ve a [Vercel Dashboard](https://vercel.com)
2. Selecciona tu proyecto
3. Ve a **Settings** → **Environment Variables**
4. Verifica que `VERASET_API_KEY` existe
5. **IMPORTANTE**: Asegúrate de que está configurada para **Production** (no solo Preview/Development)
6. Si no existe, agrégalo:
   - Key: `VERASET_API_KEY`
   - Value: Tu API key de Veraset
   - Environment: Selecciona **Production** (y opcionalmente Preview/Development)
7. **Redespliega** después de agregar/modificar la variable

### 2. Espacios en Blanco en la API Key

**Síntoma**: Error 401 "Invalid API key" aunque la key es válida

**Solución**:
- El código ahora usa `.trim()` automáticamente
- Pero verifica en Vercel que no hay espacios al inicio/final cuando copias/pegas
- Si hay espacios, elimínalos manualmente en Vercel

### 3. Formato de date_range Incorrecto

**Síntoma**: Error 400 "Invalid request" o "date_range is required"

**Solución**:
- El código ahora normaliza el formato automáticamente
- Verifica que el formato sea:
  ```json
  {
    "date_range": {
      "from_date": "2026-01-10",
      "to_date": "2026-01-12"
    }
  }
  ```

### 4. Variables de Entorno no Disponibles en Runtime

**Síntoma**: La variable existe en Vercel pero el código no la ve

**Solución**:
- Verifica que la variable está en el entorno correcto (Production)
- **Redespliega** después de agregar variables de entorno
- Vercel requiere un nuevo deploy para que las variables estén disponibles

### 5. API Key Revocada o Inválida

**Síntoma**: Error 401 persistente

**Solución**:
- Verifica en el dashboard de Veraset que la API key está activa
- Genera una nueva API key si es necesario
- Actualiza la variable en Vercel y redespliega

## Cómo Verificar el Problema

### 1. Revisar Logs en Vercel

1. Ve a Vercel Dashboard → Tu Proyecto → **Deployments**
2. Selecciona el último deployment
3. Ve a **Functions** → Busca `/api/jobs`
4. Revisa los logs para ver:
   - Si `VERASET_API_KEY` está configurada
   - El error exacto de Veraset API
   - El status code de la respuesta

### 2. Verificar Variables de Entorno

Crea un endpoint temporal para verificar:

```typescript
// app/api/debug/env/route.ts
export async function GET() {
  return NextResponse.json({
    hasVerasetKey: !!process.env.VERASET_API_KEY,
    keyLength: process.env.VERASET_API_KEY?.length || 0,
    keyPrefix: process.env.VERASET_API_KEY?.substring(0, 10) || 'missing',
    nodeEnv: process.env.NODE_ENV,
  });
}
```

**IMPORTANTE**: Elimina este endpoint después de debuggear por seguridad.

### 3. Probar Directamente la API Key

```bash
# Reemplaza YOUR_API_KEY con tu key real
curl -X POST https://platform.prd.veraset.tech/v1/movement/job/pings \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "date_range": {
      "from_date": "2026-01-10",
      "to_date": "2026-01-12"
    },
    "place_key": [
      {
        "poi_id": "Test",
        "placekey": "zzy-24c@53v-7xp-xwk"
      }
    ],
    "schema_type": "BASIC"
  }'
```

## Checklist de Verificación

- [ ] `VERASET_API_KEY` está configurada en Vercel
- [ ] La variable está configurada para **Production** environment
- [ ] No hay espacios en blanco al inicio/final de la key
- [ ] Se hizo un **nuevo deploy** después de agregar/modificar la variable
- [ ] La API key es válida y está activa en Veraset
- [ ] El formato de `date_range` es correcto (`from_date`/`to_date`)
- [ ] Los logs en Vercel muestran el error específico

## Mejoras Implementadas

El código ahora incluye:
1. ✅ `.trim()` automático en todas las API keys
2. ✅ Logging detallado de errores en producción
3. ✅ Normalización automática del formato `date_range`
4. ✅ Mensajes de error más descriptivos
5. ✅ Verificación de configuración de API key con hints útiles

## Próximos Pasos

1. Verifica las variables de entorno en Vercel
2. Revisa los logs del último deployment
3. Si el problema persiste, comparte:
   - El error exacto de los logs
   - El status code de la respuesta
   - Si la variable está configurada en Production
