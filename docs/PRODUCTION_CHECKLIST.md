# Checklist de Producción - GMC Mobility API

## Estado Actual del Proyecto

### ✅ Completado

1. **Seguridad Implementada**
   - ✅ Headers de seguridad HTTP (HSTS, CSP, X-Frame-Options, etc.)
   - ✅ CORS restringido a orígenes específicos
   - ✅ Rate limiting implementado
   - ✅ Autenticación mejorada con validación
   - ✅ Logging seguro (redacta información sensible)
   - ✅ Validación de entrada con Zod

2. **Código Actualizado**
   - ✅ Todas las referencias actualizadas a `gmc-mobility-api.vercel.app`
   - ✅ Dominio viejo eliminado de Vercel
   - ✅ Sin referencias al dominio antiguo en el código

3. **Configuración**
   - ✅ `vercel.json` configurado correctamente
   - ✅ `next.config.js` con headers de seguridad
   - ✅ Middleware de seguridad implementado

---

## ⚠️ Pendiente: Variables de Entorno en Vercel

### Variables Críticas (OBLIGATORIAS)

Configura estas variables en **Vercel Dashboard → Settings → Environment Variables**:

#### 1. Seguridad y Autenticación
```bash
# Generar con: openssl rand -base64 32
AUTH_SECRET=tu_secreto_generado_aqui

# Tu usuario y contraseña para login
AUTH_USERNAME=tu_usuario
AUTH_PASSWORD=tu_contraseña_segura

# Orígenes permitidos para CORS
ALLOWED_ORIGINS=https://cloud.garritz.com
```

#### 2. Veraset API
```bash
VERASET_API_KEY=tu_api_key_de_veraset
```

#### 3. AWS Credentials
```bash
AWS_ACCESS_KEY_ID=tu_aws_access_key_id
AWS_SECRET_ACCESS_KEY=tu_aws_secret_access_key
AWS_REGION=us-west-2
S3_BUCKET=garritz-veraset-data-us-west-2
```

#### 4. URLs (Recomendado)
```bash
NEXT_PUBLIC_API_URL=https://gmc-mobility-api.vercel.app
```

#### 5. Opcionales
```bash
# Solo si usas mapas
NEXT_PUBLIC_MAPBOX_TOKEN=tu_token_mapbox

# Solo si usas hashing de contraseñas
AUTH_PASSWORD_HASH=hash_generado
AUTH_PASSWORD_SALT=salt_generado
```

---

## 📋 Checklist Pre-Despliegue

### Configuración en Vercel

- [ ] **ALLOWED_ORIGINS** configurado: `https://cloud.garritz.com`
- [ ] **AUTH_SECRET** generado y configurado (32+ caracteres)
- [ ] **AUTH_USERNAME** configurado
- [ ] **AUTH_PASSWORD** configurado (contraseña fuerte)
- [ ] **VERASET_API_KEY** configurado
- [ ] **AWS_ACCESS_KEY_ID** configurado
- [ ] **AWS_SECRET_ACCESS_KEY** configurado
- [ ] **AWS_REGION** configurado: `us-west-2`
- [ ] **S3_BUCKET** configurado: `garritz-veraset-data-us-west-2`
- [ ] **NEXT_PUBLIC_API_URL** configurado: `https://gmc-mobility-api.vercel.app` (opcional pero recomendado)

### Verificación de Código

- [x] Todas las referencias al dominio viejo eliminadas
- [x] Headers de seguridad configurados
- [x] CORS restringido
- [x] Rate limiting implementado
- [x] Autenticación protegida
- [x] Validación de entrada implementada
- [x] Logging seguro implementado

### Verificación Post-Despliegue

Después de desplegar, verifica:

- [ ] **Headers de seguridad**: 
  ```bash
  curl -I https://gmc-mobility-api.vercel.app
  ```
  Debe mostrar: `Strict-Transport-Security`, `X-Frame-Options`, etc.

- [ ] **CORS funciona correctamente**:
  ```bash
  # Desde cloud.garritz.com debe funcionar
  # Desde otros dominios debe fallar
  ```

- [ ] **Login funciona**: 
  - Ve a `https://gmc-mobility-api.vercel.app/login`
  - Debe poder hacer login con tus credenciales

- [ ] **Rutas protegidas funcionan**:
  - Sin login: debe redirigir a `/login`
  - Con login: debe permitir acceso

- [ ] **Rate limiting funciona**:
  - Intenta hacer múltiples requests rápidos
  - Debe retornar 429 después del límite

---

## 🚀 Pasos para Desplegar

### 1. Configurar Variables de Entorno en Vercel

1. Ve a [Vercel Dashboard](https://vercel.com)
2. Selecciona tu proyecto: `gmc-mobility-api`
3. Ve a **Settings** → **Environment Variables**
4. Agrega cada variable una por una:
   - Selecciona el entorno (Production, Preview, o ambos)
   - Guarda cada variable

### 2. Generar AUTH_SECRET

En tu terminal:
```bash
openssl rand -base64 32
```

Copia el resultado y úsalo como valor de `AUTH_SECRET` en Vercel.

### 3. Desplegar

```bash
# Opción 1: Desde terminal
npm run deploy

# Opción 2: Push a main branch (si tienes integración con Git)
git push origin main
```

### 4. Verificar Despliegue

1. Espera a que termine el build en Vercel
2. Visita `https://gmc-mobility-api.vercel.app`
3. Verifica que todo funciona correctamente

---

## 🔍 Verificación de Seguridad

### Test 1: Headers de Seguridad
```bash
curl -I https://gmc-mobility-api.vercel.app
```

Debe incluir:
- `Strict-Transport-Security`
- `X-Frame-Options: DENY`
- `X-Content-Type-Options: nosniff`
- `Content-Security-Policy`

### Test 2: CORS
```bash
# Desde un dominio no permitido (debe fallar)
curl -H "Origin: https://sitio-malicioso.com" \
     -H "Access-Control-Request-Method: GET" \
     https://gmc-mobility-api.vercel.app/api/jobs \
     -v
```

### Test 3: Autenticación
```bash
# Sin autenticación (debe retornar 401)
curl https://gmc-mobility-api.vercel.app/api/jobs

# Con autenticación (después de login)
curl -H "Cookie: auth-token=..." \
     https://gmc-mobility-api.vercel.app/api/jobs
```

### Test 4: Rate Limiting
```bash
# Hacer múltiples requests rápidos
for i in {1..25}; do
  curl https://gmc-mobility-api.vercel.app/api/auth/login \
       -X POST \
       -H "Content-Type: application/json" \
       -d '{"username":"test","password":"test"}' &
done
wait

# Debe retornar 429 después del límite
```

---

## 📊 Resumen de Estado

| Categoría | Estado | Notas |
|-----------|--------|-------|
| **Código** | ✅ Listo | Todas las referencias actualizadas |
| **Seguridad** | ✅ Implementada | Headers, CORS, Rate limiting, Auth |
| **Variables de Entorno** | ⚠️ Pendiente | Debe configurarse en Vercel |
| **Despliegue** | ⏳ Listo para desplegar | Después de configurar variables |

---

## ⚡ Acción Inmediata Requerida

**ANTES de desplegar a producción, configura estas variables en Vercel:**

1. `ALLOWED_ORIGINS=https://cloud.garritz.com`
2. `AUTH_SECRET` (generar con `openssl rand -base64 32`)
3. `AUTH_USERNAME` (tu usuario)
4. `AUTH_PASSWORD` (tu contraseña)
5. `VERASET_API_KEY` (si no está ya configurado)
6. Credenciales AWS (si no están ya configuradas)

Una vez configuradas, puedes desplegar con seguridad.

---

## 📞 Troubleshooting

### Error: "Unauthorized" en todas las rutas
- Verifica que `AUTH_SECRET` está configurado en Vercel
- Asegúrate de haber redesplegado después de agregar la variable

### Error: "CORS policy blocked"
- Verifica que `ALLOWED_ORIGINS` incluye exactamente `https://cloud.garritz.com`
- Asegúrate de incluir el protocolo `https://`

### Error: "VERASET_API_KEY not configured"
- Verifica que la variable está configurada en Vercel
- Asegúrate de que está en el entorno correcto (Production)

### No puedo hacer login
- Verifica que `AUTH_USERNAME` y `AUTH_PASSWORD` están configurados
- Verifica que estás usando las credenciales correctas
- Revisa los logs en Vercel para ver errores

---

## ✅ Listo para Producción

Una vez que completes el checklist de variables de entorno, la aplicación estará lista para producción.

**Próximo paso**: Configura las variables de entorno en Vercel y despliega.
