# Mejoras de Seguridad para Producción

Este documento describe las mejoras de seguridad implementadas en la aplicación para prepararla para producción.

## ✅ Mejoras Implementadas

### 1. Headers de Seguridad HTTP

Se agregaron headers de seguridad en `next.config.js` y `vercel.json`:

- **Strict-Transport-Security (HSTS)**: Fuerza conexiones HTTPS
- **X-Frame-Options**: Previene clickjacking
- **X-Content-Type-Options**: Previene MIME type sniffing
- **X-XSS-Protection**: Protección básica contra XSS
- **Content-Security-Policy (CSP)**: Controla qué recursos puede cargar la aplicación
- **Referrer-Policy**: Controla qué información de referrer se envía
- **Permissions-Policy**: Restringe el uso de APIs del navegador

### 2. Configuración CORS Mejorada

- **Antes**: `Access-Control-Allow-Origin: *` (permitía cualquier origen)
- **Ahora**: Solo permite orígenes específicos configurados en `ALLOWED_ORIGINS`
- Se creó `lib/security.ts` con funciones helper para validar orígenes

**Configuración requerida:**
```bash
ALLOWED_ORIGINS=https://your-domain.com,https://app.your-domain.com
```

### 3. Protección de Rutas API

- **Middleware mejorado**: Ahora protege todas las rutas API excepto las públicas
- **Rutas públicas**: `/api/auth/login`, `/api/auth/logout`, `/api/health`
- **Rutas protegidas**: Todas las demás rutas API requieren autenticación
- Las rutas Veraset (`/api/veraset/*`) pueden tener su propia autenticación

### 4. Rate Limiting

Se implementó rate limiting para prevenir ataques de fuerza bruta:

- **Login endpoint**: 5 intentos por 15 minutos por IP
- **Otras rutas API**: 20 requests por minuto por IP
- **Headers de respuesta**: Incluyen información de límites (`X-RateLimit-*`)
- Implementación en memoria (para producción, considerar Redis)

### 5. Autenticación Mejorada

- **Validación de entrada**: Uso de Zod para validar datos de entrada
- **Hashing de contraseñas**: Soporte para contraseñas hasheadas (SHA-256)
- **Cookies seguras**: 
  - `httpOnly: true` (previene acceso desde JavaScript)
  - `secure: true` en producción (solo HTTPS)
  - `sameSite: 'strict'` (previene CSRF)
- **Mensajes de error genéricos**: No revelan si el usuario o contraseña son incorrectos

### 6. Validación de Entrada Robusta

Se creó `lib/validation.ts` con esquemas Zod para:

- Creación de jobs (`createJobSchema`)
- Login (`loginSchema`)
- Filtros de análisis (`analysisFiltersSchema`)
- Sanitización de strings para prevenir XSS básico

### 7. Logging Seguro

Se creó `lib/logger.ts` que:

- **En desarrollo**: Muestra toda la información
- **En producción**: Redacta automáticamente información sensible:
  - Passwords, secrets, tokens, keys, credentials
  - Solo muestra mensajes de error genéricos
  - No expone stack traces completos

### 8. Limpieza de Credenciales

- Se removieron credenciales reales de `.env.example`
- Se agregaron instrucciones para generar valores seguros
- Se agregaron comentarios sobre seguridad

## 🔧 Configuración Requerida para Producción

### Variables de Entorno

Asegúrate de configurar estas variables en tu plataforma de despliegue:

```bash
# Generar AUTH_SECRET seguro
openssl rand -base64 32

# Configurar orígenes permitidos para CORS
ALLOWED_ORIGINS=https://your-production-domain.com

# Usar contraseñas fuertes
AUTH_USERNAME=your_secure_username
AUTH_PASSWORD=your_strong_password

# Opcional: Usar hashing de contraseñas (más seguro)
# Generar hash y salt:
node -e "const crypto=require('crypto'); const salt=crypto.randomBytes(16).toString('hex'); const hash=crypto.createHash('sha256').update('your_password'+salt).digest('hex'); console.log('AUTH_PASSWORD_HASH='+hash); console.log('AUTH_PASSWORD_SALT='+salt);"
```

### Verificación

1. **Headers de seguridad**: Verifica que los headers estén presentes:
   ```bash
   curl -I https://your-domain.com
   ```

2. **CORS**: Verifica que solo orígenes permitidos puedan hacer requests:
   ```bash
   curl -H "Origin: https://unauthorized-domain.com" https://your-domain.com/api/jobs
   ```

3. **Rate limiting**: Intenta hacer múltiples requests rápidos y verifica el código 429

4. **Autenticación**: Verifica que las rutas protegidas requieren autenticación

## 📋 Checklist de Seguridad Pre-Producción

- [ ] Todas las variables de entorno están configuradas
- [ ] `AUTH_SECRET` es un valor aleatorio fuerte (32+ caracteres)
- [ ] `AUTH_PASSWORD` es una contraseña fuerte y única
- [ ] `ALLOWED_ORIGINS` contiene solo los dominios permitidos
- [ ] No hay credenciales hardcodeadas en el código
- [ ] Los logs no exponen información sensible
- [ ] HTTPS está habilitado (requerido para cookies seguras)
- [ ] Se han probado los límites de rate limiting
- [ ] Se ha verificado que CORS funciona correctamente
- [ ] Se han revisado los headers de seguridad

## 🔒 Mejoras Futuras Recomendadas

1. **JWT Tokens**: Migrar de cookies simples a tokens JWT con refresh tokens
2. **Redis para Rate Limiting**: Usar Redis en lugar de memoria para rate limiting distribuido
3. **bcrypt/Argon2**: Usar algoritmos de hashing más seguros para contraseñas
4. **2FA**: Implementar autenticación de dos factores
5. **Auditoría**: Implementar logging de eventos de seguridad
6. **WAF**: Considerar un Web Application Firewall (Cloudflare, AWS WAF)
7. **Dependencias**: Revisar y actualizar dependencias regularmente (`npm audit`)
8. **Secrets Management**: Usar un servicio de gestión de secretos (AWS Secrets Manager, Vercel Secrets)

## 📚 Recursos

- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [Next.js Security Best Practices](https://nextjs.org/docs/app/building-your-application/configuring/security-headers)
- [CORS Security](https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS)
- [Rate Limiting Best Practices](https://cloud.google.com/architecture/rate-limiting-strategies-techniques)
