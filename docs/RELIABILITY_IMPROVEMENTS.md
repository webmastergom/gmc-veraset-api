# Reliability Improvement Plan

## Problemas Identificados

1. **Radios modificados sin autorización** (30m → 22m)
2. **Días faltantes** (solicitados 30 días, recibidos menos)
3. **Conteos de archivos inconsistentes** entre ejecuciones
4. **Discrepancias entre audit y UI**
5. **Falta de trazabilidad completa** del flujo de datos

## Plan de Acción Inmediato

### 1. Sistema de Verificación End-to-End (E2E)

**Objetivo**: Verificar que cada paso del proceso mantiene la integridad de los datos.

**Implementación**:
- ✅ Candado de verificación antes de enviar a Veraset (YA IMPLEMENTADO)
- ✅ Verificación de radio antes de guardar (YA IMPLEMENTADO)
- ⚠️ **PENDIENTE**: Verificación post-sync para validar que los datos recibidos coinciden con lo solicitado
- ⚠️ **PENDIENTE**: Health checks automáticos en cada etapa

### 2. Tests Automatizados

**Objetivo**: Detectar regresiones antes de que lleguen a producción.

**Prioridades**:
1. **Tests unitarios** para validaciones críticas:
   - Cálculo de días inclusivos
   - Construcción de payloads
   - Verificación de radios
   - Verificación de rangos de fechas

2. **Tests de integración** para flujos completos:
   - Creación de job → Sync → Análisis
   - Verificación de que los datos no se modifican en el proceso

3. **Tests E2E** para casos críticos:
   - Job con radio específico debe mantener ese radio
   - Job con 30 días debe recibir exactamente 30 días

### 3. Logging y Trazabilidad Mejorada

**Objetivo**: Poder rastrear exactamente qué pasó en cada operación.

**Mejoras necesarias**:
- ✅ Logging detallado en candado de verificación (YA IMPLEMENTADO)
- ⚠️ **PENDIENTE**: Correlación de IDs entre requests
- ⚠️ **PENDIENTE**: Logging estructurado (JSON) para mejor análisis
- ⚠️ **PENDIENTE**: Dashboard de monitoreo de discrepancias

### 4. Validaciones Adicionales

**Puntos críticos a validar**:

1. **Frontend → Backend**:
   - Validar que el radio enviado coincide con el input del usuario
   - Validar que las fechas no se modifican
   - Validar que los POIs no se alteran

2. **Backend → Veraset**:
   - ✅ Verificación completa del payload (YA IMPLEMENTADO)
   - ⚠️ **PENDIENTE**: Verificación de respuesta de Veraset

3. **Sync → Storage**:
   - Verificar que todos los archivos esperados se copiaron
   - Verificar que las particiones coinciden con las fechas solicitadas
   - Verificar que no hay archivos corruptos o incompletos

4. **Análisis**:
   - Verificar que todos los días solicitados tienen datos
   - Verificar que los conteos son consistentes

### 5. Sistema de Alertas

**Objetivo**: Detectar problemas automáticamente.

**Alertas críticas**:
- Discrepancia en radio detectada
- Días faltantes detectados
- Archivos faltantes en sync
- Conteos inconsistentes

### 6. Documentación de Problemas Conocidos

**Objetivo**: Mantener registro de problemas y soluciones.

**Formato**:
- Problema identificado
- Causa raíz
- Solución implementada
- Tests para prevenir regresión

## Implementación Prioritaria

### Fase 1: Estabilización (Semana 1)
1. ✅ Candado de verificación completo (YA HECHO)
2. ⚠️ Tests unitarios para validaciones críticas
3. ⚠️ Health check endpoint
4. ⚠️ Mejora de logging estructurado

### Fase 2: Verificación (Semana 2)
1. ⚠️ Verificación post-sync
2. ⚠️ Tests de integración
3. ⚠️ Dashboard de discrepancias
4. ⚠️ Sistema de alertas

### Fase 3: Monitoreo (Semana 3)
1. ⚠️ Tests E2E automatizados
2. ⚠️ Métricas de confiabilidad
3. ⚠️ Documentación completa

## Métricas de Confiabilidad

**KPIs a monitorear**:
- Tasa de discrepancias detectadas por el candado
- Tasa de jobs con días faltantes
- Tasa de discrepancias en radios
- Tiempo promedio de detección de problemas

## Próximos Pasos

1. Revisar este documento con el equipo
2. Priorizar implementaciones según impacto
3. Crear tickets para cada mejora
4. Establecer SLA de confiabilidad objetivo
