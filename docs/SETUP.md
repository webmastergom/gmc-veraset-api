# Guía de Configuración - Veraset API Platform

## Pasos Rápidos para Empezar

### 1. Instalar Dependencias

```bash
npm install
```

### 2. Configurar Variables de Entorno

Crea un archivo `.env.local` en la raíz del proyecto:

```bash
cp .env.example .env.local
```

Edita `.env.local` y completa estos valores:

```env
# Veraset API Key (obligatorio)
VERASET_API_KEY=tu_api_key_aqui

# AWS Credentials (obligatorio)
AWS_ACCESS_KEY_ID=AKIAUFQM7C7TQ6MRSC5Q
AWS_SECRET_ACCESS_KEY=tu_secret_access_key_aqui
AWS_REGION=us-west-2

# S3 Bucket (obligatorio)
S3_BUCKET=garritz-veraset-data-us-west-2

# API Base URL (para llamadas server-side)
NEXT_PUBLIC_API_URL=https://gmc-mobility-api.vercel.app

# Opcional: Mapbox para visualizaciones de mapas
NEXT_PUBLIC_MAPBOX_TOKEN=tu_token_aqui
```

### 3. Verificar Acceso a S3

Asegúrate de que tus credenciales AWS tienen acceso al bucket `garritz-veraset-data-us-west-2`.

Puedes probar el acceso con:

```bash
aws s3 ls s3://garritz-veraset-data-us-west-2/ --region us-west-2
```

### 4. Inicializar Archivos de Configuración en S3

La aplicación creará automáticamente los archivos necesarios, pero puedes inicializarlos manualmente:

**Crear estructura inicial:**

```bash
# Crear archivo de uso inicial
echo '{"2026-02":{"used":0,"limit":200,"lastJobId":null,"lastJobAt":null}}' > usage.json
aws s3 cp usage.json s3://garritz-veraset-data-us-west-2/config/usage.json

# Crear archivo de jobs vacío
echo '{}' > jobs.json
aws s3 cp jobs.json s3://garritz-veraset-data-us-west-2/config/jobs.json

# Crear archivo de colecciones POI vacío
echo '{}' > poi-collections.json
aws s3 cp poi-collections.json s3://garritz-veraset-data-us-west-2/config/poi-collections.json

# Limpiar archivos temporales
rm usage.json jobs.json poi-collections.json
```

### 5. Ejecutar el Servidor de Desarrollo

```bash
npm run dev
```

Abre tu navegador en: **http://localhost:3000**

### 6. Verificar que Todo Funciona

1. **Dashboard**: Deberías ver el dashboard con el contador de uso de API
2. **Navegación**: El badge de uso debería aparecer en la barra superior
3. **Jobs**: Puedes ver la lista de jobs (vacía inicialmente)
4. **POIs**: Puedes ver la lista de colecciones POI (vacía inicialmente)

## Estructura de Archivos S3

La aplicación espera esta estructura en S3:

```
s3://garritz-veraset-data-us-west-2/
├── config/
│   ├── usage.json              # Uso mensual de API
│   ├── jobs.json               # Metadatos de todos los jobs
│   └── poi-collections.json    # Índice de colecciones POI
├── pois/
│   └── {collection-id}.geojson # GeoJSON de cada colección
└── {dataset-name}/             # Datos de Veraset sincronizados
    └── date=YYYY-MM-DD/
        └── *.parquet
```

## Solución de Problemas

### Error: "Failed to fetch usage"
- Verifica que las credenciales AWS estén correctas
- Verifica que el bucket existe y tienes permisos
- Verifica que el archivo `config/usage.json` existe en S3

### Error: "Database not configured"
- Esto es normal si no hay datos aún
- La aplicación creará los archivos automáticamente en el primer uso

### Error: "VERASET_API_KEY not configured"
- Asegúrate de tener `VERASET_API_KEY` en `.env.local`
- Reinicia el servidor después de agregar variables de entorno

### El badge de uso no aparece
- Verifica la consola del navegador para errores
- Asegúrate de que `/api/usage` responde correctamente
- Verifica que el archivo `config/usage.json` existe en S3

## Comandos Útiles

```bash
# Desarrollo
npm run dev

# Build para producción
npm run build

# Ejecutar producción localmente
npm start

# Linting
npm run lint

# Deploy a Vercel
vercel --prod
```

## Próximos Pasos

1. **Crear tu primera colección POI**:
   - Ve a `/pois`
   - Haz clic en "Upload" o "Import"
   - Sube un archivo GeoJSON o importa desde OSM/Overture

2. **Crear tu primer job**:
   - Ve a `/jobs/new`
   - Selecciona una colección POI
   - Configura fechas y parámetros
   - Crea el job

3. **Monitorear el job**:
   - Ve a `/jobs` para ver el estado
   - El estado se actualiza automáticamente cada 30 segundos

4. **Sincronizar datos**:
   - Cuando un job termine (SUCCESS), ve a `/sync`
   - Copia los datos del bucket de Veraset al bucket GMC

## Notas Importantes

- **Límite de API**: 200 jobs por mes (se resetea el día 1)
- **Límite por job**: Máximo 25,000 POIs
- **Rango de fechas**: Máximo 31 días por job
- **Los archivos de configuración se crean automáticamente** en el primer uso
