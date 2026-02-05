# Solución para errores de archivos estáticos (404)

## Problema
Los archivos estáticos de Next.js devuelven 404 y errores de MIME type después de cambios en el código.

## Solución

1. **Detén el servidor de desarrollo** (Ctrl+C o Cmd+C)

2. **Limpia el caché de Next.js** (ya hecho):
   ```bash
   rm -rf .next
   ```

3. **Reinicia el servidor de desarrollo**:
   ```bash
   npm run dev
   ```

## Notas

- Los warnings de webpack sobre archivos que no puede resolver son **normales** durante el desarrollo y no afectan la funcionalidad
- Estos warnings aparecen cuando Next.js intenta resolver dependencias que aún no se han generado
- El servidor de desarrollo necesita reiniciarse después de limpiar el caché para regenerar los archivos estáticos

## Si el problema persiste

1. Limpia también el caché de node_modules:
   ```bash
   rm -rf .next node_modules/.cache
   ```

2. Reinicia el servidor nuevamente
