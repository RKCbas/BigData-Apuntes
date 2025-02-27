# Búsquedas en Elasticsearch

## 1. Búsqueda Exacta (match)
Busca documentos donde el campo "nombre" contenga exactamente "Juan Pérez".

```json
{
  "query": {
    "match": {
      "nombre": "Juan Pérez"
    }
  }
}
```
🔹 **Usos:** Búsquedas simples de texto o números.

## 2. Búsqueda con match_phrase (Coincidencia Exacta de Frase)
Busca la frase "ingeniero de software" en el campo "ocupacion", asegurando que las palabras aparezcan juntas y en el mismo orden.

```json
{
  "query": {
    "match_phrase": {
      "ocupacion": "ingeniero de software"
    }
  }
}
```
🔹 **Usos:** Frases exactas, nombres completos.

## 3. Búsqueda de Varios Términos (multi_match)
Busca "Juan Pérez" en varios campos (nombre y apellido).

```json
{
  "query": {
    "multi_match": {
      "query": "Juan Pérez",
      "fields": ["nombre", "apellido"]
    }
  }
}
```
🔹 **Usos:** Cuando no sabemos en qué campo puede estar el valor.

## 4. Búsqueda con bool (Combinación de Condiciones)
Busca personas que:
- ✅ Sean de México
- ✅ Sean ingenieros o doctores
- ✅ No sean menores de 30 años

```json
{
  "query": {
    "bool": {
      "must": [
        { "match": { "pais": "México" } }
      ],
      "should": [
        { "match": { "ocupacion": "ingeniero" } },
        { "match": { "ocupacion": "doctor" } }
      ],
      "must_not": [
        { "range": { "edad": { "lt": 30 } } }
      ]
    }
  }
}
```
🔹 **Explicación:**
- **must** → Obligatorio.
- **should** → Opcional, pero mejora el puntaje.
- **must_not** → Excluye coincidencias.

## 5. Rango de Valores (range)
Busca productos con precio entre 100 y 500.

```json
{
  "query": {
    "range": {
      "precio": {
        "gte": 100,
        "lte": 500
      }
    }
  }
}
```
🔹 **Comparadores:**
- **gte (>=)** → Mayor o igual.
- **lte (<=)** → Menor o igual.
- **gt (>)** → Mayor estrictamente.
- **lt (<)** → Menor estrictamente.

## 6. Búsqueda con exists (Verificar Si un Campo Existe)
Encuentra documentos que tengan el campo "email".

```json
{
  "query": {
    "exists": {
      "field": "email"
    }
  }
}
```
🔹 **Útil para:** Filtrar registros incompletos.

## 7. Búsqueda con wildcard (Patrón con *)
Encuentra correos electrónicos que terminen en "@gmail.com".

```json
{
  "query": {
    "wildcard": {
      "email": {
        "value": "*@gmail.com"
      }
    }
  }
}
```
🔹 **Wildcard:** * → Cualquier secuencia de caracteres.

## 8. Búsqueda con fuzzy (Corrección de Errores)
Busca "juan" en el campo "nombre", pero también encuentra variaciones como "juann", "juhn".

```json
{
  "query": {
    "fuzzy": {
      "nombre": {
        "value": "juan",
        "fuzziness": "AUTO"
      }
    }
  }
}
```
🔹 **Útil para:** Corrección de errores tipográficos.

## 9. Filtrar con terms (Lista de Valores)
Busca empleados con los IDs 1, 2 o 3.

```json
{
  "query": {
    "terms": {
      "id": [1, 2, 3]
    }
  }
}
```
🔹 **Más rápido que match para listas grandes.**

## 10. Filtrado con filter (Sin Recalcular Relevancia)
Encuentra clientes de México con más de 30 años, optimizando rendimiento.

```json
{
  "query": {
    "bool": {
      "filter": [
        { "term": { "pais": "México" } },
        { "range": { "edad": { "gt": 30 } } }
      ]
    }
  }
}
```
🔹 **Diferencia con must:** filter no afecta la relevancia y es más rápido.

## 11. Agregaciones (aggregations)
Cuenta cuántos empleados hay por país.

```json
{
  "size": 0,
  "aggs": {
    "empleados_por_pais": {
      "terms": { "field": "pais.keyword" }
    }
  }
}
```
🔹 **Útil para:** Reportes y análisis de datos.

## 12. Combinando query y aggs
Busca productos con "laptop" en el nombre y muestra el precio promedio.

```json
{
  "query": {
    "match": { "nombre": "laptop" }
  },
  "aggs": {
    "precio_promedio": {
      "avg": { "field": "precio" }
    }
  }
}
```
🔹 **Útil para:** Filtrar y analizar al mismo tiempo.

## 13. Script para Actualización en Lote (bulk update)
Cambia el estado de los pedidos 1, 2 y 3 a "entregado".

```json
{
  "script": {
    "source": "ctx._source.estado = 'entregado'",
    "lang": "painless"
  },
  "query": {
    "terms": {
      "id": [1, 2, 3]
    }
  }
}
```
🔹 **Usado para:** Modificar varios documentos de una vez.


## Resumen de Operadores

| Operador       | Descripción                          |
|----------------|--------------------------------------|
| **match**      | Busca coincidencias en texto         |
| **match_phrase** | Busca frases exactas                |
| **multi_match** | Busca en múltiples campos            |
| **bool**       | Combina filtros (must, should, must_not) |
| **range**      | Filtra por rangos (gt, lt, gte, lte) |
| **exists**     | Verifica si un campo existe          |
| **wildcard**   | Búsqueda con patrones *              |
| **fuzzy**      | Corrige errores tipográficos         |
| **terms**      | Busca en una lista de valores        |
| **filter**     | Filtra sin afectar relevancia        |
| **aggregations** | Agrupa y analiza datos              |
| **script**     | Actualización en lote con scripts    |
