# **Convenciones de Nombres para el Data Warehouse**

Este documento define las convenciones de nombres utilizadas para esquemas, tablas, vistas, columnas y otros objetos en nuestro data warehouse basado en la arquitectura Medallion.

## **Tabla de Contenidos**
1. [Principios Generales](#principios-generales)
2. [Convenciones para Nombres de Tablas](#convenciones-para-nombres-de-tablas)
   - [Reglas para Bronze Layer](#reglas-para-bronze-layer)
   - [Reglas para Silver Layer](#reglas-para-silver-layer)
   - [Reglas para Gold Layer](#reglas-para-gold-layer)
3. [Convenciones para Nombres de Columnas](#convenciones-para-nombres-de-columnas)
   - [Claves Subrogadas](#claves-subrogadas)
   - [Columnas Técnicas](#columnas-técnicas)
4. [Procedimientos Almacenados](#procedimientos-almacenados)

---

## **Principios Generales**

- **Formato de Nombres**: Usar snake_case, con letras minúsculas y guiones bajos (`_`) para separar palabras.
- **Idioma**: Usar inglés para todos los nombres.
- **Evitar Palabras Reservadas**: No utilizar palabras reservadas de SQL como nombres de objetos.

## **Convenciones para Nombres de Tablas**

### **Reglas para Bronze Layer**

- Todos los nombres deben comenzar con el nombre del sistema fuente, y los nombres de las tablas deben coincidir con sus nombres originales sin renombrarlos.
- **`<sistemafuente>_<entidad>`**  
  - `<sistemafuente>`: Nombre del sistema fuente (por ejemplo, `crm`, `erp`).  
  - `<entidad>`: Nombre exacto de la tabla del sistema fuente.  
  - Ejemplo: `crm_customer_info` → Información de clientes del sistema CRM.

### **Reglas para Silver Layer**

- Todos los nombres deben comenzar con el nombre del sistema fuente, y los nombres de las tablas deben coincidir con sus nombres originales sin renombrarlos.
- **`<sistemafuente>_<entidad>`**  
  - `<sistemafuente>`: Nombre del sistema fuente (por ejemplo, `crm`, `erp`).  
  - `<entidad>`: Nombre exacto de la tabla del sistema fuente.  
  - Ejemplo: `crm_customer_info` → Información de clientes del sistema CRM.

### **Reglas para Gold Layer**

- Todos los nombres deben usar términos significativos alineados con el negocio, comenzando con el prefijo de categoría.
- **`<categoría>_<entidad>`**  
  - `<categoría>`: Describe el rol de la tabla, como `dim` (dimensión) o `fact` (tabla de hechos).  
  - `<entidad>`: Nombre descriptivo de la tabla, alineado con el dominio de negocio (por ejemplo, `customers`, `products`, `sales`).  
  - Ejemplos:
    - `dim_customers` → Tabla de dimensión para datos de clientes.  
    - `fact_sales` → Tabla de hechos que contiene transacciones de ventas.  

#### **Glosario de Patrones de Categoría**

| Patrón      | Significado                      | Ejemplo(s)                              |
|-------------|----------------------------------|-----------------------------------------|
| `dim_`      | Tabla de dimensión              | `dim_customer`, `dim_product`           |
| `fact_`     | Tabla de hechos                 | `fact_sales`                            |
| `report_`   | Tabla de reporte                | `report_customers`, `report_sales_monthly` |

## **Convenciones para Nombres de Columnas**

### **Claves Subrogadas**  

- Todas las claves primarias en las tablas de dimensión deben usar el sufijo `_key`.
- **`<nombre_tabla>_key`**  
  - `<nombre_tabla>`: Se refiere al nombre de la tabla o entidad a la que pertenece la clave.  
  - `_key`: Un sufijo que indica que esta columna es una clave subrogada.  
  - Ejemplo: `customer_key` → Clave subrogada en la tabla `dim_customers`.
  
### **Columnas Técnicas**

- Todas las columnas técnicas deben comenzar con el prefijo `dwh_`, seguido de un nombre descriptivo que indique el propósito de la columna.
- **`dwh_<nombre_columna>`**  
  - `dwh`: Prefijo exclusivamente para metadatos generados por el sistema.  
  - `<nombre_columna>`: Nombre descriptivo que indica el propósito de la columna.  
  - Ejemplo: `dwh_load_date` → Columna generada por el sistema utilizada para almacenar la fecha en que se cargó el registro.
 
## **Procedimientos Almacenados**

- Todos los procedimientos almacenados utilizados para cargar datos deben seguir el patrón de nombres:
- **`load_<capa>`**.
  
  - `<capa>`: Representa la capa en la que se están cargando los datos, como `bronze`, `silver` o `gold`.
  - Ejemplo: 
    - `load_bronze` → Procedimiento almacenado para cargar datos en la capa Bronze.
    - `load_silver` → Procedimiento almacenado para cargar datos en la capa Silver.
