# holded_dailyledger_load.py - Carga inicial del libro diario (últimos 12 meses)
import os
import requests
import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import uuid
import time

def get_db_connection():
    """Establece conexión con la base de datos Supabase."""
    conn = psycopg2.connect(
        host=os.environ.get("SUPABASE_DB_HOST"),
        database=os.environ.get("SUPABASE_DB_NAME", "postgres"),
        user=os.environ.get("SUPABASE_DB_USER"),
        password=os.environ.get("SUPABASE_DB_PASSWORD"),
        port=os.environ.get("SUPABASE_DB_PORT", "5432")
    )
    return conn

def fetch_dailyledger_page(from_timestamp, to_timestamp, page=1):
    """Obtiene una página del libro diario desde la API de Holded."""
    api_key = os.environ.get("HOLDED_API_KEY")
    
    # Parámetros de la API
    url = "https://api.holded.com/api/accounting/v1/dailyledger"
    params = {
        "starttmp": from_timestamp,
        "endtmp": to_timestamp,
        "page": page
    }
    headers = {"key": api_key}
    
    # Agregamos una pequeña pausa entre páginas para no sobrecargar la API
    if page > 1:
        time.sleep(1)
    
    # Convertir timestamps a fechas para mejor legibilidad en los logs
    from_date = datetime.fromtimestamp(int(from_timestamp))
    to_date = datetime.fromtimestamp(int(to_timestamp))
    
    print(f"Consultando página {page} de la API. Período: {from_date.strftime('%Y-%m-%d')} a {to_date.strftime('%Y-%m-%d')}")
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        entries = response.json()
        print(f"Obtenidas {len(entries)} entradas en la página {page}")
        return entries
    else:
        print(f"Error al obtener datos: {response.status_code}")
        print(f"Respuesta: {response.text}")
        return None

def fetch_all_dailyledger():
    """Obtiene todas las páginas del libro diario del último año, manejando la paginación."""
    all_entries = []
    page = 1
    max_pages = 100  # Límite de seguridad para evitar bucles infinitos
    
    # Calculamos el rango de fechas: últimos 12 meses menos 1 día por seguridad
    today = datetime.now()
    start_date = today - timedelta(days=365+1)  # Un año y un día atrás
    
    # Convertimos a timestamps
    from_timestamp = int(start_date.timestamp())
    to_timestamp = int(today.timestamp())
    
    print(f"Obteniendo datos desde {start_date.strftime('%Y-%m-%d')} hasta {today.strftime('%Y-%m-%d')}")
    
    while page <= max_pages:
        entries = fetch_dailyledger_page(from_timestamp, to_timestamp, page)
        
        if not entries or len(entries) == 0:
            print(f"No hay más entradas. Total obtenidas: {len(all_entries)}")
            break
        
        # Agregamos el número de página para trazabilidad
        for entry in entries:
            entry["_page"] = page
        
        all_entries.extend(entries)
        print(f"Acumulado: {len(all_entries)} entradas")
        page += 1
    
    if page > max_pages:
        print(f"⚠️ Alcanzado el límite de {max_pages} páginas. Puede haber más datos disponibles.")
    
    return all_entries

def load_dailyledger_to_bronze(entries):
    """Carga los datos del libro diario en la base de datos."""
    if not entries:
        print("No hay datos para cargar")
        return 0
    
    # Identificadores para el proceso
    process_id = str(uuid.uuid4())
    batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
    current_timestamp = datetime.now()
    
    # Preparamos los datos para inserción
    prepared_data = []
    skipped = 0
    
    for entry in entries:
        # Verificamos las claves requeridas
        if not all(key in entry for key in ['entryNumber', 'line', 'timestamp']):
            skipped += 1
            continue
        
        # Preparamos los datos para la inserción
        prepared_entry = (
            entry.get('entryNumber'),
            entry.get('line'),
            entry.get('timestamp'),
            entry.get('type'),
            entry.get('description', ''),
            entry.get('docDescription', ''),
            entry.get('account'),
            entry.get('debit'),
            entry.get('credit'),
            json.dumps(entry.get('tags', [])),
            entry.get('checked'),
            'holded',
            'dailyledger',
            current_timestamp,
            current_timestamp,
            batch_id,
            process_id,
            entry.get('_page')
        )
        prepared_data.append(prepared_entry)
    
    if skipped > 0:
        print(f"Se omitieron {skipped} entradas por falta de campos requeridos")
    
    try:
        # Conectamos a la base de datos
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Truncamos la tabla para la carga inicial
        print("Truncando tabla bronze.holded_dailyledger")
        cursor.execute("TRUNCATE TABLE bronze.holded_dailyledger")
        
        # Insertamos los datos en lotes para mejor rendimiento
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(prepared_data), batch_size):
            batch = prepared_data[i:i+batch_size]
            
            insert_query = """
            INSERT INTO bronze.holded_dailyledger (
                entrynumber, line, timestamp, type, description,
                docdescription, account, debit, credit, tags, checked,
                dwh_source_system, dwh_source_entity, dwh_insert_timestamp,
                dwh_update_timestamp, dwh_batch_id, dwh_process_id, dwh_page_number
            ) VALUES %s
            """
            
            execute_values(cursor, insert_query, batch)
            conn.commit()
            
            total_inserted += len(batch)
            print(f"Progreso: {total_inserted}/{len(prepared_data)} registros insertados")
        
        # Verificamos la carga y mostramos estadísticas
        cursor.execute("""
        SELECT
            to_char(to_timestamp(timestamp), 'YYYY-MM') as month,
            COUNT(*) as entries,
            COUNT(DISTINCT entrynumber) as asientos
        FROM bronze.holded_dailyledger
        GROUP BY month
        ORDER BY month
        """)
        
        stats = cursor.fetchall()
        print("\nEstadísticas por mes:")
        for month, entries, asientos in stats:
            print(f"  {month}: {entries} líneas en {asientos} asientos")
        
        # Total de registros
        cursor.execute("SELECT COUNT(*) FROM bronze.holded_dailyledger")
        count = cursor.fetchone()[0]
        
        print(f"\nCarga completada. {count} registros insertados en total")
        
        cursor.close()
        conn.close()
        return count
    
    except Exception as e:
        print(f"Error durante la carga: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        return 0

def main():
    """Función principal que orquesta el proceso de carga."""
    start_time = datetime.now()
    print("=" * 80)
    print(f"INICIO CARGA INICIAL LIBRO DIARIO: {start_time}")
    print("=" * 80)
    
    try:
        # Obtener todas las páginas del libro diario
        entries = fetch_all_dailyledger()
        
        if entries:
            # Cargar datos en la capa Bronze
            rows_inserted = load_dailyledger_to_bronze(entries)
            
            # Cálculo de tiempo total
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print("=" * 80)
            print(f"FIN CARGA INICIAL: {end_time}")
            print(f"Total registros cargados: {rows_inserted}")
            print(f"Tiempo total: {duration:.2f} segundos")
            print("=" * 80)
        else:
            print("⚠️ No se obtuvieron datos del libro diario. Verifica la API y los parámetros.")
    
    except Exception as e:
        print(f"ERROR EN EL PROCESO: {str(e)}")
        import traceback
        print(traceback.format_exc())

# Punto de entrada del script
if __name__ == "__main__":
    main()
