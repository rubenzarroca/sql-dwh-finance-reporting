import os
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, date
import json
import numpy as np

# Función para conectar a Supabase (PostgreSQL)
def get_db_connection():
    conn = psycopg2.connect(
        host=os.environ.get("SUPABASE_DB_HOST"),
        database=os.environ.get("SUPABASE_DB_NAME", "postgres"),
        user=os.environ.get("SUPABASE_DB_USER", "postgres"),
        password=os.environ.get("SUPABASE_DB_PASSWORD"),
        port=os.environ.get("SUPABASE_DB_PORT", "5432")
    )
    return conn

# Función para calcular la fecha de inicio del trimestre anterior
def get_start_date():
    today = date.today()
    current_month = today.month
    current_year = today.year
    
    # Determinar el trimestre actual
    current_quarter = (current_month - 1) // 3 + 1
    
    # Calcular el primer día del trimestre anterior
    if current_quarter == 1:  # Si estamos en Q1 (ene-mar), necesitamos Q4 del año anterior
        start_quarter_month = 10  # Octubre
        start_quarter_year = current_year - 1
    else:
        # Para otros trimestres, retrocedemos un trimestre en el mismo año
        start_quarter_month = ((current_quarter - 2) * 3) + 1
        start_quarter_year = current_year
    
    # Crear fecha de inicio (primer día del trimestre anterior)
    start_date = date(start_quarter_year, start_quarter_month, 1)
    
    print(f"Fecha de inicio calculada: {start_date} (trimestre anterior)")
    return start_date

# Función para obtener los datos del libro diario de la API de Holded
def fetch_holded_dailyledger(from_date, to_date):
    api_key = os.environ.get("HOLDED_API_KEY")
    
    headers = {
        "Accept": "application/json",
        "key": api_key
    }
    
    # Convertir fechas a timestamps de Unix (segundos)
    from_timestamp = int(datetime.combine(from_date, datetime.min.time()).timestamp())
    to_timestamp = int(datetime.combine(to_date, datetime.max.time()).timestamp())
    
    url = "https://api.holded.com/api/accounting/v1/dailyledger"
    params = {
        "starttmp": from_timestamp,
        "endtmp": to_timestamp
    }
    
    print(f"Consultando API de Holded: {url}")
    print(f"Período: {from_date} a {to_date}")
    print(f"Timestamps: {from_timestamp} a {to_timestamp}")
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        print("Datos recibidos correctamente de la API")
        return response.json()
    else:
        print(f"Error al obtener datos: {response.status_code}")
        print(f"Respuesta: {response.text}")
        return None

# Función para procesar el libro diario y actualizarlo en la base de datos
def update_dailyledger():
    print("=================================================")
    print(f"Inicio de actualización del libro diario: {datetime.now()}")
    print("=================================================")

    # Calcular fechas de inicio y fin
    start_date = get_start_date()
    end_date = date.today()
    
    # Obtener datos de la API
    ledger_data = fetch_holded_dailyledger(start_date, end_date)
    
    if not ledger_data:
        print("No se pudieron obtener datos. Finalizando proceso.")
        return
    
    # Convertir a DataFrame
    print("Procesando datos recibidos...")
    
    # La API podría devolver una lista vacía o una estructura diferente
    if isinstance(ledger_data, list) and len(ledger_data) == 0:
        print("No hay entradas en el libro diario para el período especificado. Finalizando.")
        return
    
    # Preparar los datos para la conversión a DataFrame
    flattened_data = []
    
    # La estructura puede variar, así que manejamos diferentes posibilidades
    if isinstance(ledger_data, list):
        # Caso 1: Lista de entradas del libro diario
        for entry in ledger_data:
            # Extraer campos comunes a nivel de entrada
            entry_number = entry.get('entryNumber')
            timestamp = entry.get('timestamp')
            description = entry.get('description')
            doc_description = entry.get('docDescription')
            
            # Procesar líneas de asiento (debe haber al menos una línea de débito y una de crédito)
            lines = entry.get('lines', [])
            for i, line in enumerate(lines):
                flattened_data.append({
                    'entryNumber': entry_number,
                    'line': i + 1,  # Índice base 1 para líneas
                    'timestamp': timestamp,
                    'type': line.get('type'),
                    'description': description,
                    'docDescription': doc_description,
                    'account': line.get('account'),
                    'debit': line.get('debit', 0),
                    'credit': line.get('credit', 0),
                    'tags': line.get('tags', []),
                    'checked': line.get('checked', 'No')
                })
    else:
        # Caso 2: Directamente podría ser una estructura de entrada única
        print(f"Formato de datos inesperado: {type(ledger_data)}")
        print(f"Muestra de datos: {str(ledger_data)[:500]}...")
        return
    
    # Crear DataFrame
    if not flattened_data:
        print("No hay entradas para procesar después de la conversión. Finalizando.")
        return
    
    df_ledger = pd.DataFrame(flattened_data)
    
    # Añadir columnas técnicas
    batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
    process_id = f"DL_UPDATE_{batch_id}"
    
    df_ledger['dwh_source_system'] = 'holded'
    df_ledger['dwh_source_entity'] = 'dailyledger'
    df_ledger['dwh_insert_timestamp'] = datetime.now()
    df_ledger['dwh_update_timestamp'] = datetime.now()
    df_ledger['dwh_batch_id'] = batch_id
    df_ledger['dwh_process_id'] = process_id
    df_ledger['dwh_page_number'] = 1  # Por defecto
    
    # Convertir NaN a None para evitar errores en PostgreSQL
    df_ledger = df_ledger.replace({np.nan: None})
    
    # Convertir tags a formato JSON
    df_ledger['tags'] = df_ledger['tags'].apply(
        lambda x: json.dumps(x) if x is not None else None
    )
    
    try:
        # Conectar a la base de datos
        print("Conectando a la base de datos...")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Contadores para el resumen
        new_entries = 0
        updated_entries = 0
        
        # Para cada entrada, insertar o actualizar
        print("Procesando entradas del libro diario...")
        for _, row in df_ledger.iterrows():
            # Crear una clave única para cada línea de asiento
            entry_key = (row['entryNumber'], row['line'], row['timestamp'])
            
            # Comprobar si la entrada ya existe
            cursor.execute("""
                SELECT entryNumber, line, timestamp 
                FROM bronze.holded_dailyledger 
                WHERE entryNumber = %s AND line = %s AND timestamp = %s
            """, entry_key)
            
            exists = cursor.fetchone()
            
            if exists:
                # Actualizar entrada existente
                cursor.execute("""
                    UPDATE bronze.holded_dailyledger 
                    SET 
                        type = %s,
                        description = %s,
                        docdescription = %s,
                        account = %s,
                        debit = %s,
                        credit = %s,
                        tags = %s::jsonb,
                        checked = %s,
                        dwh_update_timestamp = %s,
                        dwh_batch_id = %s,
                        dwh_process_id = %s
                    WHERE entryNumber = %s AND line = %s AND timestamp = %s
                """, (
                    row['type'], row['description'], row['docDescription'], 
                    row['account'], row['debit'], row['credit'], 
                    row['tags'], row['checked'],
                    row['dwh_update_timestamp'], row['dwh_batch_id'], row['dwh_process_id'],
                    *entry_key
                ))
                updated_entries += 1
            else:
                # Insertar nueva entrada
                cursor.execute("""
                    INSERT INTO bronze.holded_dailyledger 
                    (entryNumber, line, timestamp, type, description, docdescription, 
                     account, debit, credit, tags, checked,
                     dwh_source_system, dwh_source_entity, dwh_insert_timestamp, 
                     dwh_update_timestamp, dwh_batch_id, dwh_process_id, dwh_page_number)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['entryNumber'], row['line'], row['timestamp'], 
                    row['type'], row['description'], row['docDescription'], 
                    row['account'], row['debit'], row['credit'], 
                    row['tags'], row['checked'],
                    row['dwh_source_system'], row['dwh_source_entity'], 
                    row['dwh_insert_timestamp'], row['dwh_update_timestamp'], 
                    row['dwh_batch_id'], row['dwh_process_id'], row['dwh_page_number']
                ))
                new_entries += 1
        
        # Confirmar cambios
        conn.commit()
        
        print("=================================================")
        print(f"Actualización completada: {datetime.now()}")
        print(f"Entradas nuevas: {new_entries}")
        print(f"Entradas actualizadas: {updated_entries}")
        print(f"Total de entradas procesadas: {len(df_ledger)}")
        print("=================================================")
        
    except Exception as e:
        print(f"Error durante la actualización: {str(e)}")
    finally:
        # Cerrar conexiones
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# Punto de entrada principal
if __name__ == "__main__":
    update_dailyledger()
