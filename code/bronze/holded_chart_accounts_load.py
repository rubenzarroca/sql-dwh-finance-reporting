# holded_chart_accounts_load.py - Carga inicial del cuadro de cuentas
import os
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import uuid

def get_db_connection():
    """Establece conexión con la base de datos Supabase usando variables de entorno."""
    conn = psycopg2.connect(
        host=os.environ.get("SUPABASE_DB_HOST"),
        database=os.environ.get("SUPABASE_DB_NAME", "postgres"),
        user=os.environ.get("SUPABASE_DB_USER"),
        password=os.environ.get("SUPABASE_DB_PASSWORD"),
        port=os.environ.get("SUPABASE_DB_PORT", "5432")
    )
    return conn

def fetch_chart_of_accounts():
    """Obtiene el cuadro de cuentas desde la API de Holded."""
    api_key = os.environ.get("HOLDED_API_KEY")
    url = "https://api.holded.com/api/accounting/v1/chartofaccounts"
    
    # No se necesitan parámetros temporales para obtener todas las cuentas
    headers = {"key": api_key}
    
    print(f"Consultando API de Holded: {url}")
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        accounts = response.json()
        print(f"Obtenidas {len(accounts)} cuentas del cuadro de cuentas")
        return accounts
    else:
        print(f"Error al obtener datos: {response.status_code}")
        print(f"Respuesta: {response.text}")
        return None

def load_accounts_to_bronze(accounts_data):
    """Carga los datos del cuadro de cuentas en la tabla bronze.holded_accounts."""
    if not accounts_data:
        print("No hay datos para cargar")
        return 0
    
    # Generar información del proceso
    process_id = str(uuid.uuid4())
    batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
    current_time = datetime.now()
    
    try:
        # Conectar a la base de datos
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Truncar la tabla para carga inicial
        print("Truncando tabla bronze.holded_accounts")
        cursor.execute("TRUNCATE TABLE bronze.holded_accounts")
        
        # Preparar datos para la inserción
        prepared_data = []
        for account in accounts_data:
            prepared_account = (
                account.get('id'),
                account.get('color'),
                account.get('num'),
                account.get('name'),
                account.get('group'),
                account.get('debit'),
                account.get('credit'),
                account.get('balance'),
                'holded',         # dwh_source_system
                'accounts',       # dwh_source_entity
                current_time,     # dwh_insert_timestamp
                current_time,     # dwh_update_timestamp
                batch_id,         # dwh_batch_id
                process_id        # dwh_process_id
            )
            prepared_data.append(prepared_account)
        
        # Insertar los datos
        insert_query = """
        INSERT INTO bronze.holded_accounts (
            id, color, num, name, "group", debit, credit, balance,
            dwh_source_system, dwh_source_entity, dwh_insert_timestamp,
            dwh_update_timestamp, dwh_batch_id, dwh_process_id
        ) VALUES %s
        """
        
        print(f"Insertando {len(prepared_data)} registros en bronze.holded_accounts")
        execute_values(cursor, insert_query, prepared_data)
        conn.commit()
        
        # Verificar la carga
        cursor.execute("SELECT COUNT(*) FROM bronze.holded_accounts")
        count = cursor.fetchone()[0]
        print(f"Carga completada. {count} registros insertados")
        
        # Mostrar resumen por categoría si hay registros
        if count > 0:
            cursor.execute("""
                SELECT "group" AS categoria, COUNT(*) AS num_cuentas
                FROM bronze.holded_accounts
                GROUP BY "group"
                ORDER BY "group"
            """)
            
            print("\nResumen por categoría:")
            for row in cursor.fetchall():
                print(f"  - {row[0]}: {row[1]} cuentas")
        
        cursor.close()
        conn.close()
        return count
    
    except Exception as e:
        print(f"Error durante la carga: {str(e)}")
        return 0

def main():
    """Función principal que orquesta el proceso de carga."""
    print("=" * 50)
    print(f"INICIO CARGA INICIAL CUADRO DE CUENTAS: {datetime.now()}")
    print("=" * 50)
    
    try:
        # Paso 1: Obtener datos de la API
        accounts = fetch_chart_of_accounts()
        
        # Paso 2: Cargar datos en la capa Bronze
        rows_inserted = load_accounts_to_bronze(accounts)
        
        print("=" * 50)
        print(f"FIN CARGA INICIAL: {datetime.now()}")
        print(f"Total cuentas cargadas: {rows_inserted}")
        print("=" * 50)
    
    except Exception as e:
        print(f"ERROR EN EL PROCESO: {str(e)}")

# Punto de entrada del script
if __name__ == "__main__":
    main()
