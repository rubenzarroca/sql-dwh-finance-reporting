# holded_chart_accounts_update.py
import os
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
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

# Función para obtener los datos de la API de Holded
def fetch_holded_chart_of_accounts():
    api_key = os.environ.get("HOLDED_API_KEY")
    
    headers = {
        "Accept": "application/json",
        "key": api_key
    }
    
    url = "https://api.holded.com/api/accounting/v1/chartofaccounts"
    
    print(f"Consultando API de Holded: {url}")
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        print("Datos recibidos correctamente de la API")
        return response.json()
    else:
        print(f"Error al obtener datos: {response.status_code}")
        print(f"Respuesta: {response.text}")
        return None

# Función principal para actualizar el cuadro de cuentas
def update_chart_of_accounts():
    print("=================================================")
    print(f"Inicio de actualización del cuadro de cuentas: {datetime.now()}")
    print("=================================================")

    # Obtener datos de la API
    accounts_data = fetch_holded_chart_of_accounts()
    
    if not accounts_data:
        print("No se pudieron obtener datos. Finalizando proceso.")
        return
    
    # Convertir a DataFrame
    print("Procesando datos recibidos...")
    df_accounts = pd.DataFrame(accounts_data)
    
    # Si no hay datos, terminar
    if df_accounts.empty:
        print("No hay cuentas para procesar. Finalizando.")
        return
    
    # Asegurar que tenemos todas las columnas necesarias
    required_columns = ['id', 'color', 'num', 'name', 'group', 'debit', 'credit', 'balance']
    for col in required_columns:
        if col not in df_accounts.columns:
            df_accounts[col] = None
    
    # Añadir columnas técnicas
    batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
    process_id = f"COA_UPDATE_{batch_id}"
    
    df_accounts['dwh_source_system'] = 'holded'
    df_accounts['dwh_source_entity'] = 'accounts'
    df_accounts['dwh_insert_timestamp'] = datetime.now()
    df_accounts['dwh_update_timestamp'] = datetime.now()
    df_accounts['dwh_batch_id'] = batch_id
    df_accounts['dwh_process_id'] = process_id
    
    # Convertir NaN a None para evitar errores en PostgreSQL
    df_accounts = df_accounts.replace({np.nan: None})
    
    try:
        # Conectar a la base de datos
        print("Conectando a la base de datos...")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Obtener IDs de cuentas existentes
        cursor.execute("SELECT id FROM bronze.holded_accounts")
        existing_ids = {row[0] for row in cursor.fetchall()}
        
        # Contadores para el resumen
        new_accounts = 0
        updated_accounts = 0
        
        # Para cada cuenta, insertar o actualizar
        print("Procesando cuentas...")
        for _, row in df_accounts.iterrows():
            account_id = row['id']
            
            if account_id in existing_ids:
                # Actualizar cuenta existente
                cursor.execute("""
                    UPDATE bronze.holded_accounts 
                    SET 
                        color = %s,
                        num = %s,
                        name = %s,
                        "group" = %s,
                        debit = %s,
                        credit = %s,
                        balance = %s,
                        dwh_update_timestamp = %s,
                        dwh_batch_id = %s,
                        dwh_process_id = %s
                    WHERE id = %s
                """, (
                    row['color'], row['num'], row['name'], row['group'], 
                    row['debit'], row['credit'], row['balance'],
                    row['dwh_update_timestamp'], row['dwh_batch_id'], row['dwh_process_id'],
                    account_id
                ))
                updated_accounts += 1
            else:
                # Insertar nueva cuenta
                cursor.execute("""
                    INSERT INTO bronze.holded_accounts 
                    (id, color, num, name, "group", debit, credit, balance,
                     dwh_source_system, dwh_source_entity, dwh_insert_timestamp, 
                     dwh_update_timestamp, dwh_batch_id, dwh_process_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    account_id, row['color'], row['num'], row['name'], row['group'], 
                    row['debit'], row['credit'], row['balance'],
                    row['dwh_source_system'], row['dwh_source_entity'], 
                    row['dwh_insert_timestamp'], row['dwh_update_timestamp'], 
                    row['dwh_batch_id'], row['dwh_process_id']
                ))
                new_accounts += 1
        
        # Confirmar cambios
        conn.commit()
        
        print("=================================================")
        print(f"Actualización completada: {datetime.now()}")
        print(f"Cuentas nuevas: {new_accounts}")
        print(f"Cuentas actualizadas: {updated_accounts}")
        print(f"Total de cuentas procesadas: {len(df_accounts)}")
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
    update_chart_of_accounts()
