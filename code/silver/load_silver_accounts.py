# load_silver_accounts.py - Script para poblar la tabla silver.accounts
import os
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import pandas as pd
import numpy as np
import uuid
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Establece conexión con la base de datos Supabase usando variables de entorno."""
    try:
        conn = psycopg2.connect(
            host=os.environ.get("SUPABASE_DB_HOST"),
            database=os.environ.get("SUPABASE_DB_NAME", "postgres"),
            user=os.environ.get("SUPABASE_DB_USER"),
            password=os.environ.get("SUPABASE_DB_PASSWORD"),
            port=os.environ.get("SUPABASE_DB_PORT", "5432")
        )
        logger.info("Conexión a la base de datos establecida correctamente")
        return conn
    except Exception as e:
        logger.error(f"Error al conectar a la base de datos: {str(e)}")
        raise

def determine_account_type(account_number):
    """
    Determina el tipo de cuenta basado en su número según el PGC español.
    
    Args:
        account_number (int): Número de cuenta de 8 dígitos
        
    Returns:
        str: Uno de los siguientes valores: 'Asset', 'Liability', 'Equity', 'Income', 'Expense'
    """
    # Extraemos el primer dígito (grupo principal)
    first_digit = account_number // 10000000
    
    # Extraemos el segundo dígito (subgrupo)
    second_digit = (account_number // 1000000) % 10
    
    # Diccionario de mapeo para grupos simples (sin excepciones)
    group_type_map = {
        2: "Asset",      # Grupo 2: ACTIVO NO CORRIENTE
        3: "Asset",      # Grupo 3: EXISTENCIAS
        6: "Expense",    # Grupo 6: COMPRAS Y GASTOS
        7: "Income",     # Grupo 7: VENTAS E INGRESOS
        8: "Expense",    # Grupo 8: GASTOS IMPUTADOS AL PATRIMONIO NETO
        9: "Income"      # Grupo 9: INGRESOS IMPUTADOS AL PATRIMONIO NETO
    }
    
    # Si el grupo está en el diccionario, devolvemos directamente el tipo
    if first_digit in group_type_map:
        return group_type_map[first_digit]
    
    # Para los grupos que requieren análisis adicional
    if first_digit == 1:  # Grupo 1: FINANCIACIÓN BÁSICA
        # Subgrupos 10-13 son Patrimonio Neto
        if second_digit <= 3:
            return "Equity"
        # Subgrupos 14-19 son generalmente Pasivo
        else:
            return "Liability"
            
    elif first_digit == 4:  # Grupo 4: ACREEDORES Y DEUDORES
        # Proveedores (40), Acreedores (41), Personal-acreedor (46), 
        # Administraciones Públicas acreedoras (475-476-477-479)
        if second_digit in [0, 1] or (second_digit == 6) or \
           (second_digit == 7 and (account_number // 10000) % 100 in [50, 51, 52, 58, 59, 60, 61, 70, 79]):
            return "Liability"
        # Clientes (43), Deudores (44), Personal-deudor (46), 
        # Administraciones Públicas deudoras (470-471-472-473-474)
        else:
            return "Asset"
            
    elif first_digit == 5:  # Grupo 5: CUENTAS FINANCIERAS
        # 50-51-52 Empréstitos/deudas, 55 Otras cuentas no bancarias (partidas pendientes),
        # 56 Fianzas recibidas
        if second_digit in [0, 1, 2, 5, 6]:
            return "Liability"
        # 53-54 Inversiones, 57 Tesorería, 58-59 Activos/ajustes
        else:
            return "Asset"
    
    # Si llegamos aquí, asignamos un tipo desconocido (caso improbable)
    logger.warning(f"Tipo de cuenta no determinado para número: {account_number}")
    return "Unknown"

def determine_account_subtype(account_number):
    """
    Determina el subtipo de cuenta basado en el número de cuenta según el PGC español.
    
    Args:
        account_number (int): Número de cuenta de 8 dígitos
        
    Returns:
        str: Subtipo de cuenta
    """
    # Extraemos los primeros dos dígitos (subgrupo)
    subgroup = account_number // 1000000
    
    # Mapa de subgrupos a subtipos según el PGC español
    subtype_map = {
        # Grupo 1: FINANCIACIÓN BÁSICA
        10: "Capital",
        11: "Reservas y otros instrumentos de patrimonio",
        12: "Resultados pendientes de aplicación",
        13: "Subvenciones, donaciones y ajustes por cambios de valor",
        14: "Provisiones",
        15: "Deudas a largo plazo con características especiales",
        16: "Deudas a largo plazo con partes vinculadas",
        17: "Deudas a largo plazo por préstamos y otros",
        18: "Pasivos por fianzas y garantías a largo plazo",
        19: "Situaciones transitorias de financiación",
        
        # Grupo 2: ACTIVO NO CORRIENTE
        20: "Inmovilizaciones intangibles",
        21: "Inmovilizaciones materiales",
        22: "Inversiones inmobiliarias",
        23: "Inmovilizaciones materiales en curso",
        24: "Inversiones financieras en partes vinculadas",
        25: "Otras inversiones financieras a largo plazo",
        26: "Fianzas y depósitos constituidos a largo plazo",
        28: "Amortización acumulada del inmovilizado",
        29: "Deterioro de valor de activos no corrientes",
        
        # Grupo 3: EXISTENCIAS
        30: "Comerciales",
        31: "Materias primas",
        32: "Otros aprovisionamientos",
        33: "Productos en curso",
        34: "Productos semiterminados",
        35: "Productos terminados",
        36: "Subproductos, residuos y materiales recuperados",
        39: "Deterioro de valor de las existencias",
        
        # Grupo 4: ACREEDORES Y DEUDORES
        40: "Proveedores",
        41: "Acreedores varios",
        43: "Clientes",
        44: "Deudores varios",
        46: "Personal",
        47: "Administraciones públicas",
        48: "Ajustes por periodificación",
        49: "Deterioro de valor de créditos comerciales",
        
        # Grupo 5: CUENTAS FINANCIERAS
        50: "Empréstitos y deudas a corto plazo",
        51: "Deudas a corto plazo con partes vinculadas",
        52: "Deudas a corto plazo por préstamos y otros",
        53: "Inversiones financieras a corto plazo en partes vinculadas",
        54: "Otras inversiones financieras a corto plazo",
        55: "Otras cuentas no bancarias",
        56: "Fianzas y depósitos recibidos y constituidos a corto plazo",
        57: "Tesorería",
        58: "Activos no corrientes mantenidos para la venta",
        59: "Deterioro del valor de inversiones financieras a corto plazo",
        
        # Grupo 6: COMPRAS Y GASTOS
        60: "Compras",
        61: "Variación de existencias",
        62: "Servicios exteriores",
        63: "Tributos",
        64: "Gastos de personal",
        65: "Otros gastos de gestión",
        66: "Gastos financieros",
        67: "Pérdidas procedentes de activos no corrientes",
        68: "Dotaciones para amortizaciones",
        69: "Pérdidas por deterioro y otras dotaciones",
        
        # Grupo 7: VENTAS E INGRESOS
        70: "Ventas de mercaderías y producción",
        71: "Variación de existencias",
        73: "Trabajos realizados para la empresa",
        74: "Subvenciones, donaciones y legados",
        75: "Otros ingresos de gestión",
        76: "Ingresos financieros",
        77: "Beneficios procedentes de activos no corrientes",
        79: "Excesos y aplicaciones de provisiones"
    }
    
    return subtype_map.get(subgroup, f"Subgrupo {subgroup}")

def get_pgc_group_and_subgroup(account_number):
    """
    Extrae el grupo y subgrupo del PGC a partir del número de cuenta.
    
    Args:
        account_number (int): Número de cuenta de 8 dígitos
        
    Returns:
        tuple: (grupo, subgrupo)
    """
    group = account_number // 10000000
    subgroup = account_number // 1000000
    
    return group, subgroup

def build_parent_hierarchy(account_number):
    """
    Construye la jerarquía de cuentas padre truncando progresivamente los dígitos.
    Para cuentas de 8 dígitos, genera hasta 7 niveles de padres.
    
    Args:
        account_number (int): Número de cuenta de 8 dígitos
        
    Returns:
        list: Lista de números de cuenta padre, desde el más inmediato al más general
    """
    hierarchy = []
    # Truncamos progresivamente desde 7 dígitos hasta 1 dígito
    for i in range(1, 8):
        parent_number = (account_number // 10**i) * 10**i
        hierarchy.append(parent_number)
    
    return hierarchy

def determine_parent_account(account_number):
    """
    Determina el padre inmediato de una cuenta para la jerarquía.
    Para cuentas de 8 dígitos, el padre inmediato es la cuenta truncada a 7 dígitos.
    
    Args:
        account_number (int): Número de cuenta de 8 dígitos
        
    Returns:
        int: Número de cuenta padre
    """
    # Truncamos el último dígito para obtener el padre inmediato
    return (account_number // 10) * 10

def is_tax_relevant(account_number):
    """
    Determina si una cuenta es relevante para cálculos de impuestos.
    
    Args:
        account_number (int): Número de cuenta de 8 dígitos
        
    Returns:
        bool: True si la cuenta es relevante para impuestos, False en caso contrario
    """
    # Cuentas de IVA Repercutido e IVA Soportado
    if account_number // 10000 in [4720, 4770]:
        return True
    
    # Cuentas de Hacienda Pública (Impuesto de Sociedades)
    if account_number // 10000 in [4740, 4745, 4752]:
        return True
    
    # Cuentas de Ventas e Ingresos (para declaraciones de IVA)
    if account_number // 10000000 == 7:
        return True
    
    # Cuentas de Compras y Gastos (para declaraciones de IVA)
    if account_number // 10000000 == 6:
        return True
    
    return False

def fetch_bronze_accounts(conn):
    """
    Obtiene todos los datos de cuentas desde la capa Bronze.
    
    Args:
        conn: Conexión a la base de datos
        
    Returns:
        pd.DataFrame: DataFrame con las cuentas de la capa Bronze
    """
    try:
        cursor = conn.cursor()
        
        query = """
        SELECT 
            id, 
            color, 
            num, 
            name, 
            "group", 
            debit, 
            credit, 
            balance, 
            dwh_insert_timestamp,
            dwh_update_timestamp,
            dwh_batch_id,
            dwh_process_id
        FROM bronze.holded_accounts
        ORDER BY num
        """
        
        logger.info("Obteniendo datos de cuentas desde bronze.holded_accounts")
        cursor.execute(query)
        
        columns = [
            'id', 'color', 'num', 'name', 'group', 'debit', 'credit', 'balance',
            'insert_timestamp', 'update_timestamp', 'bronze_batch_id', 'bronze_process_id'
        ]
        
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        
        logger.info(f"Se obtuvieron {len(df)} registros de cuentas")
        cursor.close()
        
        return df
    
    except Exception as e:
        logger.error(f"Error al obtener datos de Bronze: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        raise

def transform_accounts_data(df):
    """
    Transforma y enriquece los datos de cuentas para la capa Silver.
    
    Args:
        df (pd.DataFrame): DataFrame con datos de cuentas desde la capa Bronze
        
    Returns:
        list: Lista de tuplas con los datos transformados listos para inserción
    """
    logger.info("Transformando datos de cuentas para silver.accounts")
    
    batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
    transformed_data = []
    skipped_accounts = 0
    
    for _, row in df.iterrows():
        account_id = row['id']
        
        # Validamos que tengamos un número de cuenta
        if pd.isna(row['num']) or row['num'] is None:
            logger.warning(f"Cuenta con ID {account_id} sin número, omitiendo")
            skipped_accounts += 1
            continue
        
        # Aseguramos que el número de cuenta sea un entero
        try:
            account_number = int(row['num'])
        except (ValueError, TypeError):
            logger.warning(f"Número de cuenta inválido para ID {account_id}: {row['num']}, omitiendo")
            skipped_accounts += 1
            continue
        
        # Si el número de cuenta no tiene 8 dígitos, lo ajustamos
        if account_number < 10000000:
            digits = len(str(account_number))
            logger.warning(f"Cuenta {account_number} tiene {digits} dígitos en lugar de 8, ajustando")
            account_number = account_number * 10**(8 - digits)
        
        # Determinamos el tipo y subtipo de cuenta
        account_type = determine_account_type(account_number)
        account_subtype = determine_account_subtype(account_number)
        
        # Determinamos la cuenta padre para la jerarquía
        parent_account = determine_parent_account(account_number)
        
        # Obtenemos el grupo y subgrupo del PGC
        pgc_group, pgc_subgroup = get_pgc_group_and_subgroup(account_number)
        
        # Determinamos si la cuenta es relevante para impuestos
        tax_relevant = is_tax_relevant(account_number)
        
        # Calculamos la fecha del último movimiento
        last_movement = None
        if row['debit'] > 0 or row['credit'] > 0:
            last_movement = row['update_timestamp'].date() if isinstance(row['update_timestamp'], datetime) else None
        
        # Creamos la tupla con todos los datos
        account_data = (
            account_id,                  # account_id
            account_number,              # account_number
            row['name'] or f"Account {account_number}",  # account_name
            row['group'] or "No Group",  # account_group
            account_type,                # account_type
            account_subtype,             # account_subtype
            True,                        # is_analytic (todas son analíticas al tener 8 dígitos)
            parent_account,              # parent_account_number
            5,                           # account_level (nivel 5 para cuentas de 8 dígitos)
            True,                        # is_active
            row['balance'] or 0,         # current_balance
            row['debit'] or 0,           # debit_balance
            row['credit'] or 0,          # credit_balance
            last_movement,               # last_movement_date
            pgc_group,                   # pgc_group
            pgc_subgroup,                # pgc_subgroup
            tax_relevant,                # tax_relevant
            datetime.now(),              # dwh_created_at
            datetime.now(),              # dwh_updated_at
            'bronze.holded_accounts',    # dwh_source_table
            batch_id                     # dwh_batch_id
        )
        
        transformed_data.append(account_data)
    
    logger.info(f"Transformación completada. {len(transformed_data)} cuentas procesadas, {skipped_accounts} omitidas")
    return transformed_data

def insert_accounts_to_silver(conn, accounts_data):
    """
    Inserta los datos transformados en la tabla silver.accounts.
    
    Args:
        conn: Conexión a la base de datos
        accounts_data (list): Lista de tuplas con datos de cuentas
        
    Returns:
        int: Número de registros insertados
    """
    if not accounts_data:
        logger.warning("No hay datos para insertar en silver.accounts")
        return 0
    
    try:
        cursor = conn.cursor()
        
        # Truncamos la tabla para carga inicial
        logger.info("Truncando tabla silver.accounts")
        cursor.execute("TRUNCATE TABLE silver.accounts CASCADE")
        
        # Preparamos la consulta de inserción
        insert_query = """
        INSERT INTO silver.accounts (
            account_id, account_number, account_name, account_group, 
            account_type, account_subtype, is_analytic, parent_account_number,
            account_level, is_active, current_balance, debit_balance,
            credit_balance, last_movement_date, pgc_group, pgc_subgroup,
            tax_relevant, dwh_created_at, dwh_updated_at, dwh_source_table,
            dwh_batch_id
        ) VALUES %s
        """
        
        # Insertamos todos los registros de una vez
        logger.info(f"Insertando {len(accounts_data)} registros en silver.accounts")
        execute_values(cursor, insert_query, accounts_data)
        conn.commit()
        
        # Verificamos la carga
        cursor.execute("SELECT COUNT(*) FROM silver.accounts")
        count = cursor.fetchone()[0]
        
        logger.info(f"Inserción completada. {count} registros insertados en silver.accounts")
        
        # Generamos estadísticas de la carga
        cursor.execute("""
            SELECT account_type, COUNT(*) 
            FROM silver.accounts 
            GROUP BY account_type 
            ORDER BY account_type
        """)
        
        type_stats = cursor.fetchall()
        logger.info("Distribución por tipo de cuenta:")
        for account_type, count in type_stats:
            logger.info(f"  {account_type}: {count} cuentas")
        
        cursor.execute("""
            SELECT pgc_group, COUNT(*) 
            FROM silver.accounts 
            GROUP BY pgc_group 
            ORDER BY pgc_group
        """)
        
        group_stats = cursor.fetchall()
        logger.info("Distribución por grupo del PGC:")
        for pgc_group, count in group_stats:
            logger.info(f"  Grupo {pgc_group}: {count} cuentas")
        
        cursor.close()
        return count
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Error durante la inserción en silver.accounts: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        raise

def main():
    """Función principal que orquesta el proceso ETL completo."""
    start_time = datetime.now()
    logger.info(f"Iniciando proceso ETL para silver.accounts: {start_time}")
    
    try:
        # Conexión a la base de datos
        conn = get_db_connection()
        
        # Obtener datos de la capa Bronze
        df_accounts = fetch_bronze_accounts(conn)
        
        # Transformar los datos
        transformed_data = transform_accounts_data(df_accounts)
        
        # Insertar en la capa Silver
        inserted_count = insert_accounts_to_silver(conn, transformed_data)
        
        # Cerrar conexión
        conn.close()
        
        # Calcular tiempo de ejecución
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"Proceso ETL completado exitosamente en {duration:.2f} segundos")
        logger.info(f"Se insertaron {inserted_count} registros en silver.accounts")
        
    except Exception as e:
        logger.error(f"Error en el proceso ETL: {str(e)}")
        if 'conn' in locals() and conn:
            conn.close()
        raise
    finally:
        logger.info(f"Proceso ETL finalizado: {datetime.now()}")

if __name__ == "__main__":
    main()
