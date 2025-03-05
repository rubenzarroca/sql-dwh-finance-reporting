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

# ==== FUNCIONES PARA FISCAL_PERIODS ====

def load_fiscal_periods(conn, full_refresh=True):
    """Carga o actualiza la tabla silver.fiscal_periods."""
    logger.info("Iniciando carga de silver.fiscal_periods")
    
    try:
        cursor = conn.cursor()
        
        # Si es full_refresh, truncar la tabla
        if full_refresh:
            logger.info("Truncando tabla silver.fiscal_periods")
            cursor.execute("TRUNCATE TABLE silver.fiscal_periods CASCADE")
        
        # Determinar el rango de fechas a generar
        # Buscamos la fecha mínima en el libro diario
        cursor.execute("""
            SELECT MIN(to_timestamp(timestamp)) as min_date 
            FROM bronze.holded_dailyledger
        """)
        
        min_date_result = cursor.fetchone()
        if min_date_result and min_date_result[0]:
            min_date = min_date_result[0].date()
        else:
            # Si no hay datos, usar una fecha predeterminada
            min_date = date(2024, 1, 1)
        
        # Usamos el primer día del mes para la fecha mínima
        min_date = date(min_date.year, min_date.month, 1)
        
        # La fecha máxima es el final del mes actual
        today = date.today()
        max_date = date(today.year, today.month, 1)
        # Avanzamos al próximo mes y restamos un día para obtener el último día del mes actual
        next_month = max_date.replace(day=28) + timedelta(days=4)
        max_date = next_month - timedelta(days=next_month.day)
        
        logger.info(f"Generando períodos fiscales desde {min_date} hasta {max_date}")
        
        # Generar períodos fiscales mensuales
        periods = []
        current_date = min_date
        
        while current_date <= max_date:
            year = current_date.year
            month = current_date.month
            quarter = (month - 1) // 3 + 1
            
            # Determinar el último día del mes
            if month == 12:
                end_date = date(year, month, 31)
            else:
                end_date = date(year, month + 1, 1) - timedelta(days=1)
            
            # Crear período fiscal
            period = (
                year,  # period_year
                quarter,  # period_quarter
                month,  # period_month
                f"{year}-{month:02d}",  # period_name
                current_date,  # start_date
                end_date,  # end_date
                False,  # is_closed
                None  # closing_date
            )
            
            periods.append(period)
            
            # Avanzar al siguiente mes
            if month == 12:
                current_date = date(year + 1, 1, 1)
            else:
                current_date = date(year, month + 1, 1)
        
        # Insertar períodos fiscales
        if periods:
            insert_query = """
            INSERT INTO silver.fiscal_periods (
                period_year, period_quarter, period_month, period_name,
                start_date, end_date, is_closed, closing_date
            ) VALUES %s
            ON CONFLICT (period_year, period_month) DO UPDATE SET
                period_quarter = EXCLUDED.period_quarter,
                period_name = EXCLUDED.period_name,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date
            """
            
            logger.info(f"Insertando {len(periods)} períodos fiscales")
            execute_values(cursor, insert_query, periods)
            conn.commit()
            
            # Verificar carga
            cursor.execute("SELECT COUNT(*) FROM silver.fiscal_periods")
            count = cursor.fetchone()[0]
            logger.info(f"Carga completada. {count} períodos fiscales en silver.fiscal_periods")
        else:
            logger.warning("No hay períodos fiscales para generar")
        
        cursor.close()
        return True
    
    except Exception as e:
        logger.error(f"Error al cargar silver.fiscal_periods: {str(e)}")
        if 'conn' in locals() and conn and not conn.closed:
            conn.rollback()
        if 'cursor' in locals() and cursor and not cursor.closed:
            cursor.close()
        return False

# ==== FUNCIONES PARA JOURNAL_ENTRIES ====

def load_journal_entries(conn, full_refresh=True):
    """Carga o actualiza la tabla silver.journal_entries desde bronze.holded_dailyledger."""
    logger.info("Iniciando carga de silver.journal_entries")
    
    try:
        cursor = conn.cursor()
        
        # Si es full_refresh, truncar la tabla
        if full_refresh:
            logger.info("Truncando tabla silver.journal_entries")
            cursor.execute("TRUNCATE TABLE silver.journal_entries CASCADE")
        
        # Obtener entradas del libro diario agrupadas por número de asiento
        query = """
        SELECT 
            entrynumber,
            MIN(timestamp) as timestamp,
            MAX(description) as description,
            MAX(docdescription) as docdescription,
            MAX(type) as type,
            SUM(debit) as total_debit,
            SUM(credit) as total_credit,
            MAX(dwh_update_timestamp) as last_update
        FROM bronze.holded_dailyledger
        GROUP BY entrynumber
        ORDER BY entrynumber
        """
        
        cursor.execute(query)
        
        columns = [
            'entrynumber', 'timestamp', 'description', 'docdescription', 
            'type', 'total_debit', 'total_credit', 'last_update'
        ]
        
        entries = cursor.fetchall()
        df_entries = pd.DataFrame(entries, columns=columns)
        
        logger.info(f"Se obtuvieron {len(df_entries)} asientos contables agrupados")
        
        # Obtener períodos fiscales para asignar a cada asiento
        cursor.execute("""
            SELECT period_id, start_date, end_date
            FROM silver.fiscal_periods
            ORDER BY start_date
        """)
        
        periods = cursor.fetchall()
        period_map = {}
        
        for period_id, start_date, end_date in periods:
            # Convertir fechas a timestamps para compararlas con los asientos
            start_timestamp = int(datetime.combine(start_date, datetime.min.time()).timestamp())
            end_timestamp = int(datetime.combine(end_date, datetime.max.time()).timestamp())
            period_map[(start_timestamp, end_timestamp)] = period_id
        
        # Batch ID para esta carga
        batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Preparar datos para inserción
        transformed_entries = []
        
        for _, row in df_entries.iterrows():
            # Convertir timestamp a fecha
            entry_date = datetime.fromtimestamp(row['timestamp']).date()
            
            # Determinar período fiscal
            period_id = None
            entry_timestamp = row['timestamp']
            
            for (start_timestamp, end_timestamp), pid in period_map.items():
                if start_timestamp <= entry_timestamp <= end_timestamp:
                    period_id = pid
                    break
            
            # Determinar si es un asiento especial
            is_closing_entry = "CIERRE" in str(row['description']).upper() or "CLOSING" in str(row['description']).upper()
            is_opening_entry = "APERTURA" in str(row['description']).upper() or "OPENING" in str(row['description']).upper()
            is_adjustment = "AJUSTE" in str(row['description']).upper() or "ADJUSTMENT" in str(row['description']).upper()
            
            # Crear entrada para inserción
            entry = (
                row['entrynumber'],  # entry_number
                entry_date,          # entry_date
                row['timestamp'],    # original_timestamp
                period_id,           # period_id
                row['type'],         # entry_type
                row['description'],  # description
                row['docdescription'], # document_description
                is_closing_entry,    # is_closing_entry
                is_opening_entry,    # is_opening_entry
                is_adjustment,       # is_adjustment
                False,               # is_checked
                'Posted',            # entry_status
                row['total_debit'],  # total_debit
                row['total_credit'], # total_credit
                datetime.now(),      # dwh_created_at
                datetime.now(),      # dwh_updated_at
                'bronze.holded_dailyledger', # dwh_source_table
                batch_id             # dwh_batch_id
            )
            
            transformed_entries.append(entry)
        
        # Insertar datos
        if transformed_entries:
            # Preparar la consulta de inserción o actualización
            if full_refresh:
                insert_query = """
                INSERT INTO silver.journal_entries (
                    entry_number, entry_date, original_timestamp, period_id,
                    entry_type, description, document_description,
                    is_closing_entry, is_opening_entry, is_adjustment,
                    is_checked, entry_status, total_debit, total_credit,
                    dwh_created_at, dwh_updated_at, dwh_source_table, dwh_batch_id
                ) VALUES %s
                """
            else:
                insert_query = """
                INSERT INTO silver.journal_entries (
                    entry_number, entry_date, original_timestamp, period_id,
                    entry_type, description, document_description,
                    is_closing_entry, is_opening_entry, is_adjustment,
                    is_checked, entry_status, total_debit, total_credit,
                    dwh_created_at, dwh_updated_at, dwh_source_table, dwh_batch_id
                ) VALUES %s
                ON CONFLICT (entry_number) DO UPDATE SET
                    entry_date = EXCLUDED.entry_date,
                    original_timestamp = EXCLUDED.original_timestamp,
                    period_id = EXCLUDED.period_id,
                    entry_type = EXCLUDED.entry_type,
                    description = EXCLUDED.description,
                    document_description = EXCLUDED.document_description,
                    is_closing_entry = EXCLUDED.is_closing_entry,
                    is_opening_entry = EXCLUDED.is_opening_entry,
                    is_adjustment = EXCLUDED.is_adjustment,
                    total_debit = EXCLUDED.total_debit,
                    total_credit = EXCLUDED.total_credit,
                    dwh_updated_at = CURRENT_TIMESTAMP,
                    dwh_batch_id = EXCLUDED.dwh_batch_id
                """
            
            logger.info(f"Insertando {len(transformed_entries)} asientos en silver.journal_entries")
            execute_values(cursor, insert_query, transformed_entries)
            conn.commit()
            
            # Verificar carga
            cursor.execute("SELECT COUNT(*) FROM silver.journal_entries")
            count = cursor.fetchone()[0]
            logger.info(f"Carga completada. {count} asientos en silver.journal_entries")
            
            # Generar estadísticas por mes
            cursor.execute("""
                SELECT 
                    TO_CHAR(entry_date, 'YYYY-MM') as month,
                    COUNT(*) as entries,
                    SUM(total_debit) as total_debit
                FROM silver.journal_entries
                GROUP BY TO_CHAR(entry_date, 'YYYY-MM')
                ORDER BY month
            """)
            
            month_stats = cursor.fetchall()
            logger.info("Distribución de asientos por mes:")
            for month, entries, total in month_stats:
                logger.info(f"  {month}: {entries} asientos, {total:.2f} € en débitos")
        else:
            logger.warning("No hay asientos para cargar en silver.journal_entries")
        
        cursor.close()
        return True
    
    except Exception as e:
        logger.error(f"Error al cargar silver.journal_entries: {str(e)}")
        if 'conn' in locals() and conn and not conn.closed:
            conn.rollback()
        if 'cursor' in locals() and cursor and not cursor.closed:
            cursor.close()
        return False

# ==== FUNCIONES PARA JOURNAL_LINES ====

def load_journal_lines(conn, full_refresh=True):
    """Carga o actualiza la tabla silver.journal_lines desde bronze.holded_dailyledger."""
    logger.info("Iniciando carga de silver.journal_lines")
    
    try:
        cursor = conn.cursor()
        
        # Si es full_refresh, truncar la tabla
        if full_refresh:
            logger.info("Truncando tabla silver.journal_lines")
            cursor.execute("TRUNCATE TABLE silver.journal_lines CASCADE")
        
        # Obtener las líneas de asientos y los IDs de asientos de silver.journal_entries
        query = """
        SELECT 
            dl.entrynumber,
            dl.line,
            je.entry_id,
            dl.account,
            dl.debit,
            dl.credit,
            dl.description,
            dl.tags,
            dl.checked,
            dl.dwh_update_timestamp
        FROM bronze.holded_dailyledger dl
        JOIN silver.journal_entries je ON dl.entrynumber = je.entry_number
        ORDER BY dl.entrynumber, dl.line
        """
        
        cursor.execute(query)
        
        columns = [
            'entrynumber', 'line', 'entry_id', 'account', 
            'debit', 'credit', 'description', 'tags', 
            'checked', 'update_timestamp'
        ]
        
        lines = cursor.fetchall()
        df_lines = pd.DataFrame(lines, columns=columns)
        
        logger.info(f"Se obtuvieron {len(df_lines)} líneas de asientos")
        
        # Obtener mapeo de números de cuenta a IDs de cuenta
        cursor.execute("""
            SELECT account_number, account_id
            FROM silver.accounts
        """)
        
        account_map = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Batch ID para esta carga
        batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Preparar datos para inserción
        transformed_lines = []
        skipped_lines = 0
        
        for _, row in df_lines.iterrows():
            # Obtener account_id desde el número de cuenta
            account_number = row['account']
            account_id = account_map.get(account_number)
            
            if not account_id:
                logger.warning(f"No se encontró account_id para el número de cuenta {account_number} en asiento {row['entrynumber']} línea {row['line']}")
                skipped_lines += 1
                continue
            
            # Determinar si la cuenta es relevante para impuestos
            is_tax_relevant = is_tax_relevant(account_number)
            
            # Determinar estado de reconciliación
            is_reconciled = row['checked'] == 'Yes' if row['checked'] else False
            
        # Parsear tags JSONB
            tags = row['tags']
            if tags:
                try:
                    if isinstance(tags, str):
                        tags_data = json.loads(tags)
                    else:
                        tags_data = tags
                    
                    # Extraer información de negocio de las etiquetas
                    cost_center = None
                    business_line = None
                    customer_id = None
                    vendor_id = None
                    project_id = None
                    
                    # Lógica para extraer metadatos de negocio de las etiquetas
                    # Esto dependerá de la estructura de tus etiquetas
                    # ...
                    
                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning(f"Error al parsear tags para asiento {row['entrynumber']} línea {row['line']}: {e}")
                    tags_data = {}
            else:
                tags_data = {}
           
           # Crear línea para inserción
           line = (
               row['entry_id'],          # entry_id
               row['line'],              # line_number
               account_id,               # account_id
               account_number,           # account_number
               row['debit'] or 0,        # debit_amount
               row['credit'] or 0,       # credit_amount
               row['description'],       # description
               json.dumps(tags_data),    # tags
               is_reconciled,            # is_reconciled
               is_reconciled,            # is_checked (mismo valor que is_reconciled por simplicidad)
               is_tax_relevant,          # is_tax_relevant
               None,                     # tax_code
               None,                     # cost_center (extraído de tags si está disponible)
               None,                     # business_line
               None,                     # customer_id
               None,                     # vendor_id
               None,                     # project_id
               datetime.now(),           # dwh_created_at
               datetime.now(),           # dwh_updated_at
               'bronze.holded_dailyledger', # dwh_source_table
               batch_id                  # dwh_batch_id
           )
           
           transformed_lines.append(line)
       
       # Insertar datos
       if transformed_lines:
           # Preparar la consulta de inserción o actualización
           if full_refresh:
               insert_query = """
               INSERT INTO silver.journal_lines (
                   entry_id, line_number, account_id, account_number,
                   debit_amount, credit_amount, description, tags,
                   is_reconciled, is_checked, is_tax_relevant, tax_code,
                   cost_center, business_line, customer_id, vendor_id, project_id,
                   dwh_created_at, dwh_updated_at, dwh_source_table, dwh_batch_id
               ) VALUES %s
               """
           else:
               insert_query = """
               INSERT INTO silver.journal_lines (
                   entry_id, line_number, account_id, account_number,
                   debit_amount, credit_amount, description, tags,
                   is_reconciled, is_checked, is_tax_relevant, tax_code,
                   cost_center, business_line, customer_id, vendor_id, project_id,
                   dwh_created_at, dwh_updated_at, dwh_source_table, dwh_batch_id
               ) VALUES %s
               ON CONFLICT (entry_id, line_number) DO UPDATE SET
                   account_id = EXCLUDED.account_id,
                   account_number = EXCLUDED.account_number,
                   debit_amount = EXCLUDED.debit_amount,
                   credit_amount = EXCLUDED.credit_amount,
                   description = EXCLUDED.description,
                   tags = EXCLUDED.tags,
                   is_reconciled = EXCLUDED.is_reconciled,
                   is_checked = EXCLUDED.is_checked,
                   is_tax_relevant = EXCLUDED.is_tax_relevant,
                   tax_code = EXCLUDED.tax_code,
                   cost_center = EXCLUDED.cost_center,
                   business_line = EXCLUDED.business_line,
                   customer_id = EXCLUDED.customer_id,
                   vendor_id = EXCLUDED.vendor_id,
                   project_id = EXCLUDED.project_id,
                   dwh_updated_at = CURRENT_TIMESTAMP,
                   dwh_batch_id = EXCLUDED.dwh_batch_id
               """
           
           logger.info(f"Insertando {len(transformed_lines)} líneas en silver.journal_lines")
           execute_values(cursor, insert_query, transformed_lines)
           conn.commit()
           
           # Verificar carga
           cursor.execute("SELECT COUNT(*) FROM silver.journal_lines")
           count = cursor.fetchone()[0]
           logger.info(f"Carga completada. {count} líneas en silver.journal_lines, {skipped_lines} líneas omitidas")
           
           # Generar estadísticas
           cursor.execute("""
               SELECT 
                   a.account_type,
                   COUNT(*) as line_count,
                   SUM(jl.debit_amount) as total_debit,
                   SUM(jl.credit_amount) as total_credit
               FROM silver.journal_lines jl
               JOIN silver.accounts a ON jl.account_id = a.account_id
               GROUP BY a.account_type
               ORDER BY a.account_type
           """)
           
           type_stats = cursor.fetchall()
           logger.info("Distribución de líneas por tipo de cuenta:")
           for account_type, count, debit, credit in type_stats:
               logger.info(f"  {account_type}: {count} líneas, {debit:.2f} € en débitos, {credit:.2f} € en créditos")
       else:
           logger.warning("No hay líneas para cargar en silver.journal_lines")
       
       cursor.close()
       return True
   
   except Exception as e:
       logger.error(f"Error al cargar silver.journal_lines: {str(e)}")
       if 'conn' in locals() and conn and not conn.closed:
           conn.rollback()
       if 'cursor' in locals() and cursor and not cursor.closed:
           cursor.close()
       return False

# ==== FUNCIONES PARA ACCOUNT_BALANCES ====

def load_account_balances(conn, full_refresh=True):
   """Carga o actualiza la tabla silver.account_balances agregando saldos por cuenta y período."""
   logger.info("Iniciando carga de silver.account_balances")
   
   try:
       cursor = conn.cursor()
       
       # Si es full_refresh, truncar la tabla
       if full_refresh:
           logger.info("Truncando tabla silver.account_balances")
           cursor.execute("TRUNCATE TABLE silver.account_balances")
       
       # Batch ID para esta carga
       batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
       
       # Calcular saldos por cuenta y período
       query = """
       INSERT INTO silver.account_balances (
           account_id, account_number, period_id,
           start_balance, period_debit, period_credit, end_balance,
           is_calculated, dwh_created_at, dwh_updated_at, dwh_batch_id
       )
       WITH account_movements AS (
           -- Calcular débitos y créditos por cuenta y período
           SELECT 
               jl.account_id,
               jl.account_number,
               je.period_id,
               SUM(jl.debit_amount) as total_debit,
               SUM(jl.credit_amount) as total_credit
           FROM silver.journal_lines jl
           JOIN silver.journal_entries je ON jl.entry_id = je.entry_id
           WHERE je.period_id IS NOT NULL
           GROUP BY jl.account_id, jl.account_number, je.period_id
       ),
       periods_with_accounts AS (
           -- Generar todas las combinaciones de cuentas y períodos
           SELECT 
               a.account_id,
               a.account_number,
               fp.period_id
           FROM silver.accounts a
           CROSS JOIN silver.fiscal_periods fp
       )
       SELECT 
           pwa.account_id,
           pwa.account_number,
           pwa.period_id,
           0 as start_balance, -- Inicialmente 0, se actualizará luego
           COALESCE(am.total_debit, 0) as period_debit,
           COALESCE(am.total_credit, 0) as period_credit,
           0 as end_balance, -- Inicialmente 0, se actualizará luego
           TRUE as is_calculated,
           CURRENT_TIMESTAMP as dwh_created_at,
           CURRENT_TIMESTAMP as dwh_updated_at,
           $1 as dwh_batch_id
       FROM periods_with_accounts pwa
       LEFT JOIN account_movements am 
           ON pwa.account_id = am.account_id 
           AND pwa.period_id = am.period_id
       -- Solo incluir combinaciones donde hay movimientos o la cuenta es relevante
       WHERE am.total_debit IS NOT NULL 
           OR am.total_credit IS NOT NULL
           OR EXISTS (
               SELECT 1 FROM silver.journal_lines jl
               WHERE jl.account_id = pwa.account_id
           )
       ON CONFLICT (account_id, period_id) DO UPDATE SET
           period_debit = EXCLUDED.period_debit,
           period_credit = EXCLUDED.period_credit,
           is_calculated = TRUE,
           dwh_updated_at = CURRENT_TIMESTAMP,
           dwh_batch_id = EXCLUDED.dwh_batch_id
       """
       
       cursor.execute(query, (batch_id,))
       rows_affected = cursor.rowcount
       conn.commit()
       
       logger.info(f"Insertados/actualizados {rows_affected} saldos de cuenta por período")
       
       # Ahora actualizar los saldos iniciales y finales por período
       # (Esto debe hacerse en orden cronológico)
       update_query = """
       WITH ordered_periods AS (
           SELECT period_id, start_date
           FROM silver.fiscal_periods
           ORDER BY start_date
       ),
       period_pairs AS (
           SELECT 
               op.period_id as current_period_id,
               LAG(op.period_id) OVER(ORDER BY op.start_date) as prev_period_id
           FROM ordered_periods op
       )
       UPDATE silver.account_balances ab
       SET 
           start_balance = CASE 
               WHEN pp.prev_period_id IS NULL THEN 0
               ELSE (
                   SELECT prev.end_balance
                   FROM silver.account_balances prev
                   WHERE prev.account_id = ab.account_id
                   AND prev.period_id = pp.prev_period_id
               )
           END,
           end_balance = CASE 
               WHEN pp.prev_period_id IS NULL THEN ab.period_debit - ab.period_credit
               ELSE (
                   SELECT prev.end_balance
                   FROM silver.account_balances prev
                   WHERE prev.account_id = ab.account_id
                   AND prev.period_id = pp.prev_period_id
               ) + ab.period_debit - ab.period_credit
           END
       FROM period_pairs pp
       WHERE ab.period_id = pp.current_period_id
       """
       
       cursor.execute(update_query)
       rows_updated = cursor.rowcount
       conn.commit()
       
       logger.info(f"Actualizados saldos iniciales y finales para {rows_updated} registros")
       
       # Verificar carga
       cursor.execute("SELECT COUNT(*) FROM silver.account_balances")
       count = cursor.fetchone()[0]
       logger.info(f"Carga completada. {count} saldos de cuenta por período")
       
       cursor.close()
       return True
   
   except Exception as e:
       logger.error(f"Error al cargar silver.account_balances: {str(e)}")
       if 'conn' in locals() and conn and not conn.closed:
           conn.rollback()
       if 'cursor' in locals() and cursor and not cursor.closed:
           cursor.close()
       return False

# ==== FUNCIÓN PRINCIPAL ====

def load_silver_layer(full_refresh=False, tables=None):
   """
   Carga todas las tablas de la capa Silver desde las tablas Bronze.
   
   Args:
       full_refresh (bool): Si es True, trunca las tablas antes de insertar datos
       tables (list): Lista de tablas a cargar, si es None, carga todas
   """
   # Tablas disponibles y su orden de carga
   available_tables = [
       "accounts",
       "fiscal_periods",
       "journal_entries",
       "journal_lines",
       "account_balances"
   ]
   
   # Si no se especificaron tablas, cargar todas
   if tables is None:
       tables = available_tables
   
   # Validar tablas especificadas
   for table in tables:
       if table not in available_tables:
           logger.warning(f"Tabla no reconocida: {table}")
   
   # Filtrar solo tablas válidas
   tables_to_load = [table for table in tables if table in available_tables]
   
   if not tables_to_load:
       logger.error("No hay tablas válidas para cargar")
       return False
   
   # Conectar a la base de datos
   try:
       conn = get_db_connection()
       
       logger.info(f"Iniciando carga de la capa Silver, tablas: {', '.join(tables_to_load)}")
       logger.info(f"Modo: {'Refresco completo' if full_refresh else 'Actualización incremental'}")
       
       # Iniciar la carga en el orden correcto para respetar dependencias
       for table in available_tables:
           if table in tables_to_load:
               if table == "accounts":
                   load_accounts(conn, full_refresh)
               elif table == "fiscal_periods":
                   load_fiscal_periods(conn, full_refresh)
               elif table == "journal_entries":
                   load_journal_entries(conn, full_refresh)
               elif table == "journal_lines":
                   load_journal_lines(conn, full_refresh)
               elif table == "account_balances":
                   load_account_balances(conn, full_refresh)
       
       conn.close()
       logger.info("Carga de la capa Silver completada exitosamente")
       return True
   
   except Exception as e:
       logger.error(f"Error en la carga de la capa Silver: {str(e)}")
       if 'conn' in locals() and conn and not conn.closed:
           conn.close()
       return False

# ==== FUNCIÓN MAIN PARA EJECUCIÓN DESDE LÍNEA DE COMANDOS ====

def main():
   """Función principal para ejecutar desde línea de comandos."""
   parser = argparse.ArgumentParser(description="Carga de datos en la capa Silver.")
   parser.add_argument("--full-refresh", action="store_true", help="Realizar refresco completo de las tablas")
   parser.add_argument("--tables", type=str, help="Tablas a cargar, separadas por comas (accounts,fiscal_periods,journal_entries,journal_lines,account_balances)")
   
   args = parser.parse_args()
   
   # Procesar argumentos
   full_refresh = args.full_refresh
   
   if args.tables:
       tables = args.tables.split(",")
   else:
       tables = None
   
   # Ejecutar la carga
   result = load_silver_layer(full_refresh, tables)
   
   # Salir con código apropiado
   if result:
       sys.exit(0)
   else:
       sys.exit(1)

if __name__ == "__main__":
   main()
