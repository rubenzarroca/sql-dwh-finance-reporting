"""
Loader para silver.accounts

Este módulo extrae datos de cuentas contables de la capa Bronze,
los enriquece con metadatos según el Plan General Contable español,
y los carga en la tabla silver.accounts para su uso en reporting financiero.
"""

import os
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Importar utilidades compartidas
from utils import get_db_connection

# Mapeos del PGC para el Balance
BALANCE_SECTION_MAPPING = {
    # Activo No Corriente
    '20': {'section': 'ACTIVO', 'subsection': 'ACTIVO NO CORRIENTE', 'group': 'Inmovilizado intangible', 'order': 10},
    '21': {'section': 'ACTIVO', 'subsection': 'ACTIVO NO CORRIENTE', 'group': 'Inmovilizado material', 'order': 20},
    '22': {'section': 'ACTIVO', 'subsection': 'ACTIVO NO CORRIENTE', 'group': 'Inversiones inmobiliarias', 'order': 30},
    '23': {'section': 'ACTIVO', 'subsection': 'ACTIVO NO CORRIENTE', 'group': 'Inmovilizaciones Materiales en Curso', 'order': 40},
    '24': {'section': 'ACTIVO', 'subsection': 'ACTIVO NO CORRIENTE', 'group': 'Inversiones en empresas del grupo y asociadas a largo plazo', 'order': 50},
    '25': {'section': 'ACTIVO', 'subsection': 'ACTIVO NO CORRIENTE', 'group': 'Inversiones financieras a largo plazo', 'order': 60},
    '26': {'section': 'ACTIVO', 'subsection': 'ACTIVO NO CORRIENTE', 'group': 'Inversiones financieras a largo plazo', 'order': 60},
    '28': {'section': 'ACTIVO', 'subsection': 'ACTIVO NO CORRIENTE', 'group': 'Amortización acumulada', 'order': 70},
    '29': {'section': 'ACTIVO', 'subsection': 'ACTIVO NO CORRIENTE', 'group': 'Deterioro de valor', 'order': 80},
    '474': {'section': 'ACTIVO', 'subsection': 'ACTIVO NO CORRIENTE', 'group': 'Activos por Impuesto diferido', 'order': 90},
    
    # Activo Corriente
    '30': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Existencias', 'order': 100},
    '31': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Existencias', 'order': 100},
    '32': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Existencias', 'order': 100},
    '33': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Existencias', 'order': 100},
    '34': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Existencias', 'order': 100},
    '35': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Existencias', 'order': 100},
    '36': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Existencias', 'order': 100},
    '39': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Existencias', 'order': 100},
    '407': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Existencias', 'order': 100},
    
    '43': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Deudores comerciales y otras cuentas a cobrar', 'order': 110},
    '44': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Deudores comerciales y otras cuentas a cobrar', 'order': 110},
    '460': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Deudores comerciales y otras cuentas a cobrar', 'order': 110},
    '470': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Deudores comerciales y otras cuentas a cobrar', 'order': 110},
    '471': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Deudores comerciales y otras cuentas a cobrar', 'order': 110},
    '472': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Deudores comerciales y otras cuentas a cobrar', 'order': 110},
    '544': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Deudores comerciales y otras cuentas a cobrar', 'order': 110},
    
    '53': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Inversiones en empresas del grupo y asociadas a corto plazo', 'order': 120},
    '5580': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Inversiones en empresas del grupo y asociadas a corto plazo', 'order': 120},
    
    '54': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Inversiones financieras a corto plazo', 'order': 130},
    '55': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Inversiones financieras a corto plazo', 'order': 130},
    
    '480': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Periodificaciones a corto plazo', 'order': 140},
    '567': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Periodificaciones a corto plazo', 'order': 140},
    
    '57': {'section': 'ACTIVO', 'subsection': 'ACTIVO CORRIENTE', 'group': 'Efectivo y otros activos líquidos equivalentes', 'order': 150},
    
    # Patrimonio Neto
    '10': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PATRIMONIO NETO', 'group': 'Fondos propios', 'subgroup': 'Capital', 'order': 200},
    '11': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PATRIMONIO NETO', 'group': 'Fondos propios', 'subgroup': 'Reservas', 'order': 210},
    '12': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PATRIMONIO NETO', 'group': 'Fondos propios', 'subgroup': 'Resultados', 'order': 220},
    '13': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PATRIMONIO NETO', 'group': 'Subvenciones, donaciones y legados recibidos', 'order': 230},
    
    # Pasivo No Corriente
    '14': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO NO CORRIENTE', 'group': 'Provisiones a largo plazo', 'order': 300},
    '15': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO NO CORRIENTE', 'group': 'Deudas a largo plazo', 'order': 310},
    '16': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO NO CORRIENTE', 'group': 'Deudas a largo plazo', 'order': 310},
    '17': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO NO CORRIENTE', 'group': 'Deudas a largo plazo', 'order': 310},
    '18': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO NO CORRIENTE', 'group': 'Pasivos por impuestos diferidos', 'order': 320},
    '479': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO NO CORRIENTE', 'group': 'Pasivos por impuestos diferidos', 'order': 320},
    '181': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO NO CORRIENTE', 'group': 'Periodificaciones a largo plazo', 'order': 330},
    
    # Pasivo Corriente
    '499': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO CORRIENTE', 'group': 'Provisiones a corto plazo', 'order': 400},
    '529': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO CORRIENTE', 'group': 'Provisiones a corto plazo', 'order': 400},
    '50': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO CORRIENTE', 'group': 'Deudas a corto plazo', 'order': 410},
    '51': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO CORRIENTE', 'group': 'Deudas a corto plazo', 'order': 410},
    '52': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO CORRIENTE', 'group': 'Deudas a corto plazo', 'order': 410},
    '55': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO CORRIENTE', 'group': 'Deudas a corto plazo', 'order': 410},
    '40': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO CORRIENTE', 'group': 'Acreedores comerciales y otras cuentas a pagar', 'order': 420},
    '41': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO CORRIENTE', 'group': 'Acreedores comerciales y otras cuentas a pagar', 'order': 420},
    '465': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO CORRIENTE', 'group': 'Acreedores comerciales y otras cuentas a pagar', 'order': 420},
    '475': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO CORRIENTE', 'group': 'Acreedores comerciales y otras cuentas a pagar', 'order': 420},
    '476': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO CORRIENTE', 'group': 'Acreedores comerciales y otras cuentas a pagar', 'order': 420},
    '477': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO CORRIENTE', 'group': 'Acreedores comerciales y otras cuentas a pagar', 'order': 420},
    '485': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO CORRIENTE', 'group': 'Periodificaciones a corto plazo', 'order': 430},
    '568': {'section': 'PATRIMONIO NETO Y PASIVO', 'subsection': 'PASIVO CORRIENTE', 'group': 'Periodificaciones a corto plazo', 'order': 430},
}

# Mapeos del PGC para la Cuenta de PyG
PYG_SECTION_MAPPING = {
    # Ingresos de Explotación
    '70': {'section': 'RESULTADO DE EXPLOTACIÓN', 'group': 'Importe neto de la cifra de negocios', 'order': 10},
    '71': {'section': 'RESULTADO DE EXPLOTACIÓN', 'group': 'Variación de existencias', 'order': 20},
    '73': {'section': 'RESULTADO DE EXPLOTACIÓN', 'group': 'Trabajos realizados por la empresa para su activo', 'order': 30},
    '74': {'section': 'RESULTADO DE EXPLOTACIÓN', 'group': 'Otros ingresos de explotación', 'order': 40},
    '75': {'section': 'RESULTADO DE EXPLOTACIÓN', 'group': 'Otros ingresos de explotación', 'order': 40},
    
    # Gastos de Explotación
    '60': {'section': 'RESULTADO DE EXPLOTACIÓN', 'group': 'Aprovisionamientos', 'order': 50},
    '61': {'section': 'RESULTADO DE EXPLOTACIÓN', 'group': 'Aprovisionamientos', 'order': 50},
    '62': {'section': 'RESULTADO DE EXPLOTACIÓN', 'group': 'Otros gastos de explotación', 'order': 60},
    '63': {'section': 'RESULTADO DE EXPLOTACIÓN', 'group': 'Tributos', 'order': 65},
    '64': {'section': 'RESULTADO DE EXPLOTACIÓN', 'group': 'Gastos de personal', 'order': 70},
    '65': {'section': 'RESULTADO DE EXPLOTACIÓN', 'group': 'Otros gastos de explotación', 'order': 60},
    '68': {'section': 'RESULTADO DE EXPLOTACIÓN', 'group': 'Amortización del inmovilizado', 'order': 80},
    '69': {'section': 'RESULTADO DE EXPLOTACIÓN', 'group': 'Pérdidas por deterioro y otras dotaciones', 'order': 90},
    
    # Resultado Financiero
    '76': {'section': 'RESULTADO FINANCIERO', 'group': 'Ingresos financieros', 'order': 110},
    '77': {'section': 'RESULTADO FINANCIERO', 'group': 'Ingresos excepcionales', 'order': 120},
    '66': {'section': 'RESULTADO FINANCIERO', 'group': 'Gastos financieros', 'order': 130},
    '67': {'section': 'RESULTADO FINANCIERO', 'group': 'Gastos excepcionales', 'order': 140},
    
    # Impuestos
    '630': {'section': 'IMPUESTOS', 'group': 'Impuestos sobre beneficios', 'order': 200},
    '631': {'section': 'IMPUESTOS', 'group': 'Otros tributos', 'order': 210},
}

def extract_bronze_accounts(conn) -> pd.DataFrame:
    """
    Extrae los datos de cuentas de bronze.holded_accounts.
    
    Args:
        conn: Conexión a la base de datos
        
    Returns:
        DataFrame con los datos de las cuentas
    """
    try:
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
            dwh_batch_id
        FROM bronze.holded_accounts
        ORDER BY num
        """
        
        logger.info("Extrayendo datos de cuentas desde bronze.holded_accounts")
        df = pd.read_sql(query, conn)
        
        logger.info(f"Extraídas {len(df)} cuentas de la capa bronze")
        return df
    
    except Exception as e:
        logger.error(f"Error al extraer cuentas de bronze: {str(e)}")
        raise

def determine_account_type(account_number: int) -> str:
    """
    Determina el tipo de cuenta según el Plan General Contable español.
    
    Args:
        account_number: Número de cuenta de 8 dígitos
        
    Returns:
        Tipo de cuenta: Asset, Liability, Equity, Income, o Expense
    """
    # Extraer primer dígito (grupo principal)
    first_digit = account_number // 10000000
    
    # Mapeo simple de grupos
    group_type_map = {
        2: "Asset",      # Grupo 2: Activo no corriente
        3: "Asset",      # Grupo 3: Existencias
        4: "Asset",      # Grupo 4: Acreedores y deudores (por defecto Asset)
        6: "Expense",    # Grupo 6: Gastos
        7: "Income",     # Grupo 7: Ingresos
    }
    
    # Para los grupos que necesitan análisis adicional
    if first_digit == 1:  # Grupo 1: FINANCIACIÓN BÁSICA
        # Subgrupos 10-13 son Patrimonio Neto
        if account_number // 1000000 <= 13:
            return "Equity"
        # El resto son generalmente Pasivos
        else:
            return "Liability"
            
    elif first_digit == 4:  # Grupo 4: ACREEDORES Y DEUDORES
        # Subgrupos específicos que son pasivos
        if account_number // 1000000 in [40, 41, 47]:
            return "Liability"
        # El resto son activos
        else:
            return "Asset"
            
    elif first_digit == 5:  # Grupo 5: CUENTAS FINANCIERAS
        # Subgrupos que son pasivos
        if account_number // 1000000 in [50, 51, 52, 56]:
            return "Liability"
        # Subgrupos que son activos
        else:
            return "Asset"
            
    # Usar el mapeo simple para el resto
    return group_type_map.get(first_digit, "Unknown")

def determine_account_subtype(account_number: int) -> str:
    """
    Determina el subtipo de cuenta según el Plan General Contable español.
    
    Args:
        account_number: Número de cuenta de 8 dígitos
        
    Returns:
        Subtipo de cuenta (descripción)
    """
    # Extraer dos primeros dígitos (subgrupo)
    subgroup = account_number // 1000000
    
    # Mapeo de subgrupos a subtipos
    subtype_map = {
        # Grupo 1: FINANCIACIÓN BÁSICA
        10: "Capital",
        11: "Reservas",
        12: "Resultados pendientes de aplicación",
        13: "Subvenciones y donaciones",
        14: "Provisiones",
        15: "Deudas a largo plazo con características especiales",
        16: "Deudas a largo plazo con partes vinculadas",
        17: "Deudas a largo plazo por préstamos",
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
        36: "Subproductos y residuos",
        39: "Deterioro de valor de existencias",
        
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
        52: "Deudas a corto plazo por préstamos",
        53: "Inversiones financieras a corto plazo en partes vinculadas",
        54: "Otras inversiones financieras a corto plazo",
        55: "Otras cuentas no bancarias",
        56: "Fianzas y depósitos recibidos a corto plazo",
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
        69: "Pérdidas por deterioro",
        
        # Grupo 7: VENTAS E INGRESOS
        70: "Ventas de mercaderías y producción",
        71: "Variación de existencias",
        73: "Trabajos realizados para la empresa",
        74: "Subvenciones a la explotación",
        75: "Otros ingresos de gestión",
        76: "Ingresos financieros",
        77: "Beneficios procedentes de activos no corrientes",
        79: "Excesos y aplicaciones de provisiones"
    }
    
    return subtype_map.get(subgroup, f"Subgrupo {subgroup}")

def get_balance_mapping(account_number: int) -> Dict:
    """
    Obtiene el mapeo de balance para el número de cuenta dado.
    
    Args:
        account_number: Número de cuenta
        
    Returns:
        Diccionario con la información de mapeo del balance
    """
    account_str = str(account_number)
    
    # Probar coincidencias exactas primero
    if account_str in BALANCE_SECTION_MAPPING:
        return BALANCE_SECTION_MAPPING[account_str]
    
    # Luego probar por los primeros dígitos
    first_two = account_str[:2]
    if first_two in BALANCE_SECTION_MAPPING:
        return BALANCE_SECTION_MAPPING[first_two]
    
    first_one = account_str[:1]
    if first_one in BALANCE_SECTION_MAPPING:
        return BALANCE_SECTION_MAPPING[first_one]
    
    # Si no hay coincidencia, devolver un mapeo vacío
    return {
        'section': None,
        'subsection': None,
        'group': None,
        'subgroup': None,
        'order': 999
    }

def get_pyg_mapping(account_number: int) -> Dict:
    """
    Obtiene el mapeo de PyG para el número de cuenta dado.
    
    Args:
        account_number: Número de cuenta
        
    Returns:
        Diccionario con la información de mapeo de PyG
    """
    account_str = str(account_number)
    
    # Solo aplicar a cuentas de ingresos y gastos (grupos 6 y 7)
    if not (account_str.startswith('6') or account_str.startswith('7')):
        return {
            'section': None,
            'group': None,
            'subgroup': None,
            'order': 999
        }
    
    # Probar coincidencias exactas primero
    if account_str in PYG_SECTION_MAPPING:
        return PYG_SECTION_MAPPING[account_str]
    
    # Luego probar por los primeros dígitos
    first_two = account_str[:2]
    if first_two in PYG_SECTION_MAPPING:
        return PYG_SECTION_MAPPING[first_two]
    
    first_one = account_str[:1]
    if first_one in PYG_SECTION_MAPPING:
        return PYG_SECTION_MAPPING[first_one]
    
    # Si no hay coincidencia, devolver un mapeo vacío
    return {
        'section': None,
        'group': None,
        'subgroup': None,
        'order': 999
    }

def determine_parent_account(account_number: int) -> int:
    """
    Determina el número de cuenta padre para la jerarquía.
    Para cuentas de 8 dígitos, trunca a 7 dígitos.
    
    Args:
        account_number: Número de cuenta de 8 dígitos
        
    Returns:
        Número de cuenta padre
    """
    return (account_number // 10) * 10

def is_tax_relevant(account_number: int) -> bool:
    """
    Determina si una cuenta es relevante para cálculos fiscales.
    
    Args:
        account_number: Número de cuenta
        
    Returns:
        True si la cuenta es relevante para impuestos, False en caso contrario
    """
    account_str = str(account_number)
    
    # Cuentas de IVA
    if account_str.startswith('472') or account_str.startswith('477'):
        return True
    
    # Cuentas de impuesto de sociedades
    if account_str.startswith('473') or account_str.startswith('4740') or account_str.startswith('4745'):
        return True
    
    # Cuentas de ingresos (para declaraciones de IVA)
    if account_str.startswith('7'):
        return True
    
    # Cuentas de gastos (para declaraciones de IVA)
    if account_str.startswith('6'):
        return True
    
    return False

def transform_accounts_data(df: pd.DataFrame) -> List[Tuple]:
    """
    Transforma y enriquece los datos de cuentas para la capa silver.
    
    Args:
        df: DataFrame con datos de cuentas de bronze
        
    Returns:
        Lista de tuplas con datos transformados listos para inserción
    """
    logger.info("Transformando datos de cuentas para silver.accounts")
    
    batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
    transformed_data = []
    skipped_accounts = 0
    
    for _, row in df.iterrows():
        account_id = row['id']
        
        # Validar que el número de cuenta existe
        if pd.isna(row['num']) or row['num'] is None:
            logger.warning(f"Cuenta con ID {account_id} no tiene número, omitiendo")
            skipped_accounts += 1
            continue
        
        # Asegurar que el número de cuenta es un entero
        try:
            account_number = int(row['num'])
        except (ValueError, TypeError):
            logger.warning(f"Número de cuenta inválido para ID {account_id}: {row['num']}, omitiendo")
            skipped_accounts += 1
            continue
        
        # Rellenar a 8 dígitos si es necesario
        if account_number < 10000000:
            digits = len(str(account_number))
            logger.info(f"Cuenta {account_number} tiene {digits} dígitos en lugar de 8, rellenando")
            account_number = account_number * 10**(8 - digits)
        
        # Determinar tipo y subtipo de cuenta
        account_type = determine_account_type(account_number)
        account_subtype = determine_account_subtype(account_number)
        
        # Obtener mapeos de balance y PyG
        balance_mapping = get_balance_mapping(account_number)
        pyg_mapping = get_pyg_mapping(account_number)
        
        # Determinar cuenta padre para jerarquía
        parent_account = determine_parent_account(account_number)
        
        # Obtener grupo y subgrupo PGC
        pgc_group = account_number // 10000000
        pgc_subgroup = account_number // 1000000
        pgc_detail = account_number // 10000
        
        # Determinar si la cuenta es relevante para impuestos
        tax_relevant = is_tax_relevant(account_number)
        
        # Calcular fecha del último movimiento
        last_movement = None
        if row['debit'] > 0 or row['credit'] > 0:
            last_movement = row['dwh_update_timestamp'].date() if isinstance(row['dwh_update_timestamp'], datetime) else None
        
        # Crear tupla de datos para inserción
        account_data = (
            account_id,                          # account_id
            account_number,                      # account_number
            row['name'] or f"Cuenta {account_number}",  # account_name
            row['group'] or "Sin Grupo",         # account_group
            account_type,                        # account_type
            account_subtype,                     # account_subtype
            balance_mapping['section'],          # balance_section
            balance_mapping['subsection'],       # balance_subsection
            balance_mapping['group'],            # balance_group
            balance_mapping.get('subgroup'),     # balance_subgroup
            pyg_mapping['section'],              # pyg_section
            pyg_mapping['group'],                # pyg_group
            pyg_mapping.get('subgroup'),         # pyg_subgroup
            balance_mapping['order'],            # balance_order
            pyg_mapping['order'],                # pyg_order
            True,                                # is_analytic (todas las cuentas de 8 dígitos son analíticas)
            parent_account,                      # parent_account_number
            5,                                   # account_level (nivel 5 para cuentas de 8 dígitos)
            True,                                # is_active
            row['balance'] or 0,                 # current_balance
            row['debit'] or 0,                   # debit_balance
            row['credit'] or 0,                  # credit_balance
            last_movement,                       # last_movement_date
            pgc_group,                           # pgc_group
            pgc_subgroup,                        # pgc_subgroup
            pgc_detail,                          # pgc_detail
            tax_relevant,                        # tax_relevant
            datetime.now(),                      # dwh_created_at
            datetime.now(),                      # dwh_updated_at
            'bronze.holded_accounts',            # dwh_source_table
            batch_id                             # dwh_batch_id
        )
        
        transformed_data.append(account_data)
    
    logger.info(f"Transformación completada. {len(transformed_data)} cuentas procesadas, {skipped_accounts} omitidas")
    return transformed_data

def load_accounts_to_silver(conn, accounts_data: List[Tuple], full_refresh: bool = False) -> int:
    """
    Carga los datos transformados de cuentas en la tabla silver.accounts.
    
    Args:
        conn: Conexión a la base de datos
        accounts_data: Lista de tuplas con datos transformados
        full_refresh: Si es True, truncar la tabla destino antes de cargar
        
    Returns:
        Número de registros insertados
    """
    if not accounts_data:
        logger.warning("No hay datos para cargar en silver.accounts")
        return 0
    
    try:
        cursor = conn.cursor()
        
        # Si es full refresh, truncar la tabla destino
        if full_refresh:
            logger.info("Truncando tabla silver.accounts para full refresh")
            cursor.execute("TRUNCATE TABLE silver.accounts CASCADE")
        
        # Preparar consulta de inserción
        insert_query = """
        INSERT INTO silver.accounts (
            account_id, account_number, account_name, account_group, 
            account_type, account_subtype, balance_section, balance_subsection,
            balance_group, balance_subgroup, pyg_section, pyg_group,
            pyg_subgroup, balance_order, pyg_order, is_analytic,
            parent_account_number, account_level, is_active, current_balance,
            debit_balance, credit_balance, last_movement_date, pgc_group,
            pgc_subgroup, pgc_detail, tax_relevant, dwh_created_at,
            dwh_updated_at, dwh_source_table, dwh_batch_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Ejecutar inserción por lotes
        cursor.executemany(insert_query, accounts_data)
        conn.commit()
        
        # Obtener número de registros insertados
        cursor.execute("SELECT COUNT(*) FROM silver.accounts")
        count = cursor.fetchone()[0]
        
        logger.info(f"Carga completada. {count} registros insertados en silver.accounts")
        
        # Generar estadísticas de carga
        cursor.execute("""
            SELECT account_type, COUNT(*) 
            FROM silver.accounts 
            GROUP BY account_type 
            ORDER BY account_type
        """)
        
        type_stats = cursor.fetchall()
        logger.info("Distribución por tipo de cuenta:")
        for account_type, type_count in type_stats:
            logger.info(f"  {account_type}: {type_count} cuentas")
        
        cursor.close()
        return count
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error al cargar datos en silver.accounts: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        raise

def load_accounts(full_refresh: bool = True) -> bool:
    """
    Función principal para orquestar el proceso ETL de cuentas.
    
    Args:
        full_refresh: Si es True, realizar full refresh en lugar de carga incremental
        
    Returns:
        True si la carga fue exitosa, False en caso contrario
    """
    start_time = datetime.now()
    logger.info(f"Iniciando proceso ETL de cuentas a las {start_time}")
    
    try:
        # Obtener conexión a la base de datos
        conn = get_db_connection()
        
        # Extraer datos de la capa bronze
        df_accounts = extract_bronze_accounts(conn)
        
        # Transformar datos
        transformed_data = transform_accounts_data(df_accounts)
        
        # Cargar datos en la capa silver
        inserted_count = load_accounts_to_silver(conn, transformed_data, full_refresh)
        
        # Cerrar conexión
        conn.close()
        
        # Registrar finalización
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"ETL de cuentas completado en {duration:.2f} segundos, {inserted_count} registros cargados")
        
        return True
        
    except Exception as e:
        logger.error(f"ETL de cuentas fallido: {str(e)}")
        return False

if __name__ == "__main__":
    # Esto permite ejecutar este módulo como un script independiente
    import argparse
    
    parser = argparse.ArgumentParser(description='Cargar cuentas en la capa silver')
    parser.add_argument('--full-refresh', action='store_true', help='Realizar full refresh en lugar de carga incremental')
    
    args = parser.parse_args()
    
    success = load_accounts(full_refresh=args.full_refresh)
    exit(0 if success else 1)