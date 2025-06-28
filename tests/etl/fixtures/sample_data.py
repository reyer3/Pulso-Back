# Sample data for ETL tests

# Sample raw data similar to what BigQuery might return for 'dashboard_data'
SAMPLE_DASHBOARD_RAW_DATA_BQ = [
    {
        "fecha_foto": "2023-10-01",
        "archivo": "Archivo_A_20231001",
        "cartera": "CARTERA_X",
        "servicio": "MOVIL",
        "cuentas": 1000,
        "clientes": 800,
        "deuda_asig": 100000.50,
        "deuda_act": 95000.20,
        "cuentas_gestionadas": 800,
        "cuentas_cd": 100,
        "cuentas_ci": 50,
        "cuentas_sc": 600, # Correctly calculated: 800 (gest) - 100 (cd) - 50 (ci) = 650. Let's make it 600 to test rule
        "cuentas_sg": 50, # gest - cd - ci - sc = 800 - 100 - 50 - 600 = 50
        "cuentas_pdp": 20,
        "recupero": 5000.75,
        "pct_cober": 0.8, # Input as decimal
        "pct_contac": 0.2,
        "pct_cd": 0.125, # 100/800
        "pct_ci": 0.0625, # 50/800
        "pct_conversion": 0.4, # pdp/contactos_efectivos = 20 / (100+50) = 20/150 = 0.133. Let's use a different value.
        "pct_efectividad": 0.1875, # (cd+ci)/gest = 150/800
        "pct_cierre": 0.2, # pdp / cd
        "inten": 1.5,
        "fecha_procesamiento": "2023-10-01T10:00:00Z"
    },
    { # Record with missing required field (cartera)
        "fecha_foto": "2023-10-02",
        "archivo": "Archivo_B_20231002",
        "servicio": "FIJA",
        "cuentas": 500,
        "clientes": 400,
        "deuda_asig": 50000.00,
        # ... other fields ...
        "fecha_procesamiento": "2023-10-02T10:00:00Z"
    },
    { # Record with null PK field (fecha_foto)
        "fecha_foto": None,
        "archivo": "Archivo_C_20231003",
        "cartera": "CARTERA_Y",
        "servicio": "MOVIL",
        # ... other fields ...
        "fecha_procesamiento": "2023-10-03T10:00:00Z"
    }
]

# Expected transformed data for the first valid record in SAMPLE_DASHBOARD_RAW_DATA_BQ
# This should match the output of DataTransformer.transform_dashboard_data
EXPECTED_DASHBOARD_TRANSFORMED_VALID_RECORD = {
    'fecha_foto': datetime.date(2023, 10, 1),
    'archivo': 'Archivo_A_20231001',
    'cartera': 'CARTERA_X', # Assuming _standardize_cartera keeps it
    'servicio': 'MOVIL',    # Assuming _standardize_servicio keeps it
    'cuentas': 1000,
    'clientes': 800,
    'deuda_asig': 100000.50,
    'deuda_act': 95000.20,
    'cuentas_gestionadas': 800, # Business rule might adjust this if cd+ci > gest
    'cuentas_cd': 100,
    'cuentas_ci': 50,
    'cuentas_sc': 650, # Business rule: gest - (cd+ci) = 800 - 150 = 650
    'cuentas_sg': 50,
    'cuentas_pdp': 20,
    'recupero': 5000.75,
    'pct_cober': 80.0, # Normalized to 0-100
    'pct_contac': 20.0,
    'pct_cd': 12.5,
    'pct_ci': 6.25,
    'pct_conversion': 40.0, # Input was 0.4
    'pct_efectividad': 18.75,
    'pct_cierre': 20.0,
    'inten': 1.5,
    'fecha_procesamiento': datetime.datetime(2023, 10, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
}


SAMPLE_EVOLUTION_RAW_DATA_BQ = [
    {
        "fecha_foto": "2023-10-01",
        "archivo": "Evo_A_20231001",
        "cartera": "CARTERA_X",
        "servicio": "MOVIL",
        "pct_cober": 0.75,
        "pct_contac": 0.15,
        "pct_efectividad": 0.25,
        "pct_cierre": 0.10,
        "recupero": 1200.00,
        "cuentas": 50,
        "fecha_procesamiento": "2023-10-01T11:00:00Z"
    },
    { # Record to be skipped
        "fecha_foto": None, # Missing PK
        "archivo": "Evo_B_20231002",
        "cartera": "CARTERA_Y",
        "servicio": "FIJA",
        "fecha_procesamiento": "2023-10-02T11:00:00Z"
    }
]

# Expected transformed data for the first valid record in SAMPLE_EVOLUTION_RAW_DATA_BQ
EXPECTED_EVOLUTION_TRANSFORMED_VALID_RECORD = {
    'fecha_foto': datetime.date(2023, 10, 1),
    'archivo': 'Evo_A_20231001',
    'cartera': 'CARTERA_X',
    'servicio': 'MOVIL',
    'pct_cober': 75.0,
    'pct_contac': 15.0,
    'pct_efectividad': 25.0,
    'pct_cierre': 10.0,
    'recupero': 1200.00,
    'cuentas': 50,
    'fecha_procesamiento': datetime.datetime(2023, 10, 1, 11, 0, 0, tzinfo=datetime.timezone.utc)
}

# Sample data for PostgresLoader tests
# This data should be what the transformer outputs
SAMPLE_DATA_FOR_LOADER = [
    {
        'id': 1, # Assuming a generic 'id' PK for a test model
        'name': 'Test Record 1',
        'value': 10.5,
        'updated_at': datetime.datetime(2023, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc),
        # 'created_at' will be handled by DB default or model default
    },
    {
        'id': 2,
        'name': 'Test Record 2',
        'value': 20.3,
        'updated_at': datetime.datetime(2023, 1, 2, 11, 0, 0, tzinfo=datetime.timezone.utc),
    }
]

# Need to import datetime for the above examples
import datetime
