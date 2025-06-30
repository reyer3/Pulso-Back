import pytest
from datetime import datetime
from app.etl.config import ETLConfig, ExtractionConfig, TableType, ExtractionMode

# Mock PROJECT_UID directly in the class for consistent testing if not already set
ETLConfig.PROJECT_UID = "P3fV4dWNeMkN5RJMhV8e"

def test_get_config_valid():
    """Test retrieving a valid table configuration."""
    config = ETLConfig.get_config("calendario")
    assert isinstance(config, ExtractionConfig)
    assert config.table_name == "calendario"
    assert config.table_type == TableType.RAW

def test_get_config_invalid():
    """Test retrieving a non-existent table configuration."""
    with pytest.raises(ValueError, match="No configuration found for table: non_existent_table"):
        ETLConfig.get_config("non_existent_table")

def test_get_query_template_valid():
    """Test retrieving a valid query template."""
    template = ETLConfig.get_query_template("asignaciones")
    assert isinstance(template, str)
    assert "SELECT" in template.upper()
    assert "batch_P3fV4dWNeMkN5RJMhV8e_asignacion" in template # Source table

def test_get_query_template_invalid():
    """Test retrieving a non-existent query template."""
    with pytest.raises(ValueError, match="No query template found for table: non_existent_template"):
        ETLConfig.get_query_template("non_existent_template")

def test_get_query_full_refresh():
    """Test get_query for a full refresh (since_date is None)."""
    query = ETLConfig.get_query("calendario", since_date=None)
    assert "WHERE 1=1" in query # Default filter for full refresh
    assert "{incremental_filter}" not in query

def test_get_query_incremental():
    """Test get_query for an incremental refresh."""
    since = datetime(2023, 1, 1)
    query = ETLConfig.get_query("calendario", since_date=since)
    # Check if the incremental filter placeholder is replaced
    assert "{incremental_filter}" not in query
    assert "fecha_apertura >= '2022-12-25'" in query # Based on 7-day lookback for calendario

def test_get_query_new_tables_formatting():
    """Test query formatting for new tables requiring project_id and dataset_id."""
    query_vb = ETLConfig.get_query("voicebot_gestiones", since_date=datetime(2023,1,1))
    assert f"FROM `{ETLConfig.PROJECT_ID}.{ETLConfig.DATASET}.sync_voicebot_batch`" in query_vb

    query_ma = ETLConfig.get_query("mibotair_gestiones", since_date=datetime(2023,1,1))
    assert f"FROM `{ETLConfig.PROJECT_ID}.{ETLConfig.DATASET}.sync_mibotair_batch`" in query_ma

def test_get_incremental_filter():
    """Test various scenarios for get_incremental_filter."""
    since = datetime(2023, 1, 15)
    # Calendario (lookback 7 days) -> 2023-01-08
    filter_cal = ETLConfig.get_incremental_filter("calendario", since)
    assert "fecha_apertura >= '2023-01-08'" in filter_cal

    # Asignaciones (lookback 30 days) -> 2022-12-16
    filter_asig = ETLConfig.get_incremental_filter("asignaciones", since)
    assert "DATE(creado_el) >= '2022-12-16'" in filter_asig

    # Voicebot Gestiones (lookback 7 days, incremental_column 'date')
    filter_vb = ETLConfig.get_incremental_filter("voicebot_gestiones", since)
    assert "DATE(date) >= '2023-01-08'" in filter_vb # date is the incremental_column

    # Dimension table (no incremental column, should be 1=1)
    filter_homo = ETLConfig.get_incremental_filter("homologacion_mibotair", since)
    assert filter_homo == "1=1"

def test_list_tables():
    """Test listing all configured table (base) names."""
    tables = ETLConfig.list_tables()
    assert isinstance(tables, list)
    assert "calendario" in tables
    assert "voicebot_gestiones" in tables
    assert "raw_calendario" not in tables # Should be base names

def test_get_raw_source_tables():
    """Test getting raw source table names."""
    raw_tables = ETLConfig.get_raw_source_tables()
    assert "calendario" in raw_tables
    assert "asignaciones" in raw_tables
    assert "voicebot_gestiones" in raw_tables
    assert "homologacion_mibotair" in raw_tables # Dimensions are included
    # Ensure no mart or aux tables are listed
    assert "dashboard_data" not in raw_tables
    assert "gestiones_unificadas" not in ETLConfig.EXTRACTION_CONFIGS # Verify old name removed


# --- Tests for get_fq_table_name ---
@pytest.mark.parametrize("base_name, table_type, expected_fqn", [
    ("calendario", TableType.RAW, f"raw_{ETLConfig.PROJECT_UID}.calendario"),
    ("asignaciones", TableType.RAW, f"raw_{ETLConfig.PROJECT_UID}.asignaciones"),
    ("voicebot_gestiones", TableType.RAW, f"raw_{ETLConfig.PROJECT_UID}.voicebot_gestiones"),
    ("homologacion_mibotair", TableType.DIMENSION, f"raw_{ETLConfig.PROJECT_UID}.homologacion_mibotair"), # Dimension in raw
    ("gestiones_unificadas", TableType.AUX, f"aux_{ETLConfig.PROJECT_UID}.gestiones_unificadas"),
    ("cuenta_campana_state", TableType.AUX, f"aux_{ETLConfig.PROJECT_UID}.cuenta_campana_state"),
    ("dashboard_data", TableType.MART, f"mart_{ETLConfig.PROJECT_UID}.dashboard_data"),
    ("evolution_data", TableType.EVOLUTION, f"mart_{ETLConfig.PROJECT_UID}.evolution_data"), # Evolution is a mart
    ("etl_watermarks", TableType.RAW, "public.etl_watermarks"), # RAW type for global table
    ("etl_watermarks", TableType.AUX, "public.etl_watermarks"), # AUX type for global table
    ("etl_execution_log", TableType.MART, "public.etl_execution_log"), # MART type for global table
])
def test_get_fq_table_name_project_specific(base_name, table_type, expected_fqn):
    """Test FQN construction for project-specific tables."""
    assert ETLConfig.get_fq_table_name(base_name, table_type) == expected_fqn

def test_get_fq_table_name_public_tables():
    """Test FQN for explicitly public tables, regardless of passed TableType."""
    assert ETLConfig.get_fq_table_name("etl_watermarks", TableType.RAW) == "public.etl_watermarks"
    assert ETLConfig.get_fq_table_name("etl_watermarks", TableType.MART) == "public.etl_watermarks"
    assert ETLConfig.get_fq_table_name("etl_execution_log", TableType.AUX) == "public.etl_execution_log"
    assert ETLConfig.get_fq_table_name("extraction_metrics", TableType.RAW) == "public.extraction_metrics"

def test_get_fq_table_name_unmapped_type():
    """Test FQN construction with an unmapped TableType."""
    class UnmappedTableType(str, Enum):
        WEIRD_TYPE = "weird_type"

    with pytest.raises(ValueError, match="Unknown table type 'weird_type' for FQN construction of 'some_table'."):
        ETLConfig.get_fq_table_name("some_table", UnmappedTableType.WEIRD_TYPE)

def test_extraction_config_base_names():
    """Ensure all keys in EXTRACTION_CONFIGS are base names and match config.table_name."""
    for key, config in ETLConfig.EXTRACTION_CONFIGS.items():
        assert "raw_" not in key, f"Key '{key}' should be a base name."
        assert "aux_" not in key, f"Key '{key}' should be a base name."
        assert "mart_" not in key, f"Key '{key}' should be a base name."
        assert key == config.table_name, f"Key '{key}' must match config.table_name '{config.table_name}'."

def test_query_templates_base_names():
    """Ensure all keys in EXTRACTION_QUERY_TEMPLATES are base names."""
    for key in ETLConfig.EXTRACTION_QUERY_TEMPLATES.keys():
        assert "raw_" not in key, f"Key '{key}' should be a base name."
        assert "aux_" not in key, f"Key '{key}' should be a base name."
        assert "mart_" not in key, f"Key '{key}' should be a base name."

# Example of a table that used to be TableType.DASHBOARD, now RAW
def test_calendario_config_type():
    config = ETLConfig.get_config("calendario")
    assert config.table_type == TableType.RAW

# Check a dimension table config
def test_homologacion_config_type():
    config = ETLConfig.get_config("homologacion_mibotair")
    assert config.table_type == TableType.DIMENSION
    assert config.default_mode == ExtractionMode.FULL_REFRESH

# Check a new raw table config
def test_voicebot_gestiones_config():
    config = ETLConfig.get_config("voicebot_gestiones")
    assert config.table_name == "voicebot_gestiones"
    assert config.table_type == TableType.RAW
    assert config.source_table == "sync_voicebot_batch"
    assert config.incremental_column == "date"

# Verify 'gestiones_unificadas' is truly removed
def test_gestiones_unificadas_removed():
    with pytest.raises(ValueError):
        ETLConfig.get_config("gestiones_unificadas")
    assert "gestiones_unificadas" not in ETLConfig.EXTRACTION_QUERY_TEMPLATES
    assert "gestiones_unificadas" not in ETLConfig.list_tables()

# Ensure PROJECT_UID is set
def test_project_uid_set():
    assert hasattr(ETLConfig, 'PROJECT_UID')
    assert ETLConfig.PROJECT_UID == "P3fV4dWNeMkN5RJMhV8e"

# Test TableType enum members
def test_table_type_enum():
    assert TableType.RAW == "raw"
    assert TableType.AUX == "aux"
    assert TableType.MART == "mart"
    assert TableType.DIMENSION == "dimension"
    # ... add others if needed
