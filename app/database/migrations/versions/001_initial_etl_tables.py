"""Create initial ETL tables for TimescaleDB

Revision ID: 001_initial_etl_tables
Revises: 
Create Date: 2025-06-28 10:10:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from typing import List

# revision identifiers, used by Alembic.
revision = '001_initial_etl_tables'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create all ETL tables optimized for TimescaleDB"""
    
    # =============================================================================
    # 1. ETL CONTROL TABLES (Create first - no dependencies)
    # =============================================================================
    
    # ETL Watermarks table
    op.create_table(
        'etl_watermarks',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('table_name', sa.String(length=100), nullable=False),
        sa.Column('last_extracted_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('last_extraction_status', sa.String(length=20), nullable=False, server_default='success'),
        sa.Column('records_extracted', sa.Integer(), default=0),
        sa.Column('extraction_duration_seconds', sa.Float(), default=0.0),
        sa.Column('error_message', sa.Text()),
        sa.Column('extraction_id', sa.String(length=50)),
        sa.Column('metadata', postgresql.JSONB()),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('table_name')
    )
    
    # Create indexes for ETL watermarks
    op.create_index('idx_etl_watermarks_table_name', 'etl_watermarks', ['table_name'])
    op.create_index('idx_etl_watermarks_status', 'etl_watermarks', ['last_extraction_status'])
    op.create_index('idx_etl_watermarks_updated', 'etl_watermarks', ['updated_at'])
    
    # ETL Execution Log table
    op.create_table(
        'etl_execution_log',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('execution_id', sa.String(length=50), nullable=False),
        sa.Column('table_name', sa.String(length=100), nullable=False),
        sa.Column('execution_type', sa.String(length=20), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=False),
        sa.Column('records_processed', sa.Integer(), default=0),
        sa.Column('records_inserted', sa.Integer(), default=0),
        sa.Column('records_updated', sa.Integer(), default=0),
        sa.Column('records_skipped', sa.Integer(), default=0),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('completed_at', sa.DateTime(timezone=True)),
        sa.Column('duration_seconds', sa.Float()),
        sa.Column('error_message', sa.Text()),
        sa.Column('stack_trace', sa.Text()),
        sa.Column('extraction_mode', sa.String(length=20)),
        sa.Column('source_query', sa.Text()),
        sa.Column('metadata', postgresql.JSONB()),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create indexes for execution log
    op.create_index('idx_etl_execution_log_execution_id', 'etl_execution_log', ['execution_id'])
    op.create_index('idx_etl_execution_log_table_name', 'etl_execution_log', ['table_name'])
    op.create_index('idx_etl_execution_log_status', 'etl_execution_log', ['status'])
    op.create_index('idx_etl_execution_log_started', 'etl_execution_log', ['started_at'])
    
    # =============================================================================
    # 2. MAIN DASHBOARD TABLES (TimescaleDB Hypertables)
    # =============================================================================
    
    # Dashboard Data table
    op.create_table(
        'dashboard_data',
        sa.Column('fecha_foto', sa.Date(), nullable=False, comment="Snapshot date"),
        sa.Column('archivo', sa.String(length=100), nullable=False, comment="Campaign file identifier"),
        sa.Column('cartera', sa.String(length=50), nullable=False, comment="Portfolio type"),
        sa.Column('servicio', sa.String(length=20), nullable=False, comment="Service type (MOVIL/FIJA)"),
        
        # Volume metrics
        sa.Column('cuentas', sa.Integer(), nullable=False, default=0, comment="Total accounts"),
        sa.Column('clientes', sa.Integer(), nullable=False, default=0, comment="Total clients"),
        sa.Column('deuda_asig', sa.Float(), nullable=False, default=0.0, comment="Assigned debt amount"),
        sa.Column('deuda_act', sa.Float(), nullable=False, default=0.0, comment="Current debt amount"),
        
        # Management metrics
        sa.Column('cuentas_gestionadas', sa.Integer(), nullable=False, default=0, comment="Managed accounts"),
        sa.Column('cuentas_cd', sa.Integer(), nullable=False, default=0, comment="Direct contact accounts"),
        sa.Column('cuentas_ci', sa.Integer(), nullable=False, default=0, comment="Indirect contact accounts"),
        sa.Column('cuentas_sc', sa.Integer(), nullable=False, default=0, comment="No contact accounts"),
        sa.Column('cuentas_sg', sa.Integer(), nullable=False, default=0, comment="No management accounts"),
        sa.Column('cuentas_pdp', sa.Integer(), nullable=False, default=0, comment="PDP accounts"),
        
        # Recovery metrics
        sa.Column('recupero', sa.Float(), nullable=False, default=0.0, comment="Amount recovered"),
        
        # Calculated KPIs (percentages)
        sa.Column('pct_cober', sa.Float(), nullable=False, default=0.0, comment="Coverage percentage"),
        sa.Column('pct_contac', sa.Float(), nullable=False, default=0.0, comment="Contact percentage"),
        sa.Column('pct_cd', sa.Float(), nullable=False, default=0.0, comment="Direct contact percentage"),
        sa.Column('pct_ci', sa.Float(), nullable=False, default=0.0, comment="Indirect contact percentage"),
        sa.Column('pct_conversion', sa.Float(), nullable=False, default=0.0, comment="PDP conversion percentage"),
        sa.Column('pct_efectividad', sa.Float(), nullable=False, default=0.0, comment="Effectiveness percentage"),
        sa.Column('pct_cierre', sa.Float(), nullable=False, default=0.0, comment="Closure percentage"),
        sa.Column('inten', sa.Float(), nullable=False, default=0.0, comment="Management intensity"),
        
        # Metadata
        sa.Column('fecha_procesamiento', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        
        sa.PrimaryKeyConstraint('fecha_foto', 'archivo', 'cartera', 'servicio'),
        sa.CheckConstraint('cuentas >= 0', name='chk_dashboard_cuentas_positive'),
        sa.CheckConstraint('deuda_asig >= 0', name='chk_dashboard_deuda_positive')
    )
    
    # Create indexes for dashboard_data
    op.create_index('idx_dashboard_data_fecha_foto', 'dashboard_data', ['fecha_foto'])
    op.create_index('idx_dashboard_data_cartera', 'dashboard_data', ['cartera'])
    op.create_index('idx_dashboard_data_servicio', 'dashboard_data', ['servicio'])
    op.create_index('idx_dashboard_data_procesamiento', 'dashboard_data', ['fecha_procesamiento'])
    
    # Evolution Data table
    op.create_table(
        'evolution_data',
        sa.Column('fecha_foto', sa.Date(), nullable=False, comment="Snapshot date"),
        sa.Column('archivo', sa.String(length=100), nullable=False, comment="Campaign file identifier"),
        sa.Column('cartera', sa.String(length=50), nullable=False, comment="Portfolio type"),
        sa.Column('servicio', sa.String(length=20), nullable=False, comment="Service type"),
        
        # Evolution metrics
        sa.Column('pct_cober', sa.Float(), nullable=False, default=0.0, comment="Coverage percentage"),
        sa.Column('pct_contac', sa.Float(), nullable=False, default=0.0, comment="Contact percentage"),
        sa.Column('pct_efectividad', sa.Float(), nullable=False, default=0.0, comment="Effectiveness percentage"),
        sa.Column('pct_cierre', sa.Float(), nullable=False, default=0.0, comment="Closure percentage"),
        sa.Column('recupero', sa.Float(), nullable=False, default=0.0, comment="Amount recovered"),
        sa.Column('cuentas', sa.Integer(), nullable=False, default=0, comment="Total accounts"),
        
        # Metadata
        sa.Column('fecha_procesamiento', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        
        sa.PrimaryKeyConstraint('fecha_foto', 'archivo')
    )
    
    # Create indexes for evolution_data
    op.create_index('idx_evolution_data_fecha_foto', 'evolution_data', ['fecha_foto'])
    op.create_index('idx_evolution_data_cartera', 'evolution_data', ['cartera'])
    op.create_index('idx_evolution_data_composite', 'evolution_data', ['fecha_foto', 'cartera'])
    
    # Assignment Data table
    op.create_table(
        'assignment_data',
        sa.Column('periodo', sa.String(length=7), nullable=False, comment="Period YYYY-MM"),
        sa.Column('archivo', sa.String(length=100), nullable=False, comment="Campaign file identifier"),
        sa.Column('cartera', sa.String(length=50), nullable=False, comment="Portfolio type"),
        
        # Volume metrics
        sa.Column('clientes', sa.Integer(), nullable=False, default=0, comment="Total clients"),
        sa.Column('cuentas', sa.Integer(), nullable=False, default=0, comment="Total accounts"),
        sa.Column('deuda_asig', sa.Float(), nullable=False, default=0.0, comment="Assigned debt"),
        sa.Column('deuda_actual', sa.Float(), nullable=False, default=0.0, comment="Current debt"),
        sa.Column('ticket_promedio', sa.Float(), nullable=False, default=0.0, comment="Average ticket"),
        
        # Metadata
        sa.Column('fecha_procesamiento', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        
        sa.PrimaryKeyConstraint('periodo', 'archivo', 'cartera')
    )
    
    # Create indexes for assignment_data
    op.create_index('idx_assignment_data_periodo', 'assignment_data', ['periodo'])
    op.create_index('idx_assignment_data_cartera', 'assignment_data', ['cartera'])
    
    # Operation Data table
    op.create_table(
        'operation_data',
        sa.Column('fecha_foto', sa.Date(), nullable=False, comment="Operation date"),
        sa.Column('hora', sa.Integer(), nullable=False, comment="Hour (0-23)"),
        sa.Column('canal', sa.String(length=20), nullable=False, comment="Channel (BOT/HUMANO)"),
        sa.Column('archivo', sa.String(length=100), nullable=False, server_default='GENERAL', comment="Campaign identifier"),
        
        # Operation metrics
        sa.Column('total_gestiones', sa.Integer(), nullable=False, default=0, comment="Total management actions"),
        sa.Column('contactos_efectivos', sa.Integer(), nullable=False, default=0, comment="Effective contacts"),
        sa.Column('contactos_no_efectivos', sa.Integer(), nullable=False, default=0, comment="Non-effective contacts"),
        sa.Column('total_pdp', sa.Integer(), nullable=False, default=0, comment="Payment promises"),
        sa.Column('tasa_contacto', sa.Float(), nullable=False, default=0.0, comment="Contact rate percentage"),
        sa.Column('tasa_conversion', sa.Float(), nullable=False, default=0.0, comment="PDP conversion rate"),
        
        # Metadata
        sa.Column('fecha_procesamiento', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        
        sa.PrimaryKeyConstraint('fecha_foto', 'hora', 'canal', 'archivo'),
        sa.CheckConstraint('hora >= 0 AND hora <= 23', name='chk_operation_hora_valid')
    )
    
    # Create indexes for operation_data
    op.create_index('idx_operation_data_fecha_foto', 'operation_data', ['fecha_foto'])
    op.create_index('idx_operation_data_hora', 'operation_data', ['hora'])
    op.create_index('idx_operation_data_canal', 'operation_data', ['canal'])
    op.create_index('idx_operation_data_composite', 'operation_data', ['fecha_foto', 'canal'])
    
    # Productivity Data table
    op.create_table(
        'productivity_data',
        sa.Column('fecha_foto', sa.Date(), nullable=False, comment="Performance date"),
        sa.Column('correo_agente', sa.String(length=100), nullable=False, comment="Agent email"),
        sa.Column('archivo', sa.String(length=100), nullable=False, server_default='GENERAL', comment="Campaign identifier"),
        
        # Performance metrics
        sa.Column('total_gestiones', sa.Integer(), nullable=False, default=0, comment="Total management actions"),
        sa.Column('contactos_efectivos', sa.Integer(), nullable=False, default=0, comment="Effective contacts"),
        sa.Column('total_pdp', sa.Integer(), nullable=False, default=0, comment="Payment promises"),
        sa.Column('peso_total', sa.Float(), nullable=False, default=0.0, comment="Total weight/score"),
        sa.Column('tasa_contacto', sa.Float(), nullable=False, default=0.0, comment="Contact rate"),
        sa.Column('tasa_conversion', sa.Float(), nullable=False, default=0.0, comment="PDP conversion rate"),
        sa.Column('score_productividad', sa.Float(), nullable=False, default=0.0, comment="Productivity score"),
        
        # Agent info (denormalized for performance)
        sa.Column('nombre_agente', sa.String(length=100), comment="Agent full name"),
        sa.Column('dni_agente', sa.String(length=20), comment="Agent DNI"),
        sa.Column('equipo', sa.String(length=50), comment="Team name"),
        
        # Metadata
        sa.Column('fecha_procesamiento', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        
        sa.PrimaryKeyConstraint('fecha_foto', 'correo_agente', 'archivo')
    )
    
    # Create indexes for productivity_data
    op.create_index('idx_productivity_data_fecha_foto', 'productivity_data', ['fecha_foto'])
    op.create_index('idx_productivity_data_agente', 'productivity_data', ['correo_agente'])
    op.create_index('idx_productivity_data_equipo', 'productivity_data', ['equipo'])
    op.create_index('idx_productivity_data_composite', 'productivity_data', ['fecha_foto', 'equipo'])
    
    # =============================================================================
    # 3. TIMESCALEDB HYPERTABLE CONFIGURATION
    # =============================================================================
    
    # Convert tables to TimescaleDB hypertables
    # Note: These will only execute if TimescaleDB extension is available
    
    _create_hypertable_if_timescaledb_available('dashboard_data', 'fecha_foto', '7 days')
    _create_hypertable_if_timescaledb_available('evolution_data', 'fecha_foto', '7 days')
    _create_hypertable_if_timescaledb_available('operation_data', 'fecha_foto', '1 day')
    _create_hypertable_if_timescaledb_available('productivity_data', 'fecha_foto', '7 days')
    _create_hypertable_if_timescaledb_available('etl_execution_log', 'started_at', '1 month')
    
    # Add retention policies
    _add_retention_policy_if_timescaledb_available('dashboard_data', '2 years')
    _add_retention_policy_if_timescaledb_available('evolution_data', '2 years')
    _add_retention_policy_if_timescaledb_available('operation_data', '1 year')
    _add_retention_policy_if_timescaledb_available('productivity_data', '2 years')
    _add_retention_policy_if_timescaledb_available('etl_execution_log', '6 months')


def downgrade() -> None:
    """Drop all ETL tables"""
    
    # Drop main tables (in reverse order)
    op.drop_table('productivity_data')
    op.drop_table('operation_data')
    op.drop_table('assignment_data')
    op.drop_table('evolution_data')
    op.drop_table('dashboard_data')
    
    # Drop control tables
    op.drop_table('etl_execution_log')
    op.drop_table('etl_watermarks')


def _create_hypertable_if_timescaledb_available(table_name: str, time_column: str, chunk_time_interval: str) -> None:
    """Create TimescaleDB hypertable if extension is available"""
    try:
        # Check if TimescaleDB extension is available
        op.execute("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")
        
        # Create hypertable
        op.execute(f"""
        SELECT create_hypertable(
            '{table_name}', 
            '{time_column}',
            chunk_time_interval => INTERVAL '{chunk_time_interval}',
            if_not_exists => TRUE
        )
        """)
        
        print(f"✅ Created TimescaleDB hypertable: {table_name}")
        
    except Exception as e:
        print(f"⚠️  TimescaleDB not available or hypertable creation failed for {table_name}: {str(e)}")
        print(f"📝 Table {table_name} created as regular PostgreSQL table")


def _add_retention_policy_if_timescaledb_available(table_name: str, retention_period: str) -> None:
    """Add retention policy if TimescaleDB is available"""
    try:
        # Check if TimescaleDB extension is available
        op.execute("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")
        
        # Add retention policy
        op.execute(f"""
        SELECT add_retention_policy(
            '{table_name}', 
            INTERVAL '{retention_period}',
            if_not_exists => TRUE
        )
        """)
        
        print(f"✅ Added retention policy to {table_name}: {retention_period}")
        
    except Exception as e:
        print(f"⚠️  Retention policy creation failed for {table_name}: {str(e)}")
