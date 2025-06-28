"""
🎯 Initial migration for Pulso ETL Tables
Creates all base tables and TimescaleDB hypertables

Revision ID: ${up_revision}
Revises: ${down_revision}
Create Date: ${create_date}
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}


def upgrade() -> None:
    # Create ETL watermarks table
    op.create_table('etl_watermarks',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('table_name', sa.String(length=100), nullable=False, comment='Target table name'),
        sa.Column('last_extracted_at', sa.DateTime(timezone=True), nullable=False, comment='Last extraction timestamp'),
        sa.Column('last_extraction_status', sa.String(length=20), nullable=False, server_default='success', comment='Last extraction status'),
        sa.Column('records_extracted', sa.Integer(), server_default='0', comment='Records in last extraction'),
        sa.Column('extraction_duration_seconds', sa.Float(), server_default='0.0', comment='Last extraction duration'),
        sa.Column('error_message', sa.Text(), comment='Error message if failed'),
        sa.Column('extraction_id', sa.String(length=50), comment='Unique extraction identifier'),
        sa.Column('metadata', postgresql.JSONB(astext_type=sa.Text()), comment='Additional extraction metadata'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()')),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()')),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('table_name'),
        comment='ETL watermark tracking for incremental extractions'
    )
    
    # Indexes for etl_watermarks
    op.create_index('idx_etl_watermarks_table_name', 'etl_watermarks', ['table_name'])
    op.create_index('idx_etl_watermarks_status', 'etl_watermarks', ['last_extraction_status'])
    op.create_index('idx_etl_watermarks_updated', 'etl_watermarks', ['updated_at'])

    # Create ETL execution log table
    op.create_table('etl_execution_log',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('execution_id', sa.String(length=50), nullable=False, comment='Unique execution identifier'),
        sa.Column('table_name', sa.String(length=100), nullable=False, comment='Target table name'),
        sa.Column('execution_type', sa.String(length=20), nullable=False, comment='incremental/full_refresh/manual'),
        sa.Column('status', sa.String(length=20), nullable=False, comment='running/success/failed'),
        sa.Column('records_processed', sa.Integer(), server_default='0', comment='Total records processed'),
        sa.Column('records_inserted', sa.Integer(), server_default='0', comment='Records inserted'),
        sa.Column('records_updated', sa.Integer(), server_default='0', comment='Records updated'),
        sa.Column('records_skipped', sa.Integer(), server_default='0', comment='Records skipped'),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=False, comment='Execution start time'),
        sa.Column('completed_at', sa.DateTime(timezone=True), comment='Execution completion time'),
        sa.Column('duration_seconds', sa.Float(), comment='Total execution duration'),
        sa.Column('error_message', sa.Text(), comment='Error message if failed'),
        sa.Column('stack_trace', sa.Text(), comment='Full stack trace if failed'),
        sa.Column('extraction_mode', sa.String(length=20), comment='Extraction mode used'),
        sa.Column('source_query', sa.Text(), comment='SQL query executed on source'),
        sa.Column('metadata', postgresql.JSONB(astext_type=sa.Text()), comment='Additional execution metadata'),
        sa.PrimaryKeyConstraint('id'),
        comment='Detailed ETL execution log for monitoring and debugging'
    )
    
    # Indexes for etl_execution_log
    op.create_index('idx_etl_execution_log_execution_id', 'etl_execution_log', ['execution_id'])
    op.create_index('idx_etl_execution_log_table_name', 'etl_execution_log', ['table_name'])
    op.create_index('idx_etl_execution_log_status', 'etl_execution_log', ['status'])
    op.create_index('idx_etl_execution_log_started', 'etl_execution_log', ['started_at'])

    # Create dashboard_data table
    op.create_table('dashboard_data',
        sa.Column('fecha_foto', sa.Date(), nullable=False, comment='Snapshot date'),
        sa.Column('archivo', sa.String(length=100), nullable=False, comment='Campaign file identifier'),
        sa.Column('cartera', sa.String(length=50), nullable=False, comment='Portfolio type'),
        sa.Column('servicio', sa.String(length=20), nullable=False, comment='Service type (MOVIL/FIJA)'),
        sa.Column('cuentas', sa.Integer(), nullable=False, server_default='0', comment='Total accounts'),
        sa.Column('clientes', sa.Integer(), nullable=False, server_default='0', comment='Total clients'),
        sa.Column('deuda_asig', sa.Float(), nullable=False, server_default='0.0', comment='Assigned debt amount'),
        sa.Column('deuda_act', sa.Float(), nullable=False, server_default='0.0', comment='Current debt amount'),
        sa.Column('cuentas_gestionadas', sa.Integer(), nullable=False, server_default='0', comment='Managed accounts'),
        sa.Column('cuentas_cd', sa.Integer(), nullable=False, server_default='0', comment='Direct contact accounts'),
        sa.Column('cuentas_ci', sa.Integer(), nullable=False, server_default='0', comment='Indirect contact accounts'),
        sa.Column('cuentas_sc', sa.Integer(), nullable=False, server_default='0', comment='No contact accounts'),
        sa.Column('cuentas_sg', sa.Integer(), nullable=False, server_default='0', comment='No management accounts'),
        sa.Column('cuentas_pdp', sa.Integer(), nullable=False, server_default='0', comment='PDP accounts'),
        sa.Column('recupero', sa.Float(), nullable=False, server_default='0.0', comment='Amount recovered'),
        sa.Column('pct_cober', sa.Float(), nullable=False, server_default='0.0', comment='Coverage percentage'),
        sa.Column('pct_contac', sa.Float(), nullable=False, server_default='0.0', comment='Contact percentage'),
        sa.Column('pct_cd', sa.Float(), nullable=False, server_default='0.0', comment='Direct contact percentage'),
        sa.Column('pct_ci', sa.Float(), nullable=False, server_default='0.0', comment='Indirect contact percentage'),
        sa.Column('pct_conversion', sa.Float(), nullable=False, server_default='0.0', comment='PDP conversion percentage'),
        sa.Column('pct_efectividad', sa.Float(), nullable=False, server_default='0.0', comment='Effectiveness percentage'),
        sa.Column('pct_cierre', sa.Float(), nullable=False, server_default='0.0', comment='Closure percentage'),
        sa.Column('inten', sa.Float(), nullable=False, server_default='0.0', comment='Management intensity'),
        sa.Column('fecha_procesamiento', sa.DateTime(timezone=True), server_default=sa.text('now()'), comment='Processing timestamp'),
        sa.CheckConstraint('cuentas >= 0', name='chk_dashboard_cuentas_positive'),
        sa.CheckConstraint('deuda_asig >= 0', name='chk_dashboard_deuda_positive'),
        sa.PrimaryKeyConstraint('fecha_foto', 'archivo', 'cartera', 'servicio'),
        comment='Main dashboard metrics by date, campaign, portfolio and service'
    )
    
    # Indexes for dashboard_data
    op.create_index('idx_dashboard_data_fecha_foto', 'dashboard_data', ['fecha_foto'])
    op.create_index('idx_dashboard_data_cartera', 'dashboard_data', ['cartera'])
    op.create_index('idx_dashboard_data_servicio', 'dashboard_data', ['servicio'])
    op.create_index('idx_dashboard_data_procesamiento', 'dashboard_data', ['fecha_procesamiento'])

    # Create evolution_data table
    op.create_table('evolution_data',
        sa.Column('fecha_foto', sa.Date(), nullable=False, comment='Snapshot date'),
        sa.Column('archivo', sa.String(length=100), nullable=False, comment='Campaign file identifier'),
        sa.Column('cartera', sa.String(length=50), nullable=False, comment='Portfolio type'),
        sa.Column('servicio', sa.String(length=20), nullable=False, comment='Service type'),
        sa.Column('pct_cober', sa.Float(), nullable=False, server_default='0.0', comment='Coverage percentage'),
        sa.Column('pct_contac', sa.Float(), nullable=False, server_default='0.0', comment='Contact percentage'),
        sa.Column('pct_efectividad', sa.Float(), nullable=False, server_default='0.0', comment='Effectiveness percentage'),
        sa.Column('pct_cierre', sa.Float(), nullable=False, server_default='0.0', comment='Closure percentage'),
        sa.Column('recupero', sa.Float(), nullable=False, server_default='0.0', comment='Amount recovered'),
        sa.Column('cuentas', sa.Integer(), nullable=False, server_default='0', comment='Total accounts'),
        sa.Column('fecha_procesamiento', sa.DateTime(timezone=True), server_default=sa.text('now()')),
        sa.PrimaryKeyConstraint('fecha_foto', 'archivo'),
        comment='Time series data for evolution analysis'
    )
    
    # Indexes for evolution_data
    op.create_index('idx_evolution_data_fecha_foto', 'evolution_data', ['fecha_foto'])
    op.create_index('idx_evolution_data_cartera', 'evolution_data', ['cartera'])
    op.create_index('idx_evolution_data_composite', 'evolution_data', ['fecha_foto', 'cartera'])

    # Create assignment_data table
    op.create_table('assignment_data',
        sa.Column('periodo', sa.String(length=7), nullable=False, comment='Period YYYY-MM'),
        sa.Column('archivo', sa.String(length=100), nullable=False, comment='Campaign file identifier'),
        sa.Column('cartera', sa.String(length=50), nullable=False, comment='Portfolio type'),
        sa.Column('clientes', sa.Integer(), nullable=False, server_default='0', comment='Total clients'),
        sa.Column('cuentas', sa.Integer(), nullable=False, server_default='0', comment='Total accounts'),
        sa.Column('deuda_asig', sa.Float(), nullable=False, server_default='0.0', comment='Assigned debt'),
        sa.Column('deuda_actual', sa.Float(), nullable=False, server_default='0.0', comment='Current debt'),
        sa.Column('ticket_promedio', sa.Float(), nullable=False, server_default='0.0', comment='Average ticket'),
        sa.Column('fecha_procesamiento', sa.DateTime(timezone=True), server_default=sa.text('now()')),
        sa.PrimaryKeyConstraint('periodo', 'archivo', 'cartera'),
        comment='Assignment composition data by period and portfolio'
    )
    
    # Indexes for assignment_data
    op.create_index('idx_assignment_data_periodo', 'assignment_data', ['periodo'])
    op.create_index('idx_assignment_data_cartera', 'assignment_data', ['cartera'])

    # Create operation_data table
    op.create_table('operation_data',
        sa.Column('fecha_foto', sa.Date(), nullable=False, comment='Operation date'),
        sa.Column('hora', sa.Integer(), nullable=False, comment='Hour (0-23)'),
        sa.Column('canal', sa.String(length=20), nullable=False, comment='Channel (BOT/HUMANO)'),
        sa.Column('archivo', sa.String(length=100), nullable=False, server_default='GENERAL', comment='Campaign identifier'),
        sa.Column('total_gestiones', sa.Integer(), nullable=False, server_default='0', comment='Total management actions'),
        sa.Column('contactos_efectivos', sa.Integer(), nullable=False, server_default='0', comment='Effective contacts'),
        sa.Column('contactos_no_efectivos', sa.Integer(), nullable=False, server_default='0', comment='Non-effective contacts'),
        sa.Column('total_pdp', sa.Integer(), nullable=False, server_default='0', comment='Payment promises'),
        sa.Column('tasa_contacto', sa.Float(), nullable=False, server_default='0.0', comment='Contact rate percentage'),
        sa.Column('tasa_conversion', sa.Float(), nullable=False, server_default='0.0', comment='PDP conversion rate'),
        sa.Column('fecha_procesamiento', sa.DateTime(timezone=True), server_default=sa.text('now()')),
        sa.CheckConstraint('hora >= 0 AND hora <= 23', name='chk_operation_hora_valid'),
        sa.PrimaryKeyConstraint('fecha_foto', 'hora', 'canal', 'archivo'),
        comment='Hourly operational metrics by channel'
    )
    
    # Indexes for operation_data
    op.create_index('idx_operation_data_fecha_foto', 'operation_data', ['fecha_foto'])
    op.create_index('idx_operation_data_hora', 'operation_data', ['hora'])
    op.create_index('idx_operation_data_canal', 'operation_data', ['canal'])
    op.create_index('idx_operation_data_composite', 'operation_data', ['fecha_foto', 'canal'])

    # Create productivity_data table
    op.create_table('productivity_data',
        sa.Column('fecha_foto', sa.Date(), nullable=False, comment='Performance date'),
        sa.Column('correo_agente', sa.String(length=100), nullable=False, comment='Agent email'),
        sa.Column('archivo', sa.String(length=100), nullable=False, server_default='GENERAL', comment='Campaign identifier'),
        sa.Column('total_gestiones', sa.Integer(), nullable=False, server_default='0', comment='Total management actions'),
        sa.Column('contactos_efectivos', sa.Integer(), nullable=False, server_default='0', comment='Effective contacts'),
        sa.Column('total_pdp', sa.Integer(), nullable=False, server_default='0', comment='Payment promises'),
        sa.Column('peso_total', sa.Float(), nullable=False, server_default='0.0', comment='Total weight/score'),
        sa.Column('tasa_contacto', sa.Float(), nullable=False, server_default='0.0', comment='Contact rate'),
        sa.Column('tasa_conversion', sa.Float(), nullable=False, server_default='0.0', comment='PDP conversion rate'),
        sa.Column('score_productividad', sa.Float(), nullable=False, server_default='0.0', comment='Productivity score'),
        sa.Column('nombre_agente', sa.String(length=100), comment='Agent full name'),
        sa.Column('dni_agente', sa.String(length=20), comment='Agent DNI'),
        sa.Column('equipo', sa.String(length=50), comment='Team name'),
        sa.Column('fecha_procesamiento', sa.DateTime(timezone=True), server_default=sa.text('now()')),
        sa.PrimaryKeyConstraint('fecha_foto', 'correo_agente', 'archivo'),
        comment='Daily productivity metrics by agent'
    )
    
    # Indexes for productivity_data
    op.create_index('idx_productivity_data_fecha_foto', 'productivity_data', ['fecha_foto'])
    op.create_index('idx_productivity_data_agente', 'productivity_data', ['correo_agente'])
    op.create_index('idx_productivity_data_equipo', 'productivity_data', ['equipo'])
    op.create_index('idx_productivity_data_composite', 'productivity_data', ['fecha_foto', 'equipo'])


def downgrade() -> None:
    # Drop tables in reverse order to respect foreign key constraints
    op.drop_table('productivity_data')
    op.drop_table('operation_data')
    op.drop_table('assignment_data')
    op.drop_table('evolution_data')
    op.drop_table('dashboard_data')
    op.drop_table('etl_execution_log')
    op.drop_table('etl_watermarks')
