-- ===============================================================================
-- 011-create-users-tables.sql
-- Migration: User Management System for Pulso-Back
-- ===============================================================================
-- step: Create users and roles tables with proper constraints and indexes
-- ===============================================================================

-- Create users table
CREATE TABLE public.users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    role VARCHAR(50) NOT NULL DEFAULT 'viewer',
    is_active BOOLEAN NOT NULL DEFAULT true,
    is_superuser BOOLEAN NOT NULL DEFAULT false,
    last_login TIMESTAMPTZ,
    login_count INTEGER NOT NULL DEFAULT 0,
    failed_login_attempts INTEGER NOT NULL DEFAULT 0,
    password_changed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMPTZ NULL,
    created_by UUID REFERENCES public.users(id),
    updated_by UUID REFERENCES public.users(id)
);

-- Create roles table for role definitions
CREATE TABLE public.roles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT,
    permissions JSONB NOT NULL DEFAULT '{}',
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create user sessions table for session management
CREATE TABLE public.user_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
    session_token VARCHAR(255) UNIQUE NOT NULL,
    ip_address INET,
    user_agent TEXT,
    expires_at TIMESTAMPTZ NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_accessed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create user audit log table for tracking changes
CREATE TABLE public.user_audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
    action VARCHAR(50) NOT NULL, -- CREATE, UPDATE, DELETE, LOGIN, LOGOUT, etc.
    entity_type VARCHAR(50) NOT NULL DEFAULT 'user',
    entity_id UUID,
    old_values JSONB,
    new_values JSONB,
    ip_address INET,
    user_agent TEXT,
    performed_by UUID REFERENCES public.users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ===============================================================================
-- INDEXES FOR PERFORMANCE
-- ===============================================================================

-- Users table indexes
CREATE INDEX idx_users_email ON public.users(email) WHERE deleted_at IS NULL;
CREATE INDEX idx_users_role ON public.users(role) WHERE deleted_at IS NULL;
CREATE INDEX idx_users_active ON public.users(is_active) WHERE deleted_at IS NULL;
CREATE INDEX idx_users_created_at ON public.users(created_at);
CREATE INDEX idx_users_deleted_at ON public.users(deleted_at) WHERE deleted_at IS NOT NULL;

-- Sessions table indexes
CREATE INDEX idx_user_sessions_user_id ON public.user_sessions(user_id);
CREATE INDEX idx_user_sessions_token ON public.user_sessions(session_token);
CREATE INDEX idx_user_sessions_expires_at ON public.user_sessions(expires_at);
CREATE INDEX idx_user_sessions_active ON public.user_sessions(is_active) WHERE is_active = true;

-- Audit log indexes
CREATE INDEX idx_user_audit_log_user_id ON public.user_audit_log(user_id);
CREATE INDEX idx_user_audit_log_action ON public.user_audit_log(action);
CREATE INDEX idx_user_audit_log_created_at ON public.user_audit_log(created_at);
CREATE INDEX idx_user_audit_log_entity ON public.user_audit_log(entity_type, entity_id);

-- ===============================================================================
-- TRIGGERS AND FUNCTIONS
-- ===============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger for users table
CREATE TRIGGER update_users_updated_at 
    BEFORE UPDATE ON public.users 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger for roles table
CREATE TRIGGER update_roles_updated_at 
    BEFORE UPDATE ON public.roles 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Function for audit logging
CREATE OR REPLACE FUNCTION log_user_changes()
RETURNS TRIGGER AS $$
BEGIN
    -- Only log changes to important fields
    IF TG_OP = 'INSERT' THEN
        INSERT INTO public.user_audit_log (
            user_id, action, entity_type, entity_id, new_values, performed_by
        ) VALUES (
            NEW.id, 'CREATE', 'user', NEW.id, 
            jsonb_build_object(
                'email', NEW.email,
                'first_name', NEW.first_name,
                'last_name', NEW.last_name,
                'role', NEW.role,
                'is_active', NEW.is_active
            ),
            NEW.created_by
        );
        RETURN NEW;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        -- Only log if important fields changed
        IF OLD.email != NEW.email OR 
           OLD.first_name != NEW.first_name OR 
           OLD.last_name != NEW.last_name OR 
           OLD.role != NEW.role OR 
           OLD.is_active != NEW.is_active OR
           OLD.deleted_at IS DISTINCT FROM NEW.deleted_at THEN
            
            INSERT INTO public.user_audit_log (
                user_id, action, entity_type, entity_id, old_values, new_values, performed_by
            ) VALUES (
                NEW.id, 
                CASE WHEN NEW.deleted_at IS NOT NULL AND OLD.deleted_at IS NULL THEN 'DELETE'
                     ELSE 'UPDATE' END,
                'user', NEW.id,
                jsonb_build_object(
                    'email', OLD.email,
                    'first_name', OLD.first_name,
                    'last_name', OLD.last_name,
                    'role', OLD.role,
                    'is_active', OLD.is_active
                ),
                jsonb_build_object(
                    'email', NEW.email,
                    'first_name', NEW.first_name,
                    'last_name', NEW.last_name,
                    'role', NEW.role,
                    'is_active', NEW.is_active
                ),
                NEW.updated_by
            );
        END IF;
        RETURN NEW;
    END IF;

    RETURN NULL;
END;
$$ language 'plpgsql';

-- Trigger for user audit logging
CREATE TRIGGER users_audit_trigger
    AFTER INSERT OR UPDATE ON public.users
    FOR EACH ROW
    EXECUTE FUNCTION log_user_changes();

-- ===============================================================================
-- DEFAULT ROLES DATA
-- ===============================================================================

-- Insert default roles
INSERT INTO public.roles (name, description, permissions) VALUES 
('superuser', 'Super Administrator with full system access', 
 '{"user": {"read": true, "create": true, "update": true, "delete": true}, 
   "dashboard": {"read": true}, 
   "reports": {"read": true, "export": true}, 
   "system": {"read": true, "configure": true}}'::jsonb),

('admin', 'Administrator with user management capabilities',
 '{"user": {"read": true, "create": true, "update": true, "delete": false}, 
   "dashboard": {"read": true}, 
   "reports": {"read": true, "export": true}}'::jsonb),

('manager', 'Manager with advanced dashboard and reporting access',
 '{"user": {"read": true, "create": false, "update": false, "delete": false}, 
   "dashboard": {"read": true}, 
   "reports": {"read": true, "export": true}}'::jsonb),

('analyst', 'Analyst with dashboard and basic reporting access',
 '{"user": {"read": false, "create": false, "update": false, "delete": false}, 
   "dashboard": {"read": true}, 
   "reports": {"read": true, "export": false}}'::jsonb),

('viewer', 'Basic user with read-only dashboard access',
 '{"user": {"read": false, "create": false, "update": false, "delete": false}, 
   "dashboard": {"read": true}, 
   "reports": {"read": false, "export": false}}'::jsonb);

-- ===============================================================================
-- VIEWS FOR EASIER QUERYING
-- ===============================================================================

-- View for active users with role information
CREATE VIEW public.active_users AS
SELECT 
    u.id,
    u.email,
    u.first_name,
    u.last_name,
    u.role,
    r.description as role_description,
    r.permissions,
    u.is_active,
    u.last_login,
    u.login_count,
    u.created_at,
    u.updated_at
FROM public.users u
LEFT JOIN public.roles r ON u.role = r.name
WHERE u.deleted_at IS NULL;

-- View for user statistics
CREATE VIEW public.user_statistics AS
SELECT 
    COUNT(*) as total_users,
    COUNT(*) FILTER (WHERE is_active = true) as active_users,
    COUNT(*) FILTER (WHERE is_active = false) as inactive_users,
    COUNT(*) FILTER (WHERE deleted_at IS NOT NULL) as deleted_users,
    COUNT(*) FILTER (WHERE last_login > CURRENT_TIMESTAMP - INTERVAL '30 days') as active_last_30_days,
    COUNT(*) FILTER (WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '30 days') as created_last_30_days
FROM public.users;

-- ===============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ===============================================================================

COMMENT ON TABLE public.users IS 'User accounts for the Pulso-Back application';
COMMENT ON COLUMN public.users.role IS 'User role: superuser, admin, manager, analyst, viewer';
COMMENT ON COLUMN public.users.deleted_at IS 'Soft delete timestamp - NULL means not deleted';
COMMENT ON COLUMN public.users.failed_login_attempts IS 'Counter for failed login attempts (for security)';

COMMENT ON TABLE public.roles IS 'Role definitions with permissions';
COMMENT ON COLUMN public.roles.permissions IS 'JSON object defining role permissions';

COMMENT ON TABLE public.user_sessions IS 'Active user sessions for session management';
COMMENT ON TABLE public.user_audit_log IS 'Audit trail for user-related actions';

COMMENT ON VIEW public.active_users IS 'Active users with role information (excludes soft-deleted users)';
COMMENT ON VIEW public.user_statistics IS 'User statistics for monitoring and reporting';

-- ===============================================================================
-- GRANT PERMISSIONS
-- ===============================================================================

-- Grant appropriate permissions to the application user
-- (These might need to be adjusted based on your specific database user setup)

-- Basic permissions for the application
GRANT SELECT, INSERT, UPDATE ON public.users TO postgres;
GRANT SELECT, INSERT, UPDATE ON public.roles TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.user_sessions TO postgres;
GRANT SELECT, INSERT ON public.user_audit_log TO postgres;

-- View permissions
GRANT SELECT ON public.active_users TO postgres;
GRANT SELECT ON public.user_statistics TO postgres;

-- Sequence permissions (if needed)
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO postgres;

-- ===============================================================================
-- VERIFICATION QUERIES
-- ===============================================================================

-- These queries can be used to verify the migration worked correctly

-- Check tables were created
SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE '%user%';

-- Check indexes were created
SELECT indexname, tablename FROM pg_indexes WHERE schemaname = 'public' AND tablename LIKE '%user%';

-- Check roles were inserted
SELECT name, description FROM public.roles ORDER BY name;

-- Check views were created
SELECT viewname FROM pg_views WHERE schemaname = 'public' AND viewname LIKE '%user%';

-- ===============================================================================
-- END MIGRATION
-- ===============================================================================
