-- Insert default permissions
-- Populates the 'permissions' table with a comprehensive set of system permissions.
-- These permissions are categorized by resource and action, and marked as 'is_system = TRUE'.

-- Begin transaction
BEGIN;

-- Define default permissions
-- Format: resource.action
-- These permissions cover common CRUD operations and specific application features.

INSERT INTO permissions (name, resource, action, description, is_system, is_active)
VALUES
    -- User Management
    ('user.create', 'user', 'create', 'Allows creating new users.', TRUE, TRUE),
    ('user.read', 'user', 'read', 'Allows viewing user profiles and information.', TRUE, TRUE),
    ('user.update', 'user', 'update', 'Allows editing user details (excluding roles/permissions).', TRUE, TRUE),
    ('user.delete', 'user', 'delete', 'Allows deleting users.', TRUE, TRUE),
    ('user.manage_roles', 'user', 'manage_roles', 'Allows assigning and revoking roles for users.', TRUE, TRUE),
    ('user.manage_status', 'user', 'manage_status', 'Allows activating, deactivating, or suspending user accounts.', TRUE, TRUE),
    ('user.list', 'user', 'list', 'Allows listing all users.', TRUE, TRUE),

    -- Role Management
    ('role.create', 'role', 'create', 'Allows creating new roles.', TRUE, TRUE),
    ('role.read', 'role', 'read', 'Allows viewing role details and assigned permissions.', TRUE, TRUE),
    ('role.update', 'role', 'update', 'Allows editing role details (name, description).', TRUE, TRUE),
    ('role.delete', 'role', 'delete', 'Allows deleting non-system roles.', TRUE, TRUE),
    ('role.manage_permissions', 'role', 'manage_permissions', 'Allows assigning and revoking permissions for roles.', TRUE, TRUE),
    ('role.list', 'role', 'list', 'Allows listing all roles.', TRUE, TRUE),

    -- Permission Management (typically for SuperAdmin only)
    ('permission.create', 'permission', 'create', 'Allows creating new permissions (rarely used, mostly system-defined).', TRUE, TRUE),
    ('permission.read', 'permission', 'read', 'Allows viewing permission details.', TRUE, TRUE),
    ('permission.update', 'permission', 'update', 'Allows editing permission details (rarely used).', TRUE, TRUE),
    ('permission.delete', 'permission', 'delete', 'Allows deleting non-system permissions (rarely used).', TRUE, TRUE),
    ('permission.list', 'permission', 'list', 'Allows listing all permissions.', TRUE, TRUE),

    -- Authentication & Session Management
    ('auth.manage_sessions', 'auth', 'manage_sessions', 'Allows viewing and revoking active user sessions/refresh tokens.', TRUE, TRUE),
    ('auth.impersonate', 'auth', 'impersonate', 'Allows logging in as another user (for support/troubleshooting).', TRUE, TRUE),

    -- Audit Log Management
    ('auditlog.read', 'auditlog', 'read', 'Allows viewing audit logs.', TRUE, TRUE),
    ('auditlog.export', 'auditlog', 'export', 'Allows exporting audit logs.', TRUE, TRUE),

    -- System Settings / Configuration
    ('system.read_settings', 'system', 'read_settings', 'Allows viewing system configuration settings.', TRUE, TRUE),
    ('system.update_settings', 'system', 'update_settings', 'Allows modifying system configuration settings.', TRUE, TRUE),

    -- Dashboard Access (Example - tailor to your application)
    ('dashboard.view_main', 'dashboard', 'view_main', 'Allows viewing the main dashboard.', TRUE, TRUE),
    ('dashboard.view_analytics', 'dashboard', 'view_analytics', 'Allows viewing the analytics dashboard.', TRUE, TRUE),

    -- Report Access (Example - tailor to your application)
    ('report.generate_user_activity', 'report', 'generate_user_activity', 'Allows generating user activity reports.', TRUE, TRUE),
    ('report.view_financials', 'report', 'view_financials', 'Allows viewing financial reports.', TRUE, TRUE)

ON CONFLICT (name) DO NOTHING; -- Avoid errors if permissions already exist

-- Commit transaction
COMMIT;

-- Comments for clarity
COMMENT ON TABLE permissions IS 'Stores granular permissions. Default permissions are critical for system operation.';
