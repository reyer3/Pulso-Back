-- Assign default permissions to roles
-- Populates the 'role_permissions' table to link predefined roles with their respective permissions.
-- This script assumes that default roles and permissions have already been inserted by previous migrations.

-- Begin transaction
BEGIN;

-- Helper function to get role_id by name
CREATE OR REPLACE FUNCTION get_role_id_by_name(role_name_param VARCHAR(50))
RETURNS UUID AS $$
DECLARE
    role_uuid UUID;
BEGIN
    SELECT id INTO role_uuid FROM roles WHERE name = role_name_param;
    IF role_uuid IS NULL THEN
        RAISE EXCEPTION 'Role with name % not found.', role_name_param;
    END IF;
    RETURN role_uuid;
END;
$$ LANGUAGE plpgsql;

-- Helper function to get permission_id by name
CREATE OR REPLACE FUNCTION get_permission_id_by_name(permission_name_param VARCHAR(100))
RETURNS UUID AS $$
DECLARE
    permission_uuid UUID;
BEGIN
    SELECT id INTO permission_uuid FROM permissions WHERE name = permission_name_param;
    IF permission_uuid IS NULL THEN
        RAISE EXCEPTION 'Permission with name % not found.', permission_name_param;
    END IF;
    RETURN permission_uuid;
END;
$$ LANGUAGE plpgsql;

-- Superadmin: Assign all permissions
-- This is a simplified approach. In a real scenario, you might explicitly list them or use a wildcard if supported.
-- For this example, we will insert all known permissions.
INSERT INTO role_permissions (role_id, permission_id)
SELECT get_role_id_by_name('superadmin'), p.id
FROM permissions p
ON CONFLICT (role_id, permission_id) DO NOTHING;

-- Admin: Assign a broad set of permissions, excluding highly sensitive superadmin tasks
INSERT INTO role_permissions (role_id, permission_id)
SELECT get_role_id_by_name('admin'), get_permission_id_by_name(p_name)
FROM (VALUES
    ('user.create'), ('user.read'), ('user.update'), ('user.delete'),
    ('user.manage_roles'), ('user.manage_status'), ('user.list'),
    ('role.create'), ('role.read'), ('role.update'), ('role.delete'),
    ('role.manage_permissions'), ('role.list'),
    ('permission.read'), ('permission.list'), -- Admins can see permissions but not usually manage them
    ('auth.manage_sessions'),
    ('auditlog.read'), ('auditlog.export'),
    ('system.read_settings'), ('system.update_settings'),
    ('dashboard.view_main'), ('dashboard.view_analytics'),
    ('report.generate_user_activity'), ('report.view_financials')
) AS perms(p_name)
ON CONFLICT (role_id, permission_id) DO NOTHING;

-- Standard User: Assign basic application access permissions
INSERT INTO role_permissions (role_id, permission_id)
SELECT get_role_id_by_name('user'), get_permission_id_by_name(p_name)
FROM (VALUES
    ('user.read'), -- Typically can read their own profile
    ('dashboard.view_main')
    -- Add other basic permissions relevant to a standard user
) AS perms(p_name)
ON CONFLICT (role_id, permission_id) DO NOTHING;
-- Note: 'user.read' for a standard user might be restricted by application logic to only their own data.

-- Auditor: Assign read-only access to relevant areas
INSERT INTO role_permissions (role_id, permission_id)
SELECT get_role_id_by_name('auditor'), get_permission_id_by_name(p_name)
FROM (VALUES
    ('user.read'), ('user.list'),
    ('role.read'), ('role.list'),
    ('permission.read'), ('permission.list'),
    ('auditlog.read'), ('auditlog.export'),
    ('system.read_settings')
) AS perms(p_name)
ON CONFLICT (role_id, permission_id) DO NOTHING;


-- Clean up helper functions if they are not needed elsewhere
DROP FUNCTION IF EXISTS get_role_id_by_name(VARCHAR(50));
DROP FUNCTION IF EXISTS get_permission_id_by_name(VARCHAR(100));

-- Commit transaction
COMMIT;

-- Comments for clarity
COMMENT ON TABLE role_permissions IS 'Association table linking roles to permissions. This migration populates it with default assignments.';
