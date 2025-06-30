-- Insert default roles
-- Populates the 'roles' table with essential system roles.
-- These roles are marked as 'is_system = TRUE' to protect them from accidental deletion.

-- Begin transaction
BEGIN;

-- Define default roles
-- Note: UUIDs are hardcoded to ensure consistency across environments if needed for direct reference.
-- Alternatively, use `gen_random_uuid()` if specific UUIDs are not required for system logic.

INSERT INTO roles (id, name, display_name, description, is_system, is_active)
VALUES
    (gen_random_uuid(), 'superadmin', 'Super Administrator', 'Has all permissions and manages the entire system. Highest level of access.', TRUE, TRUE),
    (gen_random_uuid(), 'admin', 'Administrator', 'Manages users, roles, and system settings. High level of access, but less than Super Admin.', TRUE, TRUE),
    (gen_random_uuid(), 'user', 'Standard User', 'Regular user with basic access to application features. Limited permissions.', TRUE, TRUE),
    (gen_random_uuid(), 'auditor', 'Auditor', 'Read-only access to logs and system configurations for auditing purposes.', TRUE, TRUE)
ON CONFLICT (name) DO NOTHING; -- Avoid errors if roles already exist

-- Commit transaction
COMMIT;

-- Comments for clarity
COMMENT ON TABLE roles IS 'Stores user roles for role-based access control (RBAC). Default roles are critical for system operation.';
