-- Create permissions table
-- Stores granular permissions for actions on resources (e.g., 'user.read', 'dashboard.edit').
-- Helps in defining precise access control for different roles.

CREATE TABLE IF NOT EXISTS permissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) UNIQUE NOT NULL, -- e.g., 'user.read', 'dashboard.edit'
    resource VARCHAR(50) NOT NULL,     -- e.g., 'user', 'dashboard'
    action VARCHAR(50) NOT NULL,      -- e.g., 'read', 'write', 'delete', 'edit'
    description TEXT,
    is_system BOOLEAN DEFAULT FALSE, -- Prevents deletion of critical permissions
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_permissions_name_active ON permissions (name, is_active);
CREATE INDEX IF NOT EXISTS idx_permissions_resource_action ON permissions (resource, action);

-- Trigger to update 'updated_at' timestamp on any row modification
CREATE OR REPLACE FUNCTION update_permissions_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_permissions_updated_at
BEFORE UPDATE ON permissions
FOR EACH ROW
EXECUTE FUNCTION update_permissions_updated_at_column();

-- Comments for clarity
COMMENT ON TABLE permissions IS 'Stores granular permissions for actions on resources (e.g., user.read).';
COMMENT ON COLUMN permissions.id IS 'Unique identifier for the permission.';
COMMENT ON COLUMN permissions.name IS 'Unique name for the permission (e.g., user.read, dashboard.edit).';
COMMENT ON COLUMN permissions.resource IS 'The resource this permission applies to (e.g., user, dashboard).';
COMMENT ON COLUMN permissions.action IS 'The action allowed by this permission (e.g., read, write, delete).';
COMMENT ON COLUMN permissions.description IS 'Detailed description of what this permission allows.';
COMMENT ON COLUMN permissions.is_system IS 'Flag to indicate if the permission is system-critical (prevents deletion).';
COMMENT ON COLUMN permissions.is_active IS 'Flag to indicate if the permission is currently active.';
COMMENT ON COLUMN permissions.created_at IS 'Timestamp of when the permission was created.';
COMMENT ON COLUMN permissions.updated_at IS 'Timestamp of when the permission was last updated.';
