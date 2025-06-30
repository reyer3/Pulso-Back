-- Create roles table
-- Stores user roles for role-based access control (RBAC).
-- Ensures that critical system roles cannot be accidentally deleted.

CREATE TABLE IF NOT EXISTS roles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(50) UNIQUE NOT NULL,
    display_name VARCHAR(100) NOT NULL,
    description TEXT,
    is_system BOOLEAN DEFAULT FALSE, -- Prevents deletion of critical roles
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_by UUID REFERENCES users(id) ON DELETE SET NULL,
    updated_by UUID REFERENCES users(id) ON DELETE SET NULL
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_roles_name_active ON roles (name, is_active);

-- Trigger to update 'updated_at' timestamp on any row modification
CREATE OR REPLACE FUNCTION update_roles_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_roles_updated_at
BEFORE UPDATE ON roles
FOR EACH ROW
EXECUTE FUNCTION update_roles_updated_at_column();

-- Comments for clarity
COMMENT ON TABLE roles IS 'Stores user roles for role-based access control (RBAC).';
COMMENT ON COLUMN roles.id IS 'Unique identifier for the role.';
COMMENT ON COLUMN roles.name IS 'Unique internal name for the role (e.g., superadmin, analyst).';
COMMENT ON COLUMN roles.display_name IS 'User-friendly display name for the role (e.g., Super Administrator).';
COMMENT ON COLUMN roles.description IS 'Detailed description of the role and its responsibilities.';
COMMENT ON COLUMN roles.is_system IS 'Flag to indicate if the role is a system-critical role (prevents deletion).';
COMMENT ON COLUMN roles.is_active IS 'Flag to indicate if the role is currently active and usable.';
COMMENT ON COLUMN roles.created_at IS 'Timestamp of when the role was created.';
COMMENT ON COLUMN roles.updated_at IS 'Timestamp of when the role was last updated.';
COMMENT ON COLUMN roles.created_by IS 'User ID of the creator of this role.';
COMMENT ON COLUMN roles.updated_by IS 'User ID of the last user who updated this role.';
