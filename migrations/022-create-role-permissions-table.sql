-- Create role_permissions table (Many-to-Many association)
-- Links roles to permissions, defining what actions a role can perform.
-- Ensures data integrity with foreign key constraints and cascading deletes.

CREATE TABLE IF NOT EXISTS role_permissions (
    role_id UUID NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    permission_id UUID NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,
    PRIMARY KEY (role_id, permission_id), -- Composite primary key
    CONSTRAINT unique_role_permission UNIQUE (role_id, permission_id) -- Ensure unique pairings
);

-- Indexes for performance on join operations
CREATE INDEX IF NOT EXISTS idx_role_permissions_role_id ON role_permissions (role_id);
CREATE INDEX IF NOT EXISTS idx_role_permissions_permission_id ON role_permissions (permission_id);

-- Comments for clarity
COMMENT ON TABLE role_permissions IS 'Association table linking roles to their assigned permissions (Many-to-Many).';
COMMENT ON COLUMN role_permissions.role_id IS 'Foreign key referencing the role.';
COMMENT ON COLUMN role_permissions.permission_id IS 'Foreign key referencing the permission.';
COMMENT ON CONSTRAINT unique_role_permission ON role_permissions IS 'Ensures that each role-permission assignment is unique.';
