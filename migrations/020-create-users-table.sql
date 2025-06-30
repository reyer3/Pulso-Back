-- Create users table
-- Stores user information, credentials, and status.
-- Designed for secure authentication and detailed user management.

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    dni VARCHAR(20) UNIQUE,
    phone VARCHAR(20),

    -- Account status and security
    status VARCHAR(20) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'suspended')),
    is_email_verified BOOLEAN DEFAULT FALSE,
    email_verified_at TIMESTAMPTZ,

    -- Role relationship
    role_id UUID REFERENCES roles(id) ON DELETE RESTRICT NOT NULL , -- Prevent deleting role if users are assigned

    -- Security tracking
    last_login_at TIMESTAMPTZ,
    last_login_ip VARCHAR(45), -- Supports IPv6
    failed_login_attempts INTEGER DEFAULT 0,
    locked_until TIMESTAMPTZ,
    password_changed_at TIMESTAMPTZ DEFAULT NOW(),

    -- Audit fields
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_by UUID REFERENCES users(id) ON DELETE SET NULL,
    updated_by UUID REFERENCES users(id) ON DELETE SET NULL
);

-- Indexes for performance and common queries
CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);
CREATE INDEX IF NOT EXISTS idx_users_status ON users (status);
CREATE INDEX IF NOT EXISTS idx_users_role_id ON users (role_id);
CREATE INDEX IF NOT EXISTS idx_users_email_status ON users (email, status);
CREATE INDEX IF NOT EXISTS idx_users_role_status ON users (role_id, status);
CREATE INDEX IF NOT EXISTS idx_users_last_login ON users (last_login_at);

-- Trigger to update 'updated_at' timestamp on any row modification
CREATE OR REPLACE FUNCTION update_users_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_users_updated_at
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION update_users_updated_at_column();

-- Comments for clarity
COMMENT ON TABLE users IS 'Stores user information, credentials, and status for authentication and authorization.';
COMMENT ON COLUMN users.id IS 'Unique identifier for the user.';
COMMENT ON COLUMN users.email IS 'Unique email address for the user, used for login.';
COMMENT ON COLUMN users.password_hash IS 'Hashed password for secure storage.';
COMMENT ON COLUMN users.first_name IS 'User''s first name.';
COMMENT ON COLUMN users.last_name IS 'User''s last name.';
COMMENT ON COLUMN users.dni IS 'User''s National Identity Document number (optional, unique).';
COMMENT ON COLUMN users.phone IS 'User''s phone number (optional).';
COMMENT ON COLUMN users.status IS 'Account status (active, inactive, suspended).';
COMMENT ON COLUMN users.is_email_verified IS 'Flag indicating if the user''s email address has been verified.';
COMMENT ON COLUMN users.email_verified_at IS 'Timestamp of when the email was verified.';
COMMENT ON COLUMN users.role_id IS 'Foreign key referencing the user''s assigned role in the roles table.';
COMMENT ON COLUMN users.last_login_at IS 'Timestamp of the user''s last successful login.';
COMMENT ON COLUMN users.last_login_ip IS 'IP address from which the user last logged in.';
COMMENT ON COLUMN users.failed_login_attempts IS 'Counter for consecutive failed login attempts.';
COMMENT ON COLUMN users.locked_until IS 'Timestamp until which the account is locked due to failed attempts.';
COMMENT ON COLUMN users.password_changed_at IS 'Timestamp of the last password change.';
COMMENT ON COLUMN users.created_at IS 'Timestamp of when the user account was created.';
COMMENT ON COLUMN users.updated_at IS 'Timestamp of when the user account was last updated.';
COMMENT ON COLUMN users.created_by IS 'User ID of the creator of this user account.';
COMMENT ON COLUMN users.updated_by IS 'User ID of the last user who updated this account.';
