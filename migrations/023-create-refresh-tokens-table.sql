-- Create refresh_tokens table
-- Stores JWT refresh tokens for persistent sessions and secure token management.
-- Includes features for token rotation, revocation, and device tracking.

CREATE TABLE IF NOT EXISTS refresh_tokens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token_hash VARCHAR(255) UNIQUE NOT NULL,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,

    -- Token metadata
    expires_at TIMESTAMPTZ NOT NULL,
    is_revoked BOOLEAN DEFAULT FALSE,
    revoked_at TIMESTAMPTZ,
    revoked_reason VARCHAR(100), -- e.g., 'logout', 'expired', 'security_breach'

    -- Device/session tracking
    device_info TEXT, -- User agent, device details
    ip_address VARCHAR(45), -- Supports IPv6

    -- Audit fields
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_used_at TIMESTAMPTZ
);

-- Indexes for performance and common queries
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_token_hash ON refresh_tokens (token_hash);
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_user_id ON refresh_tokens (user_id);
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_user_valid ON refresh_tokens (user_id, is_revoked, expires_at);
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_expires_at ON refresh_tokens (expires_at);


-- Comments for clarity
COMMENT ON TABLE refresh_tokens IS 'Stores JWT refresh tokens for persistent user sessions and secure token management.';
COMMENT ON COLUMN refresh_tokens.id IS 'Unique identifier for the refresh token.';
COMMENT ON COLUMN refresh_tokens.token_hash IS 'Hashed version of the refresh token for secure storage.';
COMMENT ON COLUMN refresh_tokens.user_id IS 'Foreign key referencing the user who owns this token.';
COMMENT ON COLUMN refresh_tokens.expires_at IS 'Timestamp when the refresh token expires.';
COMMENT ON COLUMN refresh_tokens.is_revoked IS 'Flag indicating if the token has been revoked (e.g., due to logout or security event).';
COMMENT ON COLUMN refresh_tokens.revoked_at IS 'Timestamp when the token was revoked.';
COMMENT ON COLUMN refresh_tokens.revoked_reason IS 'Reason for token revocation (e.g., logout, expired, security_breach).';
COMMENT ON COLUMN refresh_tokens.device_info IS 'Information about the device or client that requested the token (e.g., user agent).';
COMMENT ON COLUMN refresh_tokens.ip_address IS 'IP address from which the token was requested.';
COMMENT ON COLUMN refresh_tokens.created_at IS 'Timestamp of when the refresh token was created.';
COMMENT ON COLUMN refresh_tokens.last_used_at IS 'Timestamp of when the refresh token was last used.';
