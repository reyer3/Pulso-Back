-- Create csrf_tokens table
-- Stores CSRF tokens for protection against Cross-Site Request Forgery attacks.
-- Implements a double submit cookie pattern with token rotation and expiration.

CREATE TABLE IF NOT EXISTS csrf_tokens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token_hash VARCHAR(255) UNIQUE NOT NULL,
    user_id UUID REFERENCES users(id) ON DELETE CASCADE, -- Can be NULL for anonymous users

    -- Token metadata
    expires_at TIMESTAMPTZ NOT NULL,
    is_used BOOLEAN DEFAULT FALSE,
    used_at TIMESTAMPTZ,

    -- Session tracking
    session_id VARCHAR(255), -- Optional: Link to a specific session if applicable
    ip_address VARCHAR(45),   -- Supports IPv6

    -- Audit fields
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_csrf_tokens_token_hash ON csrf_tokens (token_hash);
CREATE INDEX IF NOT EXISTS idx_csrf_tokens_user_id ON csrf_tokens (user_id);
CREATE INDEX IF NOT EXISTS idx_csrf_tokens_expires_at ON csrf_tokens (expires_at);
CREATE INDEX IF NOT EXISTS idx_csrf_tokens_user_valid ON csrf_tokens (user_id, expires_at, is_used);

-- Comments for clarity
COMMENT ON TABLE csrf_tokens IS 'Stores CSRF tokens for Cross-Site Request Forgery protection.';
COMMENT ON COLUMN csrf_tokens.id IS 'Unique identifier for the CSRF token.';
COMMENT ON COLUMN csrf_tokens.token_hash IS 'Hashed version of the CSRF token for secure storage.';
COMMENT ON COLUMN csrf_tokens.user_id IS 'Foreign key referencing the user this token is associated with (can be NULL for anonymous users).';
COMMENT ON COLUMN csrf_tokens.expires_at IS 'Timestamp when the CSRF token expires.';
COMMENT ON COLUMN csrf_tokens.is_used IS 'Flag indicating if the token has already been used.';
COMMENT ON COLUMN csrf_tokens.used_at IS 'Timestamp when the token was used.';
COMMENT ON COLUMN csrf_tokens.session_id IS 'Optional session identifier to bind the token to a specific session.';
COMMENT ON COLUMN csrf_tokens.ip_address IS 'IP address from which the token was generated.';
COMMENT ON COLUMN csrf_tokens.created_at IS 'Timestamp of when the CSRF token was created.';
