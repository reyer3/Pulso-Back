-- Create audit_logs table
-- Records significant events and actions within the system for security and compliance.
-- Tracks user actions, system events, and changes to critical data.

CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES users(id) ON DELETE SET NULL, -- User who performed the action (NULL if system event)

    -- Event details
    event_type VARCHAR(50) NOT NULL,        -- e.g., 'login', 'user_created', 'role_assigned'
    event_category VARCHAR(30) NOT NULL,   -- e.g., 'auth', 'user_mgmt', 'security'
    event_description TEXT NOT NULL,

    -- Result and status
    result VARCHAR(20) NOT NULL CHECK (result IN ('success', 'failure', 'error')),
    error_message TEXT,

    -- Context information
    ip_address VARCHAR(45),    -- Supports IPv6
    user_agent TEXT,
    request_id VARCHAR(100),   -- Correlation ID for tracing requests

    -- Target resource (what was affected by the event)
    target_type VARCHAR(50),   -- e.g., 'user', 'role', 'permission'
    target_id VARCHAR(100),    -- ID of the affected resource
    target_details TEXT,       -- JSON or structured text with details of the change or event

    -- Timing
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for performance and common queries
CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_user_event ON audit_logs (user_id, event_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_event_category ON audit_logs (event_category, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_target ON audit_logs (target_type, target_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_ip_created ON audit_logs (ip_address, created_at DESC);


-- Comments for clarity
COMMENT ON TABLE audit_logs IS 'Records significant system events and user actions for security, compliance, and debugging.';
COMMENT ON COLUMN audit_logs.id IS 'Unique identifier for the audit log entry.';
COMMENT ON COLUMN audit_logs.user_id IS 'Foreign key referencing the user who performed the action (NULL for system events).';
COMMENT ON COLUMN audit_logs.event_type IS 'Type of event that occurred (e.g., login, user_created, data_exported).';
COMMENT ON COLUMN audit_logs.event_category IS 'Category of the event (e.g., auth, user_mgmt, security, system).';
COMMENT ON COLUMN audit_logs.event_description IS 'Detailed description of the event.';
COMMENT ON COLUMN audit_logs.result IS 'Outcome of the event (success, failure, error).';
COMMENT ON COLUMN audit_logs.error_message IS 'Error message if the event resulted in a failure or error.';
COMMENT ON COLUMN audit_logs.ip_address IS 'IP address associated with the event.';
COMMENT ON COLUMN audit_logs.user_agent IS 'User agent string of the client that initiated the event.';
COMMENT ON COLUMN audit_logs.request_id IS 'Optional request or correlation ID for tracing related events.';
COMMENT ON COLUMN audit_logs.target_type IS 'Type of the resource affected by the event (e.g., user, role).';
COMMENT ON COLUMN audit_logs.target_id IS 'Identifier of the affected resource.';
COMMENT ON COLUMN audit_logs.target_details IS 'Additional details about the event or changes made (e.g., JSON diff).';
COMMENT ON COLUMN audit_logs.created_at IS 'Timestamp of when the event was logged.';
