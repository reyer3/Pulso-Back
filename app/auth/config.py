"""
🔐 Authentication and Authorization Configuration
Secure settings management for JWT, CSRF, and security features

Features:
- Environment-based configuration
- Security defaults and validation
- JWT token configuration
- CSRF protection settings
- Rate limiting configuration
- Password security requirements
"""

import os
import re
from typing import Optional
from datetime import timedelta
from pydantic import BaseSettings, validator, Field


class AuthSettings(BaseSettings):
    """
    Authentication and authorization configuration
    
    Loads settings from environment variables with secure defaults
    """
    
    # JWT Configuration
    jwt_secret_key: str = Field(..., min_length=32, description="JWT signing secret key (minimum 32 characters)")
    jwt_algorithm: str = Field(default="HS256", description="JWT signing algorithm")
    jwt_access_token_expire_minutes: int = Field(default=30, ge=5, le=120, description="Access token expiration in minutes")
    jwt_refresh_token_expire_days: int = Field(default=7, ge=1, le=30, description="Refresh token expiration in days")
    
    # CSRF Configuration
    csrf_secret_key: str = Field(..., min_length=32, description="CSRF token secret key (minimum 32 characters)")
    csrf_token_expire_hours: int = Field(default=24, ge=1, le=48, description="CSRF token expiration in hours")
    
    # Password Security
    bcrypt_rounds: int = Field(default=12, ge=10, le=15, description="Bcrypt hashing rounds")
    password_min_length: int = Field(default=8, ge=6, le=50, description="Minimum password length")
    password_require_uppercase: bool = Field(default=True, description="Require uppercase letter in password")
    password_require_lowercase: bool = Field(default=True, description="Require lowercase letter in password")
    password_require_numbers: bool = Field(default=True, description="Require numbers in password")
    password_require_special_chars: bool = Field(default=True, description="Require special characters in password")
    
    # Rate Limiting
    rate_limit_login_attempts: int = Field(default=5, ge=3, le=20, description="Max login attempts before locking")
    rate_limit_login_window_minutes: int = Field(default=15, ge=5, le=60, description="Rate limiting window in minutes")
    account_lockout_duration_minutes: int = Field(default=30, ge=15, le=1440, description="Account lockout duration")
    
    # Session Security
    session_cookie_secure: bool = Field(default=True, description="Use secure cookies in production")
    session_cookie_httponly: bool = Field(default=True, description="Use HTTP-only cookies")
    session_cookie_samesite: str = Field(default="lax", regex="^(strict|lax|none)$", description="SameSite cookie attribute")
    session_cookie_max_age_hours: int = Field(default=24, ge=1, le=168, description="Session cookie max age in hours")
    
    # CORS Configuration
    cors_allowed_origins: list = Field(default=["http://localhost:3000", "http://localhost:3001"], description="Allowed CORS origins")
    cors_allow_credentials: bool = Field(default=True, description="Allow credentials in CORS requests")
    cors_max_age: int = Field(default=3600, description="CORS preflight max age")
    
    # Security Headers
    security_headers_enabled: bool = Field(default=True, description="Enable security headers")
    
    # Development/Production Mode
    environment: str = Field(default="development", regex="^(development|staging|production)$", description="Environment mode")
    debug_mode: bool = Field(default=False, description="Enable debug mode")
    
    @validator('jwt_secret_key', 'csrf_secret_key')
    def validate_secret_keys(cls, v):
        """Ensure secret keys are sufficiently strong"""
        if len(v) < 32:
            raise ValueError('Secret keys must be at least 32 characters long')
        if v in ['change-me', 'secret', 'password', 'dev-secret']:
            raise ValueError('Secret keys cannot use default/weak values')
        return v
    
    @validator('environment')
    def validate_environment_settings(cls, v, values):
        """Validate security settings for production"""
        if v == 'production':
            # Ensure production security requirements
            if values.get('debug_mode', False):
                raise ValueError('Debug mode must be disabled in production')
            if not values.get('session_cookie_secure', True):
                raise ValueError('Secure cookies must be enabled in production')
        return v
    
    @property
    def jwt_access_token_expire_timedelta(self) -> timedelta:
        """Get JWT access token expiration as timedelta"""
        return timedelta(minutes=self.jwt_access_token_expire_minutes)
    
    @property
    def jwt_refresh_token_expire_timedelta(self) -> timedelta:
        """Get JWT refresh token expiration as timedelta"""
        return timedelta(days=self.jwt_refresh_token_expire_days)
    
    @property
    def csrf_token_expire_timedelta(self) -> timedelta:
        """Get CSRF token expiration as timedelta"""
        return timedelta(hours=self.csrf_token_expire_hours)
    
    @property
    def rate_limit_window_timedelta(self) -> timedelta:
        """Get rate limiting window as timedelta"""
        return timedelta(minutes=self.rate_limit_login_window_minutes)
    
    @property
    def account_lockout_timedelta(self) -> timedelta:
        """Get account lockout duration as timedelta"""
        return timedelta(minutes=self.account_lockout_duration_minutes)
    
    @property
    def session_cookie_max_age_timedelta(self) -> timedelta:
        """Get session cookie max age as timedelta"""
        return timedelta(hours=self.session_cookie_max_age_hours)
    
    @property
    def is_production(self) -> bool:
        """Check if running in production environment"""
        return self.environment == 'production'
    
    @property
    def is_development(self) -> bool:
        """Check if running in development environment"""
        return self.environment == 'development'
    
    class Config:
        env_prefix = ""  # No prefix for environment variables
        case_sensitive = False
        env_file = ".env"
        env_file_encoding = "utf-8"


# Global settings instance
auth_settings = AuthSettings()


def get_auth_settings() -> AuthSettings:
    """Get authentication settings instance"""
    return auth_settings


# Validation helper functions
def validate_password_strength(password: str) -> tuple[bool, list[str]]:
    """
    Validate password against security requirements
    
    Returns:
        tuple: (is_valid, list_of_errors)
    """
    settings = get_auth_settings()
    errors = []
    
    # Check minimum length
    if len(password) < settings.password_min_length:
        errors.append(f"Password must be at least {settings.password_min_length} characters long")
    
    # Check uppercase requirement
    if settings.password_require_uppercase and not re.search(r'[A-Z]', password):
        errors.append("Password must contain at least one uppercase letter")
    
    # Check lowercase requirement
    if settings.password_require_lowercase and not re.search(r'[a-z]', password):
        errors.append("Password must contain at least one lowercase letter")
    
    # Check numbers requirement
    if settings.password_require_numbers and not re.search(r'\d', password):
        errors.append("Password must contain at least one number")
    
    # Check special characters requirement
    if settings.password_require_special_chars and not re.search(r'[!@#$%^&*()_+\-=\[\]{};:"\\|,.<>\?]', password):
        errors.append("Password must contain at least one special character")
    
    # Check for common weak patterns
    weak_patterns = [
        (r'(.)\1{2,}', "Password cannot contain 3 or more consecutive identical characters"),
        (r'(012|123|234|345|456|567|678|789|890)', "Password cannot contain sequential numbers"),
        (r'(abc|bcd|cde|def|efg|fgh|ghi|hij|ijk|jkl|klm|lmn|mno|nop|opq|pqr|qrs|rst|stu|tuv|uvw|vwx|wxy|xyz)', "Password cannot contain sequential letters"),
    ]
    
    for pattern, message in weak_patterns:
        if re.search(pattern, password.lower()):
            errors.append(message)
    
    # Check against common passwords
    common_passwords = [
        'password', '123456', '123456789', 'qwerty', 'abc123', 
        'password123', 'admin', 'letmein', 'welcome', 'monkey'
    ]
    
    if password.lower() in common_passwords:
        errors.append("Password is too common and easily guessable")
    
    return len(errors) == 0, errors


def validate_email_format(email: str) -> bool:
    """
    Validate email format using regex
    
    Args:
        email: Email address to validate
        
    Returns:
        bool: True if valid email format
    """
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(email_pattern, email) is not None


def is_safe_redirect_url(url: str, allowed_hosts: list = None) -> bool:
    """
    Validate if a redirect URL is safe (prevents open redirect attacks)
    
    Args:
        url: URL to validate
        allowed_hosts: List of allowed hostnames
        
    Returns:
        bool: True if URL is safe for redirect
    """
    if not url:
        return False
    
    # Only allow relative URLs or URLs from allowed hosts
    if url.startswith('/'):
        return True
    
    if allowed_hosts and any(url.startswith(f"http://{host}") or url.startswith(f"https://{host}") for host in allowed_hosts):
        return True
    
    return False


def generate_secure_token(length: int = 32) -> str:
    """
    Generate a cryptographically secure random token
    
    Args:
        length: Token length in bytes
        
    Returns:
        str: Hex-encoded secure token
    """
    import secrets
    return secrets.token_hex(length)


def get_client_ip(request) -> str:
    """
    Extract client IP address from request, considering proxies
    
    Args:
        request: FastAPI request object
        
    Returns:
        str: Client IP address
    """
    # Check for IP in forwarded headers (common with load balancers/proxies)
    forwarded_for = request.headers.get('X-Forwarded-For')
    if forwarded_for:
        # Take the first IP in the chain (original client)
        return forwarded_for.split(',')[0].strip()
    
    real_ip = request.headers.get('X-Real-IP')
    if real_ip:
        return real_ip
    
    # Fallback to direct connection IP
    return request.client.host if request.client else '0.0.0.0'


def get_user_agent(request) -> str:
    """
    Extract user agent from request
    
    Args:
        request: FastAPI request object
        
    Returns:
        str: User agent string
    """
    return request.headers.get('User-Agent', 'Unknown')


def mask_sensitive_data(data: str, mask_char: str = '*', visible_chars: int = 4) -> str:
    """
    Mask sensitive data for logging (emails, tokens, etc.)
    
    Args:
        data: Sensitive data to mask
        mask_char: Character to use for masking
        visible_chars: Number of characters to keep visible at the end
        
    Returns:
        str: Masked data
    """
    if not data or len(data) <= visible_chars:
        return mask_char * len(data) if data else ''
    
    masked_length = len(data) - visible_chars
    return mask_char * masked_length + data[-visible_chars:]


# Export main configuration
__all__ = [
    'AuthSettings',
    'auth_settings',
    'get_auth_settings',
    'validate_password_strength',
    'validate_email_format',
    'is_safe_redirect_url',
    'generate_secure_token',
    'get_client_ip',
    'get_user_agent',
    'mask_sensitive_data'
]
