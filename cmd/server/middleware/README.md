# Hatch Middleware

This package provides authentication and authorization middleware for the Hatch server. It supports multiple authentication methods and integrates with gRPC interceptors for both unary and streaming RPCs.

## Features

- Multiple authentication methods:
  - Basic Authentication
  - Bearer Token Authentication
  - JWT Authentication (HMAC, RSA, and ECDSA)
  - OAuth2 Authentication
- Role-based access control
- Health check endpoint bypass
- Configurable authentication settings
- Comprehensive test coverage

## Authentication Methods

### Basic Authentication

Basic authentication uses username/password pairs stored in the configuration. Passwords are stored as plain text in the configuration file.

```yaml
auth:
  enabled: true
  type: basic
  basic_auth:
    users:
      admin:
        password: "admin123"
        roles: ["admin"]
      user:
        password: "user123"
        roles: ["user"]
```

### Bearer Token Authentication

Bearer token authentication uses predefined tokens mapped to users. Each token is associated with a specific user.

```yaml
auth:
  enabled: true
  type: bearer
  bearer_auth:
    tokens:
      "token1": "user1"
      "token2": "user2"
```

### JWT Authentication

JWT authentication supports multiple signing methods:

- HMAC (HS256, HS384, HS512)
- RSA (RS256, RS384, RS512)
- ECDSA (ES256, ES384, ES512)

```yaml
auth:
  enabled: true
  type: jwt
  jwt_auth:
    secret: "your-secret-key"  # For HMAC
    public_key: "path/to/public.pem"  # For RSA/ECDSA
    issuer: "your-issuer"
    audience: "your-audience"
```

### OAuth2 Authentication

OAuth2 authentication supports:

- Authorization Code flow
- Client Credentials flow
- Refresh Token flow

```yaml
auth:
  enabled: true
  type: oauth2
  oauth2_auth:
    clients:
      client1:
        secret: "client1secret"
        redirect_uris: ["http://localhost:8080/callback"]
        grant_types: ["authorization_code", "refresh_token"]
    token_expiry: 3600  # in seconds
    refresh_token_expiry: 604800  # in seconds
```

## Usage

The middleware can be used with both unary and streaming gRPC interceptors:

```go
// Create middleware instance
middleware := NewAuthMiddleware(config, logger)

// Use with unary interceptor
server := grpc.NewServer(
    grpc.UnaryInterceptor(middleware.UnaryInterceptor()),
)

// Use with stream interceptor
server := grpc.NewServer(
    grpc.StreamInterceptor(middleware.StreamInterceptor()),
)
```

## Context Values

The middleware adds the following values to the context:

- `UserKey`: The authenticated username
- `RolesKey`: The user's roles

These can be accessed using the provided helper functions:

```go
user := AuthenticatedUser(ctx)
roles := AuthenticatedRoles(ctx)
```

## Health Check Bypass

The middleware automatically bypasses authentication for health check endpoints:

- `/grpc.health.v1.Health/Check`
- `/grpc.health.v1.Health/Watch`

## Security Considerations

1. **Basic Auth**:
   - Use HTTPS in production
   - Consider using a more secure authentication method
   - Regularly rotate passwords

2. **Bearer Tokens**:
   - Use strong, random tokens
   - Implement token rotation
   - Store tokens securely

3. **JWT**:
   - Use appropriate key sizes
   - Set reasonable expiration times
   - Validate all claims
   - Use secure signing algorithms

4. **OAuth2**:
   - Use HTTPS for all endpoints
   - Implement proper token storage
   - Validate all redirect URIs
   - Use secure client secrets

## Testing

The package includes comprehensive tests for all authentication methods and edge cases. Run the tests using:

```bash
go test -v ./...
```

## Contributing

When adding new authentication methods or features:

1. Add appropriate configuration options
2. Implement the authentication logic
3. Add comprehensive tests
4. Update this documentation
5. Follow the existing code style and patterns
