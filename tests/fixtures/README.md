# Test fixtures

`test-cert.pem` / `test-key.pem` are a **self-signed certificate and key used only
by the TLS integration test** (`cargo test --features tls`). They carry no secret
value, are not used by any build, and must never be used in production.

Regenerate with:

```console
openssl req -x509 -newkey rsa:2048 -keyout test-key.pem -out test-cert.pem \
  -days 3650 -nodes -subj "/CN=localhost" \
  -addext "subjectAltName=IP:127.0.0.1,DNS:localhost"
```
