"""Self-signed CA used by the DockerProxy to MITM-decrypt outbound HTTPS.

Two files live in `_CERT_DIR`:

- `ca-cert.pem` — public cert. Trusted by the worker container so HTTPS
  through the proxy doesn't fail validation.
- `ca.pem` — combined key + cert. Used only by the proxy container
  (mitmproxy expects this concatenated format). The private key in here
  is local-development only; it's gitignored and regenerated per host.
"""

import datetime
from pathlib import Path

_CERT_DIR = Path(__file__).parent / "_certs"
_CA_CERT_PEM = _CERT_DIR / "ca-cert.pem"
# mitmproxy looks for `<confdir>/mitmproxy-ca.pem` (the combined PEM with
# both the private key and the cert). We use that exact name in our
# cert dir so mounting `_CERT_DIR` as the proxy's confdir Just Works.
_CA_COMBINED_PEM = _CERT_DIR / "mitmproxy-ca.pem"
_CN = "agent-harness-platform-proxy"
_VALIDITY_DAYS = 3650


def ensure_certs() -> None:
    """Generate the proxy CA cert + key into `_CERT_DIR` if missing."""
    if _CA_CERT_PEM.exists() and _CA_COMBINED_PEM.exists():
        return
    _CERT_DIR.mkdir(parents=True, exist_ok=True)
    _generate_ca()


def cert_dir() -> Path:
    """Where the cert files live (call `ensure_certs()` first)."""
    return _CERT_DIR


def public_cert_path() -> Path:
    """Path to the public cert (mount into sandbox containers)."""
    return _CA_CERT_PEM


def combined_cert_path() -> Path:
    """Path to the key + cert combined file (mount into the proxy)."""
    return _CA_COMBINED_PEM


def _generate_ca() -> None:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, _CN)])
    now = datetime.datetime.now(datetime.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=_VALIDITY_DAYS))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(key, hashes.SHA256())
    )
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )
    cert_pem = cert.public_bytes(serialization.Encoding.PEM)
    _CA_CERT_PEM.write_bytes(cert_pem)
    _CA_COMBINED_PEM.write_bytes(key_pem + cert_pem)
