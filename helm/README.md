# Kitaru Helm Chart

![Kitaru Logo](https://kitaru.ai/kitaru-logo.svg)

## Overview

[Kitaru](https://kitaru.ai) is a durable execution layer for AI agents. It provides primitives that make agent workflows persistent, replayable, and observable — without requiring users to learn a graph DSL or change their Python control flow.

## Quickstart

### Install the Chart

To install the Kitaru chart directly from Amazon ECR, use the following command:

```bash
# example command for version 0.2.0
helm install my-kitaru oci://public.ecr.aws/kitaru/kitaru --version 0.2.0
```

Note: Ensure you have OCI support enabled in your Helm client.

## Configuration

This chart offers a multitude of configuration options. For detailed
information, check the default [`values.yaml`](values.yaml) file. For full
details of the configuration options, refer to the [Kitaru documentation](https://docs.zenml.io/getting-started/deploying-kitaru/deploy-with-helm).

### PostgreSQL TLS

Configure PostgreSQL TLS with `server.database.sslMode`. Supported modes are
`disable`, `require`, `verify-ca`, and `verify-full`. The `verify-full` mode
uses the system CA trust store unless `sslCa` supplies a custom CA.

Certificates can be supplied inline:

```yaml
server:
  database:
    sslMode: verify-full
    sslCa:
      value: |
        -----BEGIN CERTIFICATE-----
        ...
        -----END CERTIFICATE-----
```

For production deployments, referencing an existing Kubernetes Secret avoids
placing certificate material in Helm values:

```yaml
server:
  database:
    sslMode: verify-full
    sslCa:
      secretRef:
        name: postgres-ca
        key: ca.crt
    sslCert:
      secretRef:
        name: postgres-client
        key: tls.crt
    sslKey:
      secretRef:
        name: postgres-client
        key: tls.key
```

`sslCert` and `sslKey` configure mutual TLS and must be supplied together. For
each certificate, configure either `value` or `secretRef`, but not both.

### Custom CA Certificates

If you need to connect to services using HTTPS with certificates signed by custom Certificate Authorities (e.g., self-signed certificates), you can configure custom CA certificates. There are two ways to provide custom CA certificates:

1. Direct injection in values.yaml:
```yaml
server:
  certificates:
    customCAs:
      - name: "my-custom-ca"
        certificate: |
          -----BEGIN CERTIFICATE-----
          MIIDXTCCAkWgAwIBAgIJAJC1HiIAZAiIMA0GCSqGSIb3DQEBCwUAMEUxCzAJBgNV
          ...
          -----END CERTIFICATE-----
```

2. Reference existing Kubernetes secrets:
```yaml
server:
  certificates:
    secretRefs:
      - name: "my-secret"
        key: "ca.crt"
```

The certificates will be installed in the server container, allowing it to securely connect to services using these custom CA certificates.

### HTTP Proxy Configuration

If your environment requires a proxy for external connections, you can configure it using:

```yaml
server:
  proxy:
    enabled: true
    httpProxy: "http://proxy.example.com:8080"
    httpsProxy: "http://proxy.example.com:8080"
    # Additional hostnames/domains/IPs/CIDRs to exclude from proxying
    additionalNoProxy:
      - "internal.example.com"
      - "10.0.0.0/8"
```

By default, the following hostnames/domains are excluded from proxying:
- `localhost`, `127.0.0.1`, `::1` (IPv4 and IPv6 localhost)
- `fe80::/10` (IPv6 link-local addresses)
- `.svc` and `.svc.cluster.local` (Kubernetes service DNS domains)
- The hostname from `server.serverURL` if configured
- The ingress hostname (`server.ingress.host`) if configured
- Internal service names used for communication between components

You can add additional exclusions using the `additionalNoProxy` list. The NO_PROXY environment variable accepts:
- Hostnames (e.g., "kitaru.example.com")
- Domain names with leading dot for wildcards (e.g., ".example.com")
- IPv4 addresses (e.g., "10.0.0.1")
- IPv4 ranges in CIDR notation (e.g., "10.0.0.0/8")
- IPv6 addresses (e.g., "::1")
- IPv6 ranges in CIDR notation (e.g., "fe80::/10")

## Telemetry

The Kitaru server collects anonymous usage data to help us improve the product. You can opt out by setting `server.analyticsOptIn` to false.

## Contributing

Feel free to [submit issues or pull requests](https://github.com/zenml-io/kitaru) if you would like to improve the chart.

## License

[This project is licensed](https://github.com/zenml-io/kitaru/blob/main/LICENSE) under the terms of the Apache-2.0 license.

## Further Reading

- [Kitaru Documentation](https://docs.zenml.io)
- [Kitaru Source Code](https://github.com/zenml-io/kitaru)
