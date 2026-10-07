# OpenShift Provider Compatibility Tool (`opct`)

OpenShift Provider Compatibility Tool (OPCT) is used to orchestrate workflows for conformance
test suites on OpenShift/OKD installations on cloud providers or hardware.

## Documentation

- [OPCT Overview](https://redhat-openshift-ecosystem.github.io/opct/)
- [User Guide](https://redhat-openshift-ecosystem.github.io/opct/user/)
- [Development Guide](https://redhat-openshift-ecosystem.github.io/opct/devel/guide)

## Getting started

- Download OPCT

```bash
curl -fsSL https://redhat-openshift-ecosystem.github.io/opct/install.sh | bash
```

Optionally pin a release version or choose a custom directory:

```bash
curl -fsSL https://redhat-openshift-ecosystem.github.io/opct/install.sh | \
  OPCT_VERSION=v0.6.7 INSTALL_DIR="$HOME/bin" bash
```

Or download and inspect the script before running:

```bash
curl -fsSL -o install.sh https://redhat-openshift-ecosystem.github.io/opct/install.sh && \
  less install.sh && \
  bash install.sh
```

After installation, ensure the install directory is in your `PATH` (default
`$HOME/.local/bin`):

```bash
export PATH="$HOME/.local/bin:$PATH"
```

If you chose a custom `INSTALL_DIR`, use that path instead — the installer
prints the correct export command. To make this permanent, add the export
line to your shell profile (`~/.bashrc`, `~/.zshrc`, or `~/.profile`).

> **Note:** The hosted install script URL becomes available after this change
> is merged and the documentation site is deployed.

- Setup a dedicated node to run the test environment (preferred to prevent disruption)

```bash
opct adm e2e-dedicated taint-node
```

- Run regular conformance tests

```bash
opct run --wait
```

- Check the status (optional when not using `--wait` on `run`)

```bash
opct status --wait
```

- Collcet the results

```bash
opct retrieve
```

- Read the report

```bash
opct report *.tar.gz
```

- Destroy the environment

```bash
opct destroy
```

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.
