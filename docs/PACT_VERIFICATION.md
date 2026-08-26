# Pact Provider Verification
This service is verified as a provider against consumer contracts via Pact Broker.

## Overview

This service uses Pact to ensure compatibility with consumer contracts. Provider verification is run on each pull request (PR) and the results are published back to the Pact Broker. If a change breaks a known consumer contract, the PR check fails.

## Prerequisites

- Access to a Pact Broker instance.
- The `PACT_BROKER_URL` secret configured in GitHub repository secrets.
- An optional `PACT_BROKER_TOKEN` secret for authenticated access if required.

## Local Verification

@r test -tags=pact ./tests/pact/...

This triggers the provider verification test scaffold which fetches the latest consumer pacts from the broker and runs them against the provider.

## CI Workflow

The CI workflow is defined in `.github/workflows/pact-provider.yml`. It runs on `pull_request` events and performs the following steps:

1. Checkout the repository.
2. Set up Go.
3. Run the Pact provider verification tests with the `pact` build tag.
4. Publish the verification results back to the Pact Broker using the `pact-broker` CLI (or `go run` task) so the broker can update the compatibility matrix.

### Workflow Configuration

The workflow expects the following secrets:

- `PACT_BROKER_URL` : The base URL of the Pact Broker (required).
- `PACT_BROKER_TOKEN` : An API token for publishing verification results (required if broker requires authentication).

The workflow uses `PACT_BROKER_URL` directly in the test invocation and the `pact-broker` publish step.

## Security

- Credentials are stored as GitHub secrets and referenced using the `secrets` context. They are never hardcoded.
- Sensitive values are masked in logs.

## Edge Cases

- **New consumer added upstream**: The provider verification picks up new consumer contracts automatically because it fetches all pacts for the provider from the broker. No code changes are needed.
- @provider state changes: The test scaffold includes provider state handlers to support consumer expectations.

## Contributing

When modifying the provider, ensure that you run the verification tests locally before pushing. The CI check will block merges if any consumer contract is broken.

## References

- [Pact documentation](https://docs.pact.io/)
- [Pact Go](https://github.com/pact-foundation/pact-go)
