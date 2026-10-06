# Praxis API contracts

This module is the canonical source for Praxis protobuf contracts and their
generated Go transport types. Service modules import these packages; they do
not maintain service-local copies of shared `.proto` files or generated code.

Regenerate code from the repository root:

```bash
make protobuf-generate
```

Lint contracts before committing:

```bash
buf lint api
```

Keep domain models, persistence types, and business logic in their owning
services. This module contains only wire contracts and generated transport code.
