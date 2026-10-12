# Trademarks and affiliation

**Open Lakehouse Compute Fabric (OLCF)** is an independent open-source project created by Justin Dodrill and licensed under the
[Apache License 2.0](LICENSE). It is **not** a Microsoft product, and it is not affiliated with, sponsored by, certified by or endorsed by
Microsoft Corporation or any other company named below.

## Third-party marks

All product and service names below are trademarks or registered trademarks of their respective owners. They are used in this repository
only to describe interoperability and compatibility (nominative use).

| Name | Owner |
|---|---|
| Microsoft, Microsoft Fabric, OneLake, Azure, Microsoft Entra, Entra ID, Azure Data Lake Storage, Azure Key Vault, Power BI, Windows | Microsoft Corporation |
| Apache, Apache Spark, Apache Iceberg, Apache Arrow, Apache Parquet, Spark | The Apache Software Foundation |
| Delta Lake, Delta | The Linux Foundation (Delta Lake project) |
| Amazon S3, AWS | Amazon.com, Inc. or its affiliates |
| MinIO | MinIO, Inc. |
| PostgreSQL | PostgreSQL Community Association of Canada |
| Docker | Docker, Inc. |
| Kubernetes | The Linux Foundation |
| Helm | The Linux Foundation / Helm project |
| Prometheus, OpenTelemetry | The Linux Foundation (CNCF) |
| Claude, Anthropic | Anthropic, PBC |
| Model Context Protocol (MCP) | Anthropic, PBC and the MCP project |
| macOS, Xcode | Apple Inc. |
| Java, Temurin (Eclipse Temurin) | Oracle and/or its affiliates; Eclipse Foundation |
| Jupyter | NumFOCUS / Project Jupyter |
| Python | Python Software Foundation |

## Rules this project follows

1. **No implied endorsement.** Documentation and the user interface say "Fabric Runtime 2.0 *Compatible*", never "Fabric Runtime 2.0". The
   runtime never reports itself as Microsoft Fabric (`notebookutils.runtime.context["productType"]` is not `Fabric`).
2. **Compute extension, not Fabric.** The Fabric feature is described as external compute that executes compatible Fabric notebooks on
   customer-controlled machines while OneLake remains the data plane. It does not enlarge or replace Microsoft-managed Spark.
3. **No parity claims.** OLCF reproduces documented, supported semantics deterministically and fails closed otherwise. It does not claim to match
   proprietary Fabric optimisations or behaviour.
4. **The project name.** "Open Lakehouse Compute Fabric" and "OLCF" identify this project. Forks and derived distributions should use a different
   name and must not suggest that they are the official project. Apache License 2.0 section 6 grants no trademark rights.
5. **Contributors** must not add Microsoft logos, product artwork or other third-party brand assets to the repository.

Questions about the use of the project name: open an issue on the project's repository.
