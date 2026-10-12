# Concepts

These pages explain how OLCF is put together and why. Read them in this order, or jump to the topic you need.

| Page | Question it answers |
|---|---|
| [Architecture](architecture.md) | What are the parts, how do they run, and what does a deployment look like? |
| [Components and data flow](components-and-data-flow.md) | What happens, step by step, when a user runs SQL, a notebook, a pipeline or a Fabric submission? Which component touches the data? |
| [Compute model](compute-model.md) | How do machines, Spark clusters, runtimes, providers, queues and the job lifecycle fit together? |
| [Workspaces, items and lakehouses](workspaces-items-lakehouses.md) | What are the objects users work with, and how are they stored, versioned, exported and referenced? |
| [Fabric extension model](fabric-extension-model.md) | How does one call in a Fabric notebook run that notebook on your Spark, and what is guaranteed? |
| [OLCF Intelligence](intelligence.md) | Where does a number come from, what breaks if I change it, what is stale or unused, where does sensitive data end up? How does the asset graph, with confidence and gaps, answer that? |
| [Intelligence on-premises, with Fabric, and against Fabric only](intelligence-fabric-and-onprem.md) | What does Intelligence see, and what does it not, in each deployment shape? |
| [Security model](security-model.md) | Who can do what, how are credentials, tokens, secrets and the audit log protected, and what is explicitly out of scope? |

Related: [architecture decision records](../adr/README.md) explain individual design choices; the [glossary](../reference/glossary.md) defines the vocabulary; [requirements traceability](../compliance/requirements-traceability.md) says which requirements are implemented and how well they are tested.

## The ideas in one page

* **Fabric is an adapter, not the core.** Everything works without Microsoft Fabric. The Fabric code lives in `olcf.fabric` and in the runtime-side `notebookutils` shim. The scheduler reaches the adapter only through an optional service lookup (`maybe_service("fabric")`); storage and catalog do not import it (OneLake is just another storage provider).
* **A single machine is a cluster of one.** The control plane enrols an embedded worker agent over the same protocol that remote machines use.
* **Machines connect outward.** A worker agent opens a WebSocket to the control plane; no inbound port on the worker is needed for control. Spark traffic between nodes of one cluster does need network paths ([ports](../reference/ports-and-networking.md)).
* **Runtimes are immutable and signed.** A runtime (Spark, Python, Java, Delta, hash-pinned wheels) is installed side by side with others and never modified; per-job dependencies are layered on top and keyed by hash.
* **The database is the state.** Jobs, pipeline runs, sessions and the worker registry live in SQLite or PostgreSQL. Any control-plane process can continue after another one fails.
* **Fail closed.** Unsupported semantics raise an actionable error instead of being silently changed: the compatibility analyzer blocks a run before it starts, a Delta protocol upgrade that would break Fabric interoperability is refused, an update without a trusted signature is not applied.
* **Observe, do not interfere.** Intelligence builds a graph of what exists and how it connects from events and scans, says how sure each link is and what it could not see, and never sits on the execution path: if it breaks, jobs and queries do not notice.
* **Authorise on the server, every time.** UI visibility is never a security boundary; every handler checks a permission against the workspace of the object it touches.
