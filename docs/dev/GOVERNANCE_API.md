# OLCF Intelligence governance: REST contract

Author: Justin Dodrill. Status: contract for `src/olcf/intelligence/governance/`; read together with [INTELLIGENCE.md](INTELLIGENCE.md). The governance package **observes and reports**:
nothing in the compute, SQL or pipeline path calls it, a policy with `mode: "enforce"` is refused, and it is optional (the module works, and so do all `/v1/intelligence/*`
routes, when its tables or the whole package are missing).

All routes are under `/v1`. JSON in, JSON out. Errors use the platform envelope `{"error": {"code", "message", "details", "remediation", "correlation_id"}}`.

## 0. Conventions

* **Permissions.** `R` = `intelligence.read`; `C` = `governance.curate`; `A` = `governance.approve`. "in ws" means the permission is checked for the **workspace of the asset**
  (an organisation-wide binding also satisfies it); "org" means an organisation-wide binding is required.
* **Asset visibility.** An asset outside the caller's `intelligence.read` scope does not exist for the caller: `404 not_found` (never 403). Lists silently leave such assets out.
  Evidence (lineage paths, inheritance sources) that names an asset the caller cannot see shows `{"hidden": true}` instead of the asset.
* **Org-level objects** (glossary terms, domains, data products, classification catalog, policies): readable by anyone with `intelligence.read` org-wide **or** in any workspace
  (otherwise `403`); written with org-wide `C` (policies and approvals: org-wide `A`).
* **ETags.** Mutable single resources (term, domain, product, classification, policy, change request, violation, certification) return `ETag: "<etag>"` and an `etag` field; `PATCH`,
  `PUT` and `DELETE` honour `If-Match` (`412 precondition_failed`, `details.current_etag`). Without `If-Match` the last writer wins.
* **Request bodies** are typed pydantic models (`olcf.intelligence.governance.models`; every one is `extra="forbid"`, so a misspelt field is an error, not ignored). A body that has the
  wrong shape (unknown field, wrong type, value outside an enum such as role, status, severity, rule type, mode or bulk action, empty or over-long text, a missing required field, an empty or
  oversized `ids` list) is `422 validation_failed` and the error `details` name the field. What a model cannot know (the asset or user does not exist, a name is taken, a state change is
  not allowed, `params` of a policy rule are invalid) stays `400 bad_request`, `404`, `409` or `422 unsupported` from the manager. Optional-body routes (scan, suggest-links, approve, reject
  of a review item, approve a change, acknowledge a violation) accept no body at all or `{}`. Nullable fields keep "absent" and "explicit null" apart (`PUT /assets/{id}/domain` requires
  `domain_id`; `null` clears it).
* **Pagination.** List routes take `limit` (1..500, default 50) and `cursor` and return `{"items": [...], "next_cursor": str|null}`.
* **Ids.** `trm_` term, `dom_` domain, `dpr_` data product, `cls_` classification, `chg_` change request, `plc_` policy, `vio_` violation, `ast_` asset. Link, assignment and owner rows have
  integer ids. A review-queue item id is `"<kind>:<int>"`, kind `classification` or `term_link`.
* **Compact shapes** used below: `AssetRef = {"id","type","name","path","workspace_id"}`; `ClassRef = {"id","name","category","level","color"}`; `TermRef = {"id","name","status"}`;
  `DomainRef = {"id","name"}`; `ProductRef = {"id","name","status"}`.
* **Owner** `= {"id": int|null, "role": "owner|steward|custodian|contact", "kind": "user|external", "user_id": str|null, "name": str, "email": str|null, "notes": str|null,
  "inherited_from": null | {"asset_id","type","name","distance"} , "created_at"}`. An owner request body is `{"role", "user": "<usr id | e-mail | user name>"}` for an OLCF user, or
  `{"role", "name": "...", "email": "..."}` for an external contact.
* **Violation severities** `low|medium|high|critical`. **Certification states** `none|draft|certified|deprecated`.

## 1. Business glossary (`/governance/glossary`)

| Route | Perm | Notes |
|---|---|---|
| `GET /governance/glossary/terms` | R | `q`, `status`, `glossary`, `domain_id`, `parent_id` (`root` = top level), `certification`, `owner`, `limit`, `cursor` |
| `POST /governance/glossary/terms` | C org | body below; `status: "approved"` needs A org; `201` + ETag |
| `GET /governance/glossary/terms/{id}` | R | term + `ancestors` (TermRef[], root first), `children` (TermRef[]), `related_terms` (TermRef[]; `related` stays the list of ids), `owners`, `certification`, `link_count` |
| `PATCH /governance/glossary/terms/{id}` | C org | `If-Match`; changing `status` to `approved` needs A; cycles in `parent_id` refused (`400`) |
| `DELETE /governance/glossary/terms/{id}?hard=false` | C org (`hard=true`: A org) | soft: `status` becomes `deprecated` and every link is **unlinked** (kept as history, `status: "unlinked"`); hard: row, links and references are removed (children move up one level) |
| `GET /governance/glossary/search?q=&limit=` | R | ranked: exact name 100, exact synonym 90, abbreviation 80, name prefix 60, name contains 40, synonym contains 30, definition 10; items `{term, score, matched_on}` |
| `GET /governance/glossary/terms/{id}/links` | R | `status` (default `approved,pending`), paginated; hidden assets omitted |
| `POST /governance/glossary/terms/{id}/links` | C in ws of the asset (product: org) | `{"asset_id"}` or `{"product_id"}` + optional `comment`; manual links are approved at once; `201` |
| `DELETE /governance/glossary/terms/{id}/links/{link_id}` | C in ws | unlinks |
| `POST /governance/assets/{id}/terms` | C in ws | `{"term_id"}`; same as above from the asset side |
| `DELETE /governance/assets/{id}/terms/{term_id}` | C in ws | |
| `POST /governance/glossary/suggest-links` | C (org, or in `workspace_id`) | `{"workspace_id"?}`: proposes links by exact name / synonym / abbreviation match; they enter the review queue as `suggested`; returns counts |

Term body: `{"name", "definition"?: markdown, "abbreviation"?, "synonyms"?: [str], "glossary"?: "default", "status"?: "draft|approved|deprecated", "parent_id"?, "domain_id"?,
"related"?: [trm ids], "owners"?: [owner]}`. Name is unique (case-insensitive) within a `glossary`; a parent must be in the same glossary.

Term response: `{"id","glossary","name","definition","abbreviation","synonyms","status","certification","parent_id","domain_id","related":[trm ids],"owners":[Owner],"link_count",
"etag","created_by","created_at","updated_at"}`.

Term delete response: `{"id","status": "deleted|deprecated","deleted": bool,"deprecated": bool,"links_unlinked": int}` (soft delete: `status: "deprecated"`; hard delete: `status: "deleted"`).

Link response: `{"id": int, "term": TermRef, "subject_type": "asset|product", "asset": AssetRef|null, "product": ProductRef|null, "source": "manual|suggested", "status":
"approved|pending|rejected|unlinked", "confidence": float|null, "evidence": {...}, "comment", "created_by", "created_at", "reviewed_by", "reviewed_at"}`.

## 2. Domains and data products

| Route | Perm | Notes |
|---|---|---|
| `GET /governance/domains` | R | `q`, `parent_id` (`root`), paginated; items carry `asset_count`, `product_count`, `child_count` |
| `POST /governance/domains` | C org | `{"name","description"?,"parent_id"?,"owners"?}` |
| `GET /governance/domains/{id}` | R | + `ancestors` (root first), `children`, `owners` |
| `PATCH /governance/domains/{id}` | C org | `If-Match` |
| `DELETE /governance/domains/{id}` | C org | `409` while it has sub-domains, products, assigned assets or linked terms |
| `GET /governance/domains/{id}/assets` | R | `type`, `include_descendants=true`, paginated: assets in the domain directly, by inheritance from a parent asset, or through a data product |
| `PUT /governance/assets/{id}/domain` | C in ws | `{"domain_id": "dom_..."|null}` |
| `GET /governance/products` | R | `q`, `status`, `domain_id`, `certification`, `owner` |
| `POST /governance/products` | C org | body below; `status: "published"` needs A org |
| `GET /governance/products/{id}` | R | + `members` (visible only, `hidden_members` = how many the caller cannot see), `consumers`, `quality` (score only), `owners`, `terms`, `certification` |
| `PATCH /governance/products/{id}` | C org | `If-Match` |
| `DELETE /governance/products/{id}` | C org | `409` while `published` (deprecate first) |
| `PUT /governance/products/{id}/members` | C org | `{"asset_ids": [...]}` replaces the member set |
| `POST /governance/products/{id}/members` | C org | `{"asset_id"}`; `DELETE /governance/products/{id}/members/{asset_id}` |
| `GET /governance/products/{id}/consumers` | R | `depth` (1..6), `min_confidence`: assets / reports downstream of the members that are not members, each with the member it reads and the path confidence |

Product body: `{"name","description"?,"domain_id"?,"status"?: "draft|published|deprecated","owners"?,"member_asset_ids"?,"sla"?: {"freshness_hours"?: number, "availability"?: str},
"contract"?: {"columns": [{"name","type"?,"description"?,"nullable"?,"classification"?}], "expectations": [{"name","description"?,"rule"?}]}}`. Members must be of type
`table|view|model_table|measure|report|semantic_model|dashboard` and visible to the caller.

Product response: `{"id","name","description","domain": DomainRef|null,"status","certification","sla","contract","member_count","etag","created_by","created_at","updated_at"}`.

## 3. Owners and stewards

For each subject kind there are the same four routes, with the prefix `assets/{id}`, `domains/{id}`, `products/{id}` or `glossary/terms/{id}`:

| Route | Perm |
|---|---|
| `GET /governance/<prefix>/owners` | R |
| `PUT /governance/<prefix>/owners` | C (asset: in ws; others: org); body `{"owners": [owner,...]}` replaces the **direct** owners |
| `POST /governance/<prefix>/owners` | C; body is one owner; `201`; `409` if already assigned |
| `DELETE /governance/<prefix>/owners/{owner_id}` | C |

Response: `{"subject_type","subject_id","direct": [Owner], "effective": [Owner]}`. `effective` applies inheritance (assets only): **per role** the nearest ancestor-or-self
with an assignment of that role supplies it (column -> table -> lakehouse/schema -> workspace asset); `inherited_from` says where. Domains, products and terms have no inheritance.

## 4. Classifications

| Route | Perm | Notes |
|---|---|---|
| `GET /governance/classifications` | R | catalog; items `{id,name,category,level,description,color,builtin,assignment_count,etag}` |
| `POST /governance/classifications` | C org | `{"name","category","level": 0..5,"description"?,"color"?: "#rrggbb"}` |
| `GET /governance/classifications/{id}` | R | |
| `PATCH /governance/classifications/{id}` | C org | `If-Match`; built-in labels allow only `description` and `color` |
| `DELETE /governance/classifications/{id}` | A org | not built-in; `409` while it has non-rejected assignments |
| `GET /governance/assets/{id}/classifications` | R | `{"asset_id","direct": [Assignment], "inherited": [Assignment], "suggestions": [Assignment], "rejected": [Assignment], "contained": [{"classification": ClassRef, "columns": n}]}`; `contained` counts approved labels on the asset's own columns |
| `POST /governance/assets/{id}/classifications` | C in ws | `{"classification": "<id|name>", "comment"?}`: `201`. Status `approved`, unless the setting `governance.classification_four_eyes` is on and the caller lacks A: then it is `suggested` and waits in the review queue |
| `DELETE /governance/assets/{id}/classifications/{class}` | C in ws (A with four eyes) | `{class}` is an id or a name |
| `GET /governance/classifications/{id}/assets` | R | `workspace_id`, `include_inherited=true`, `type`; items `{asset, source, status, confidence, inherited, precision}` |
| `GET /governance/classifications/{id}/exposure` | R | where does data labelled X end up; see below |
| `POST /governance/classifications/scan` | C (org, or in `workspace_id`) | `{"workspace_id"?, "scan_id"?}`: ingest `attrs.suggested_classifications` now -> review queue; without `workspace_id` org-wide curate is required; returns `{"assets_examined", "classification": {"examined","created","updated","withdrawn","skipped","unmapped"}, "term_links": {"examined","suggested"}}`; audit `governance.classification_scanned` |
| `GET /governance/review-queue` | R | pending suggestions: `kind` (`classification|term_link`), `asset_id`, `workspace_id`, `classification`, `term_id`, `min_confidence`, `q`; oldest first |
| `GET /governance/review-queue/summary` | R | `{"total","classification","term_link","change_requests"}` |
| `POST /governance/review-queue/{item_id}/approve` | C in ws (A with four eyes) | `{"comment"?}` |
| `POST /governance/review-queue/{item_id}/reject` | C in ws | `{"comment"?}` (a rejected suggestion is not proposed again) |
| `POST /governance/review-queue/bulk` | as above | `{"action": "approve|reject", "ids": [item ids] (max 500), "comment"?}` -> `{"results": [{"id","ok","error"?}], "ok": n, "failed": n}` |

Suggestions are **never approved automatically**. Auto-suggested rows carry `source: "auto"`, `confidence` and `evidence = {"rule", "detector_label", "evidence_kinds", "category"}`
(rule names only, never sample values). Detector labels map to catalog labels: email, phone, national_id, person_name, address, date_of_birth, ip_address, geolocation -> `PII`;
credit_card -> `PCI`; iban, bank_account, financial -> `Financial`; credential -> `Credentials`; special_category -> `Restricted`.

**Inheritance (propagation).** Approved classifications propagate along the lineage data flow to downstream assets as `inherited` assignments, computed on demand (never stored) with a
bounded walk (`governance.propagation_depth`, `governance.propagation_max_nodes`, `governance.propagation_min_confidence`), cached per graph version. An inherited item is
`{"id": null, "source": "inherited", "status": "approved", "classification": ClassRef, "confidence": 0.9|0.75|0.4|0.2, "confidence_floor": "verified|derived|inferred|unknown",
"precision": "column|table|contains", "proven": bool, "evidence": {"from_asset": AssetRef|{"hidden":true}, "assignment_id", "path": {"nodes": [AssetRef|{"hidden":true}], "edges": [{"id","type","confidence"}]}}}`.
`precision` is `column` (reached through column-level edges: columns, measures, visuals), `table` (a table-level label followed table-level edges), or `contains`
(the asset contains such a column: a table, semantic model or report that holds an inherited / classified column). `proven` is true for `column` and `table`
precision and for `contains`; the one **unproven** case is *possible exposure*: a classified **column** whose table feeds a downstream table or report by table-level lineage
only (the column-level link is not known). It is listed by the exposure query as `precision: "table"`, `proven: false`, confidence capped at `inferred`, and is never an inherited assignment of the asset.

`GET /governance/classifications/{id}/exposure` query: `depth` (default `governance.propagation_depth`), `min_confidence`, `include_possible=true`, `types` (comma list of asset
types), `workspace_id`. Response: `{"classification": ClassRef, "sources": [{"asset": AssetRef|{"hidden":true}, "assignment_id"}], "exposures": [{"asset": AssetRef, "kind":
"report|visual|semantic_model|data_product|table|measure|...", "from_asset", "precision", "proven", "confidence", "confidence_floor", "path"}], "summary": {"by_type": {...},
"reports": n, "visuals": n, "semantic_models": n, "products": n, "proven": n, "possible": n}, "data_products": [{"product": ProductRef, "via_assets": [asset ids]}], "truncated": bool, "notes": [str]}`.
Only visible exposures are listed; an exposure of a `contains` item carries `contains` (the classified column or visual inside the asset).

Review-queue item: `{"id": "classification:12|term_link:5","kind","asset": AssetRef,"source": "auto|manual|suggested","confidence","evidence","comment","created_by","created_at",
"classification": ClassRef (classification items) | "term": TermRef (term_link items),"requires": "governance.curate|governance.approve"}`. Decisions: `{"id","kind","decision": "approved|rejected","asset_id", ...}`.
An invalid bulk `action` or an empty / over-long `ids` list is `422`; per-item failures (forbidden, already decided, not visible) are reported in `results` and do not stop the rest.
**Permission before state:** a caller who may not decide an item (no curate / approve for the asset's workspace, or four-eyes denial) gets `403` whether or not the item was already decided; only a caller who may decide is told `409 conflict` for a decided item, so the state of an item is never revealed to someone who cannot act on it. An item outside the caller's scope is `404`.

## 5. Certification and change requests

| Route | Perm | Notes |
|---|---|---|
| `GET /governance/assets/{id}/certification`, `.../products/{id}/certification`, `.../glossary/terms/{id}/certification` | R | `{"subject_type","subject_id","state","certified_by","certified_at","comment","invalidated_at","invalidated_reason","schema_known" (assets: false when no schema was known at certification),"schema_changed_since","etag"}` |
| `PUT` the same routes | `draft`/`none`: C; `certified`/`deprecated`: A (asset: in ws; others: org) | `{"state","comment"?}`; `If-Match`. Certifying a **term** needs the term to be `approved`. |
| `GET /governance/certifications` | R | `state`, `subject_type`: what is certified / deprecated |
| `GET /governance/changes` | R | `status`, `type`, `subject_type`, `subject_id`, `requester=me|<id>`, `reviewer=me`, paginated; you see changes whose subject you can see, and your own |
| `POST /governance/changes` | C (in ws of the subject; org for org-level) | `{"type","subject_type","subject_id","payload","reviewers"?: [user refs],"comment"?}` -> `201` pending; event `governance.review.requested` |
| `GET /governance/changes/{id}` | R | + `comments` |
| `POST /governance/changes/{id}/comments` | C | `{"text"}` |
| `POST /governance/changes/{id}/approve` | A (in ws / org) | `{"comment"?}`: approves **and applies**; refused for the requester unless `governance.allow_self_approval` (`403 policy_denied`) |
| `POST /governance/changes/{id}/reject` | A | `{"comment"}` required |
| `POST /governance/changes/{id}/withdraw` | the requester | |
| `POST /governance/changes/{id}/apply` | A | re-applies an approved change whose application failed; applying is idempotent |

Change types and payloads (`subject_type` in brackets):

* `certify` (asset|product|term): `{"state": "certified"}` (default).
* `deprecate` (asset|product|term): `{"reason"?}`; certification becomes `deprecated` (a product also `status: deprecated`, a term `status: deprecated`).
* `classification_change` (asset): `{"add": ["<class id|name>", ...], "remove": [...]}`.
* `term_link` (asset|product): `{"term_id", "action": "link|unlink"}`.
* `ownership_change` (asset|domain|product|term): `{"add": [owner,...], "remove": [owner id|{"role","user"|"email"}...]}`.
* `policy_exception` (violation, `subject_id` = `vio_`): `{"reason", "expires_at"?: ISO time}`; applying marks the violation `excepted`.

Change response: `{"id","type","subject_type","subject_id","subject": AssetRef|{...}|null,"workspace_id","status":"pending|approved|rejected|withdrawn","requester": {"id","name"},"payload",
"reviewers":[...],"comments":[{"at","by","by_name","text","action": "created|comment|approve|reject|withdraw"}],"decided_by","decided_at","decision_comment","applied_at","apply_result","etag","created_at","updated_at"}`.

When the schema of a **certified** asset changes after it was certified (compared with the schema fingerprint stored at certification time; checked after scans, on
`dataset.schema_changed` and periodically), its certification is dropped to `draft` with `invalidated_reason`, an audit record `governance.certification_invalidated` and the event
`governance.certification.changed` (`state: "draft"`, `previous: "certified"`, `reason`: a sentence such as "the schema changed after the asset was certified"). A data product is dropped to `draft` the same way when its member set changes. An unchanged asset, or one whose schema was never known, keeps its certification.

## 6. Policies and violations (observe mode only)

| Route | Perm | Notes |
|---|---|---|
| `GET /governance/policies/rule-types` | R | the closed set of rule types with parameter documentation |
| `GET /governance/policies` | R | `rule_type`, `enabled`; items carry `open_violations`, `last_evaluated_at` |
| `POST /governance/policies` | A org (organisation-wide `governance.approve`: WorkspaceAdmin / SecurityAdmin bound at the organisation; workspace approvers cannot) | `{"name","description"?,"rule_type","params","severity"?: "medium","mode"?: "observe","enabled"?: true}`; `mode: "enforce"` -> `422 unsupported` |
| `GET /governance/policies/{id}`; `PATCH`; `DELETE` | R; A org | `If-Match`; delete removes its violations |
| `POST /governance/policies/{id}/evaluate` | A org | evaluates now -> `{"policy_id","evaluated","opened","updated","resolved","reopened","unknown","truncated","duration_ms"}` |
| `POST /governance/policies/evaluate` | A org | all enabled policies; `{"results": [...]}` |
| `POST /governance/policies/preview` | A org | `{"rule_type","params","severity"?, "limit"?}`: what **would** be reported; stores nothing |
| `GET /governance/violations` | R | `policy_id`, `asset_id`, `severity`, `status` (default `open,acknowledged`; `any`), `rule_type`, `workspace_id`; visible assets only |
| `GET /governance/violations/{id}` | R | + `policy`, `asset`, `evidence` |
| `POST /governance/violations/{id}/acknowledge` | C in ws | `{"comment"?}`; stays reported, but is not "open" |
| `POST /governance/violations/{id}/reopen` | C in ws | `acknowledged|excepted -> open` |

Rule types and `params` (all validated when the policy is saved):

* `classification_flow`: `{"classification": "<id|name>", "target": {selector}, "allowed_labels"?: ["Restricted"], "masking_tag"?: "masked", "include_possible"?: false}`. Data labelled
  `classification` must not reach assets matching `target` unless the target itself carries an approved label in `allowed_labels`, or the target or an asset on the path carries
  the tag `masking_tag` ("masking noted"). Only **proven** exposures count unless `include_possible`. Evidence = the lineage path.
* `required_owner`: `{"types"?: ["table","view","semantic_model","report","dashboard"], "roles"?: ["owner"], "selector"?: {...}}`; effective (inherited) owners count.
* `required_description`: `{"types"?: [...], "min_length"?: 10, "selector"?: {...}}`.
* `required_classification`: `{"name_patterns": ["*email*","ssn"], "allowed_labels"?: [...], "selector"?: {...}}`: columns whose name matches (glob, case-insensitive) must carry an approved (direct or inherited) classification.
* `certified_sources_only`: `{"target_types"?: ["report","dashboard"], "include_products"?: true, "source_types"?: ["table","view","model_table","semantic_model"], "depth"?: 5, "selector"?: {...}}`: walks upstream from the target and reports it when the first source on a path is not certified (itself, or through a certified data product). Lineage weaker than `governance.propagation_min_confidence` is not followed, so an *inferred* link produces no finding by default.
* `freshness`: `{"max_age_hours": number, "types"?: ["table"], "selector"?: {...}}`: assets whose newest known write (Delta last-modified or observed write) is older than the limit; assets with no known timestamp are counted as `unknown`, never reported.
* `quality_min`: `{"min_score": 0..100, "types"?: ["table","view"], "include_products"?: false, "selector"?: {...}}`: the quality score ignores violations of `quality_min` policies (a policy cannot lower the score it measures); an asset without a score is `unknown`, never reported.

Selector: `{"types"?: [..], "workspace_ids"?: [..], "name_pattern"?: "glob", "tags"?: [..], "domain"?: "<id|name>", "outside_domain"?: "<id|name>"}` (all given keys must match).

Violation: `{"id","policy_id","policy": {"id","name","rule_type"},"subject_type":"asset|product","asset": AssetRef|null,"product": ProductRef|null,"workspace_id","severity","message","evidence",
"status":"open|acknowledged|excepted|resolved","first_seen_at","last_seen_at","resolved_at","acknowledged_by","acknowledged_at","exception":{"until","change_id","reason"}|null,"etag"}`.
Evaluation is idempotent: a violation that is still true keeps its id and `first_seen_at` and moves `last_seen_at`; one that is no longer true becomes `resolved`; one that
returns is `open` again; an `excepted` violation stays excepted until the exception expires.

## 7. Quality

* `GET /governance/assets/{id}/quality` (R) -> `{"asset_id","type","own_score","score","components": [{"name","weight","applicable","fraction","points","max_points","reasons": [{"points_lost","reason"}]}],
  "rollup": {"columns": {"count","mean","sampled"},"own_score","column_weight"}|null,"weights","computed_at"}`; an asset type quality does not apply to (a lakehouse, a workspace ...) returns `score: null` and a `note`.
  Components: `documentation`, `ownership`, `classification`, `freshness`, `certification`, `lineage`, `violations`, `schema_stability`. A component that does not apply to the asset type
  (or has no data, such as freshness without a timestamp) is `applicable: false` and its weight is removed from the normalisation, so the `points` of the applicable components add up to
  `own_score` (to within rounding). `score` is `own_score` blended with the mean score of the table's columns (`rollup.columns.mean`, weight `governance.quality_column_rollup_weight`;
  `sampled: true` when a table has more than 300 columns and only some were scored). Violations of `quality_min` policies never lower a score. Weights are the settings
  `governance.quality_weight.<component>` (0 removes a component). With every weight at 0 the score is `null`, not 0.
* `GET /governance/products/{id}/quality` (R) -> `{"product_id","score","own_score","product_components": [...],"member_mean","member_weight","members_scored","members_total",
  "members": [{"asset": AssetRef,"score"}],"hidden_members","weights","computed_at"}`. A product's own score uses documentation, ownership and certification; `score` blends it with the mean score of
  all its members (`governance.quality_product_member_weight`); members the caller cannot see are counted in `hidden_members` but not named and are included in the mean.

## 8. Overview and per-asset view

* `GET /governance/summary` (R; `workspace_id` optional) -> `{"glossary": {"terms", "by_status"}, "domains", "products": {"total","by_status"}, "classifications": {"labels", "assignments": {"approved","suggested","rejected"}, "by_label": {...}},
  "review_queue": {"total","classification","term_link","change_requests"}, "violations": {"open","by_severity","by_status"}, "certification": {"by_state"}, "coverage": {"datasets","owner_pct","description_pct","domain_pct","term_pct","classified_pct","classification_reviewed_pct","datasets_with_pending_suggestions","certified_pct","sampled"},
  "top_unowned": [AssetRef], "top_unclassified": [AssetRef + "pending_suggestions"], "evaluation": {"last_evaluated_at"}, "health": {...}}`.
  Coverage counts the visible datasets (tables, views, model tables, semantic models; at most 20,000, `sampled: true` beyond that). `owner_pct`: an effective `owner`; `description_pct`: at least 10 characters;
  `classified_pct`: an approved label on the dataset or on one of its columns; `classification_reviewed_pct`: classified **and** no suggestion waiting for review. A dataset nobody has classified is
  *not* counted as reviewed. Percentages are `null` when there are no datasets.
* `GET /governance/assets/{id}` (R) -> everything governance knows: `{"asset": AssetRef, "domain": {"domain": DomainRef, "source": "direct|inherited|product", "inherited_from"}|null, "domains": [...], "products": [ProductRef],
  "terms": [TermRef + link], "pending_term_links": [...], "owners": {"direct","effective"}, "classifications": {"direct","inherited","suggestions","contained"},
  "certification": Certification, "quality": {"score","own_score"}, "violations": [Violation], "open_changes": [Change]}`. `domains` lists every domain that applies (the effective one first, then
  those reached only through a data product, `source: "product"`).
* `GET /governance/assets/{id}/graph` (R) -> virtual governance edges, never stored in the lineage edge table: `{"nodes": [{"id","type": "asset|domain|data_product|glossary_term|classification","name"}], "edges": [{"from","to","type":
  "GOVERNED_BY|CLASSIFIED_AS","virtual": true,"attrs": {...}}]}`. `GOVERNED_BY` goes to domains (`attrs.via`: `direct|inherited|product`), data products and glossary terms; `CLASSIFIED_AS` to classifications (`attrs`: source, status, precision).

Hooks served to the Intelligence core (not REST): `decorate`, `owners_of`, `asset_filter(domain, classification, certification, owner)` (so `GET /intelligence/assets?domain=&classification=&owner=&certification=`
works), `decorate_compact`, `governance_info`, and a merge hook that re-points all governance rows when two assets merge. The asset `GET /intelligence/assets/{id}` gains `governance`: `{"domain","certification","classifications","owners","glossary_terms","data_product","violations"}`.

**List cards.** `GET /intelligence/assets` embeds, in every item of a page, a compact `governance` object so catalog lists need no per-asset call: `{"classifications": [{"name","level","inherited"}]
(highest level first, approved direct labels plus proven lineage labels), "certification": "none|draft|certified|deprecated", "domain": {"id","name"}|null, "quality_score": number|null (the figure
`GET /governance/assets/{id}/quality` reports), "owners": int (effective owners, inherited included)}`. It is computed once per page (a fixed number of queries; `decorate_compact` of the
governance service, reached through `IntelligenceService.governance_compact`) and only for assets the caller can see; if governance is not installed or the computation fails the
`governance` key is simply absent.

**Governance filters and the id cap.** `domain`, `classification`, `certification` and `owner` filters ask governance for the matching asset ids. When the list is long the core first intersects it with
the ids that pass the caller's scope and the other filters (an id-only query) and then queries in chunks, so matches are no longer lost on large estates. At most 20 000 ids (`FILTER_ID_CAP`
in `olcf.intelligence.service`) are carried into one query; if more match, the response has `"truncated_filter": true` (the key is absent otherwise) and narrow filters (another filter, `q`, `type`)
give the complete answer. The same cap applies to `search`, which does not report the flag.

## 9. Settings (`olcf settings`, `PUT /v1/admin/settings/{key}`)

`governance.allow_self_approval` (false), `governance.classification_four_eyes` (false), `governance.auto_classify_on_scan` (true), `governance.suggest_term_links` (true),
`governance.evaluate_policies_on_scan` (true), `governance.suggestion_min_confidence` (0.5), `governance.propagation_depth` (6), `governance.propagation_max_nodes` (500), `governance.propagation_min_confidence` (`derived`),
`governance.freshness_hours` (24), `governance.schema_stability_days` (30), `governance.quality_weight.{documentation,ownership,classification,freshness,certification,lineage,violations,schema_stability}`,
`governance.quality_column_rollup_weight` (0.3), `governance.quality_product_member_weight` (0.7), `governance.tick_seconds` (30), `governance.max_evaluation_assets` (20000),
`governance.violation_retention_days` (365).

## 10. Audit actions and events

Audit: `governance.term_created|updated|deleted|linked|unlinked`, `governance.domain_created|updated|deleted|assigned`, `governance.product_created|updated|deleted|members_changed`,
`governance.owner_added|removed`, `governance.classification_created|updated|deleted|assigned|removed|reviewed|scanned`, `governance.certification_changed|certification_invalidated`,
`governance.change_requested|commented|decided|withdrawn|applied`, `governance.policy_created|updated|deleted|evaluated`, `governance.violation_updated`.

Events (webhooks): `governance.review.requested`, `governance.review.decided`, `governance.violation.opened`, `governance.violation.resolved`, `governance.certification.changed`,
`governance.classification.changed`, `governance.change.requested`, `governance.change.decided`, `governance.policy.evaluated`.
