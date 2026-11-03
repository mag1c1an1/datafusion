<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache DataFusion Physical Plan

[Apache DataFusion] is an extensible query execution framework, written in Rust, that uses [Apache Arrow] as its in-memory format.

This crate is a submodule of DataFusion that contains the `ExecutionPlan` trait and the various implementations of that
trait for built in operators such as filters, projections, joins, aggregations, etc.

Most projects should use the [`datafusion`] crate directly, which re-exports
this module. If you are already using the [`datafusion`] crate, there is no
reason to use this crate directly in your project as well.

## LakeSoul 55.2.0 patch branch

The `feature/lakesoul-physical-plan-55.2` branch is based on the upstream
`55.2.0` tag (`500744d5c25499055e3b2ca46b022bc425989203`). It carries:

- The spill-pool deadlock fix from apache/datafusion#24891
  (`09a2aff72d88c16499ae3266a943a4cd9841b618`), including its regression tests.
- `ScalarSubqueryCardinalityError`, returned for multi-row scalar subqueries so
  LakeSoul's PostgreSQL wire layer can map them to SQLSTATE `21000` without
  matching error text. Regression coverage includes multiple batches and
  multiple partitions.

This branch is intended for a single-crate `[patch.crates-io]` pinned to an
immutable Git revision, not for replacing the whole DataFusion workspace.
The crate manifest deliberately uses the published 55.2.0 registry dependency
layout, rather than workspace/path dependencies, so the consuming application
keeps one shared set of DataFusion types from crates.io.

The source changes are identical to LakeSoul's former vendored patch.
Keep the consuming application's lockfile when validating this branch; this
patch migration does not require upgrading Arrow, Tokio, or other dependencies.
The spill fix can be dropped once the consuming DataFusion release includes it.
Retain the typed cardinality error until the protocol mapping has migrated to
an equivalent upstream error classification.

[apache arrow]: https://arrow.apache.org/
[apache datafusion]: https://datafusion.apache.org/
[`datafusion`]: https://crates.io/crates/datafusion
