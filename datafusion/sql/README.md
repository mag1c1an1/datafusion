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

# Apache DataFusion SQL Query Planner

This crate provides a general purpose SQL query planner that can parse SQL and translate queries into logical
plans. Although this crate is used by the [Apache DataFusion] query engine, it was designed to be easily usable from any
project that requires a SQL query planner and does not make any assumptions about how the resulting logical plan
will be translated to a physical plan. For example, there is no concept of row-based versus columnar execution in the
logical plan.

Note that the [`datafusion`] crate re-exports this module. If you are already
using the [`datafusion`] crate in your project, there is no reason to use this
crate directly in your project as well.

[apache datafusion]: https://datafusion.apache.org/
[`datafusion`]: https://crates.io/crates/datafusion

## LakeSoul 55.2.0 patch branch

The `feature/lakesoul-55.2` branch is stacked on the LakeSoul
`datafusion-physical-plan` patch (`feature/lakesoul-physical-plan-55.2`), which
is based on the upstream `55.2.0` tag
(`500744d5c25499055e3b2ca46b022bc425989203`). One immutable revision of this
branch therefore patches both `datafusion-physical-plan` and `datafusion-sql`.
This crate carries:

- Multiset `INTERSECT ALL` / `EXCEPT ALL` (lakesoul-io/LakeSoul#1320,
  upstream apache/datafusion#12955 and apache/datafusion#12956). A row that
  occurs `m` times on the left and `n` times on the right is returned
  `min(m, n)` times by `INTERSECT ALL` and `max(m - n, 0)` times by
  `EXCEPT ALL`, as in PostgreSQL. The planner casts both inputs to the common
  column types, counts each distinct row per side, joins the counts with
  `NULL`-safe equality and expands each row with `unnest(range(copies))`,
  following apache/datafusion#25767. It uses the built-in `count` and `range`
  implementations, so user functions with the same names cannot change the
  result. Distinct `INTERSECT` / `EXCEPT` plans are unchanged.

Only SQL planning changes: `LogicalPlanBuilder::intersect` / `except` (used
by the DataFrame API and the Substrait consumer) keep the old membership
semantics, and Substrait export of the new plans fails on `Unnest`.

This branch is intended for single-crate `[patch.crates-io]` entries pinned to
an immutable Git revision, not for replacing the whole DataFusion workspace.
The crate manifest deliberately uses the published 55.2.0 registry dependency
layout, rather than workspace/path dependencies, so the consuming application
keeps one shared set of DataFusion types from crates.io. The only dependency
added to the published manifest is `datafusion-functions-aggregate`, which is
already part of every DataFusion dependency graph.

The patch can be dropped once the consuming DataFusion release plans
`INTERSECT ALL` / `EXCEPT ALL` with multiset semantics.

## Example Usage

See the [examples] directory for fully working examples.

Here is an example of producing a logical plan from a SQL string.

```rust,ignore
fn main() {
    let sql = "SELECT \
            c.id, c.first_name, c.last_name, \
            COUNT(*) as num_orders, \
            SUM(o.price) AS total_price, \
            SUM(o.price * s.sales_tax) AS state_tax \
        FROM customer c \
        JOIN state s ON c.state = s.id \
        JOIN orders o ON c.id = o.customer_id \
        WHERE o.price > 0 \
        AND c.last_name LIKE 'G%' \
        GROUP BY 1, 2, 3 \
        ORDER BY state_tax DESC";

    // parse the SQL
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    let statement = &ast[0];

    // create a logical query plan
    let context_provider = MyContextProvider::new();
    let sql_to_rel = SqlToRel::new(&context_provider);
    let plan = sql_to_rel.sql_statement_to_plan(statement.clone()).unwrap();

    // show the plan
    println!("{:?}", plan);
}
```

This is the logical plan that is produced from this example. Note that this is an **unoptimized**
logical plan. The [datafusion-optimizer] crate provides a query optimizer that can be applied to
plans produced by this crate.

```
Sort: state_tax DESC NULLS FIRST
  Projection: c.id, c.first_name, c.last_name, COUNT(Int64(1)) AS num_orders, SUM(o.price) AS total_price, SUM(o.price * s.sales_tax) AS state_tax
    Aggregate: groupBy=[[c.id, c.first_name, c.last_name]], aggr=[[COUNT(Int64(1)), SUM(o.price), SUM(o.price * s.sales_tax)]]
      Filter: o.price > Int64(0) AND c.last_name LIKE Utf8("G%")
        Inner Join: c.id = o.customer_id
          Inner Join: c.state = s.id
            SubqueryAlias: c
              TableScan: customer
            SubqueryAlias: s
              TableScan: state
          SubqueryAlias: o
            TableScan: orders
```

[examples]: examples
[datafusion-optimizer]: https://crates.io/crates/datafusion-optimizer
