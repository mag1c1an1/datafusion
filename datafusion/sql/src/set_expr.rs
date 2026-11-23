// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashSet;
use std::sync::Arc;

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use arrow::datatypes::DataType;
use datafusion_common::{
    Column, DataFusionError, Diagnostic, NullEquality, Result, Span, internal_err,
    not_impl_err, plan_datafusion_err, plan_err,
};
use datafusion_expr::logical_plan::requalify_sides_if_needed;
use datafusion_expr::type_coercion::binary::comparison_coercion;
use datafusion_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder, cast, lit, when};
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_functions_nested::range::range_udf;
use sqlparser::ast::{SetExpr, SetOperator, SetQuantifier, Spanned};

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(super) fn set_expr_to_plan(
        &self,
        set_expr: SetExpr,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        crate::stack::maybe_grow(|| {
            let set_expr_span = Span::try_from_sqlparser_span(set_expr.span());
            match set_expr {
                SetExpr::Select(s) => self.select_to_plan(*s, None, planner_context),
                SetExpr::Values(v) => self.sql_values_to_plan(v, planner_context),
                SetExpr::SetOperation {
                    op,
                    left,
                    right,
                    set_quantifier,
                } => {
                    let left_span = Span::try_from_sqlparser_span(left.span());
                    let right_span = Span::try_from_sqlparser_span(right.span());
                    let left_plan = self.set_expr_to_plan(*left, planner_context);
                    // Store the left plan's schema so that the right side can
                    // alias duplicate expressions to match. Skip for BY NAME
                    // operations since those match columns by name, not position.
                    if let Ok(plan) = &left_plan
                        && plan.schema().fields().len() > 1
                        && !matches!(
                            set_quantifier,
                            SetQuantifier::ByName
                                | SetQuantifier::AllByName
                                | SetQuantifier::DistinctByName
                        )
                    {
                        planner_context
                            .set_set_expr_left_schema(Some(Arc::clone(plan.schema())));
                    }
                    let right_plan = self.set_expr_to_plan(*right, planner_context);
                    planner_context.set_set_expr_left_schema(None);
                    let (left_plan, right_plan) = match (left_plan, right_plan) {
                        (Ok(left_plan), Ok(right_plan)) => (left_plan, right_plan),
                        (Err(left_err), Err(right_err)) => {
                            return Err(DataFusionError::Collection(vec![
                                left_err, right_err,
                            ]));
                        }
                        (Err(err), _) | (_, Err(err)) => {
                            return Err(err);
                        }
                    };
                    if !(set_quantifier == SetQuantifier::ByName
                        || set_quantifier == SetQuantifier::AllByName)
                    {
                        self.validate_set_expr_num_of_columns(
                            op,
                            left_span,
                            right_span,
                            &left_plan,
                            &right_plan,
                            set_expr_span,
                        )?;
                    }
                    self.set_operation_to_plan(op, left_plan, right_plan, set_quantifier)
                }
                SetExpr::Query(q) => self.query_to_plan(*q, planner_context),
                _ => not_impl_err!("Query {set_expr} not implemented yet"),
            }
        })
    }

    pub(super) fn is_union_all(set_quantifier: SetQuantifier) -> Result<bool> {
        match set_quantifier {
            SetQuantifier::All | SetQuantifier::AllByName => Ok(true),
            SetQuantifier::Distinct
            | SetQuantifier::ByName
            | SetQuantifier::DistinctByName
            | SetQuantifier::None => Ok(false),
        }
    }

    fn validate_set_expr_num_of_columns(
        &self,
        op: SetOperator,
        left_span: Option<Span>,
        right_span: Option<Span>,
        left_plan: &LogicalPlan,
        right_plan: &LogicalPlan,
        set_expr_span: Option<Span>,
    ) -> Result<()> {
        if left_plan.schema().fields().len() == right_plan.schema().fields().len() {
            return Ok(());
        }
        let diagnostic = Diagnostic::new_error(
            format!("{op} queries have different number of columns"),
            set_expr_span,
        )
        .with_note(
            format!("this side has {} fields", left_plan.schema().fields().len()),
            left_span,
        )
        .with_note(
            format!(
                "this side has {} fields",
                right_plan.schema().fields().len()
            ),
            right_span,
        );
        plan_err!("{} queries have different number of columns", op; diagnostic =diagnostic)
    }

    pub(super) fn set_operation_to_plan(
        &self,
        op: SetOperator,
        left_plan: LogicalPlan,
        right_plan: LogicalPlan,
        set_quantifier: SetQuantifier,
    ) -> Result<LogicalPlan> {
        match (op, set_quantifier) {
            (SetOperator::Union, SetQuantifier::All) => {
                LogicalPlanBuilder::from(left_plan)
                    .union(right_plan)?
                    .build()
            }
            (SetOperator::Union, SetQuantifier::AllByName) => {
                LogicalPlanBuilder::from(left_plan)
                    .union_by_name(right_plan)?
                    .build()
            }
            (SetOperator::Union, SetQuantifier::Distinct | SetQuantifier::None) => {
                LogicalPlanBuilder::from(left_plan)
                    .union_distinct(right_plan)?
                    .build()
            }
            (
                SetOperator::Union,
                SetQuantifier::ByName | SetQuantifier::DistinctByName,
            ) => LogicalPlanBuilder::from(left_plan)
                .union_by_name_distinct(right_plan)?
                .build(),
            (SetOperator::Intersect, SetQuantifier::All) => plan_intersect_or_except_all(
                left_plan,
                right_plan,
                SetOperator::Intersect,
            ),
            (SetOperator::Intersect, SetQuantifier::Distinct | SetQuantifier::None) => {
                LogicalPlanBuilder::intersect(left_plan, right_plan, false)
            }
            (SetOperator::Except, SetQuantifier::All) => {
                plan_intersect_or_except_all(left_plan, right_plan, SetOperator::Except)
            }
            (SetOperator::Except, SetQuantifier::Distinct | SetQuantifier::None) => {
                LogicalPlanBuilder::except(left_plan, right_plan, false)
            }
            (op, quantifier) => {
                not_impl_err!("{op} {quantifier} not implemented")
            }
        }
    }
}

/// Name of the per-row count of the left input in `INTERSECT ALL` / `EXCEPT ALL`
/// plans. The names match apache/datafusion#25767.
const SET_OPERATION_LEFT_COUNT: &str = "__datafusion_set_operation_left_count";
/// Name of the per-row count of the right input.
const SET_OPERATION_RIGHT_COUNT: &str = "__datafusion_set_operation_right_count";
/// Name of the list of copy ordinals that is unnested to repeat each row.
const SET_OPERATION_COPIES: &str = "__datafusion_set_operation_copies";

/// Plans `INTERSECT ALL` / `EXCEPT ALL` with multiset semantics.
///
/// A row that occurs `m` times on the left and `n` times on the right is
/// returned `min(m, n)` times by `INTERSECT ALL` and `max(m - n, 0)` times by
/// `EXCEPT ALL` (SQL:1992, PostgreSQL). The plan:
///
/// 1. casts both inputs to the common comparison type of each column,
/// 2. counts every distinct row of each input (`GROUP BY` all columns),
/// 3. joins the counts with `NULL`-safe equality (inner join for
///    `INTERSECT ALL`, left join for `EXCEPT ALL`),
/// 4. computes the number of copies to keep, and
/// 5. expands each row back into that many copies with `unnest(range(n))`.
///
/// Grouping (rather than numbering rows within partitions) compares values the
/// same way `GROUP BY` and the join do, so `0.0` and `-0.0`, or values that
/// are only equal after coercion, count as the same row.
///
/// `count` and `range` are the built-in implementations rather than functions
/// resolved through the [`ContextProvider`], so user-registered functions with
/// the same names cannot change the result.
fn plan_intersect_or_except_all(
    left_plan: LogicalPlan,
    right_plan: LogicalPlan,
    op: SetOperator,
) -> Result<LogicalPlan> {
    let left_len = left_plan.schema().fields().len();
    let right_len = right_plan.schema().fields().len();
    if left_len != right_len {
        return plan_err!(
            "INTERSECT/EXCEPT query must have the same number of columns. Left is {left_len} and right is {right_len}."
        );
    }

    // Requalify sides if needed to avoid duplicate qualified field names
    // (e.g., when both sides reference the same table)
    let (left, right, _requalified) = requalify_sides_if_needed(
        LogicalPlanBuilder::from(left_plan),
        LogicalPlanBuilder::from(right_plan),
    )?;

    let common_types = left
        .schema()
        .iter()
        .zip(right.schema().iter())
        .map(|((_, left_field), (_, right_field))| {
            let (left_type, right_type) = (left_field.data_type(), right_field.data_type());
            if left_type == right_type {
                return Ok(left_type.clone());
            }
            comparison_coercion(left_type, right_type).ok_or_else(|| {
                plan_datafusion_err!(
                    "{op} ALL cannot compare column {} of type {left_type} with column {} of type {right_type}",
                    left_field.name(),
                    right_field.name()
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut used_names: HashSet<String> = left
        .schema()
        .fields()
        .iter()
        .chain(right.schema().fields().iter())
        .map(|field| field.name().clone())
        .collect();
    let left_count = unique_name(SET_OPERATION_LEFT_COUNT, &mut used_names);
    let right_count = unique_name(SET_OPERATION_RIGHT_COUNT, &mut used_names);
    let copies = unique_name(SET_OPERATION_COPIES, &mut used_names);

    let (left, left_columns) = count_distinct_rows(left, &common_types, &left_count)?;
    let (right, right_columns) = count_distinct_rows(right, &common_types, &right_count)?;

    let left_count = Expr::Column(Column::from_name(left_count));
    let right_count = Expr::Column(Column::from_name(right_count));
    let join_type = match op {
        SetOperator::Intersect => JoinType::Inner,
        SetOperator::Except => JoinType::Left,
        _ => return internal_err!("{op} ALL is not planned with grouped counts"),
    };
    let joined = left.join_detailed(
        right.build()?,
        join_type,
        (left_columns.clone(), right_columns),
        None,
        NullEquality::NullEqualsNull,
    )?;

    let (joined, copy_count) = match op {
        // min(m, n); the inner join guarantees both counts are at least 1.
        SetOperator::Intersect => (
            joined,
            when(right_count.clone().lt(left_count.clone()), right_count)
                .otherwise(left_count)?,
        ),
        // max(m - n, 0); `n` is NULL when the row is absent on the right.
        _ => (
            joined.filter(
                right_count
                    .clone()
                    .is_null()
                    .or(left_count.clone().gt(right_count.clone())),
            )?,
            when(right_count.clone().is_null(), left_count.clone())
                .otherwise(left_count - right_count)?,
        ),
    };

    let output_columns = left_columns.into_iter().map(Expr::Column);
    joined
        .project(output_columns.clone().chain(std::iter::once(
            range_udf().call(vec![copy_count]).alias(&copies),
        )))?
        .unnest_column(Column::from_name(copies))?
        .project(output_columns)?
        .build()
}

/// Casts the columns of `input` to `types` (keeping their qualified names) and
/// counts how often each distinct row occurs, as a column named `count_name`.
///
/// Returns the plan and its grouping columns.
fn count_distinct_rows(
    input: LogicalPlanBuilder,
    types: &[DataType],
    count_name: &str,
) -> Result<(LogicalPlanBuilder, Vec<Column>)> {
    let schema = Arc::clone(input.schema());
    let needs_cast = schema
        .fields()
        .iter()
        .zip(types)
        .any(|(field, data_type)| field.data_type() != data_type);
    let input = if needs_cast {
        input.project(schema.iter().zip(types).map(
            |((qualifier, field), data_type)| {
                let column = Expr::Column(Column::new(qualifier.cloned(), field.name()));
                if field.data_type() == data_type {
                    column
                } else {
                    cast(column, data_type.clone())
                        .alias_qualified(qualifier.cloned(), field.name())
                }
            },
        ))?
    } else {
        input
    };
    let group_by = input.schema().columns();
    let counted = input.aggregate(
        group_by.iter().cloned().map(Expr::Column),
        vec![count_udaf().call(vec![lit(1_i64)]).alias(count_name)],
    )?;
    Ok((counted, group_by))
}

/// Returns `base`, or `base` with the first numeric suffix that is not in
/// `used`, and records the result in `used`.
fn unique_name(base: &str, used: &mut HashSet<String>) -> String {
    let mut name = base.to_string();
    let mut suffix = 1;
    while used.contains(&name) {
        name = format!("{base}_{suffix}");
        suffix += 1;
    }
    used.insert(name.clone());
    name
}
