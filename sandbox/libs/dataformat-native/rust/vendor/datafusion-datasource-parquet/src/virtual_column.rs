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

//! Typed wrapper for parquet virtual columns.
//!
//! NOTE (OpenSearch forward-port): this is a backport of DataFusion 55's parquet
//! virtual-column support onto the pinned 54.0.0 crate. Upstream matches
//! `parquet::arrow::RowNumber::NAME`, but that extension type lives behind a
//! feature-gated module in parquet 58.3.0, so we match the extension-type *name*
//! string directly (`parquet.virtual.row_number`). Remove this file when the
//! workspace bumps to DataFusion 55.

use arrow::datatypes::FieldRef;
use datafusion_common::{not_impl_err, DataFusionError, Result};
use std::sync::Arc;

/// Arrow extension-type name for the parquet row-number virtual column
/// (`parquet::arrow::schema::virtual_type::RowNumber::NAME` in newer parquet).
pub(crate) const ROW_NUMBER_EXTENSION_NAME: &str = "parquet.virtual.row_number";

/// A parquet virtual column validated to have a supported arrow extension type.
///
/// Construct via [`TryFrom<&FieldRef>`]; add a new variant (and update the
/// `TryFrom` impl) when we gain support for another arrow-rs virtual extension
/// type.
#[derive(Debug, Clone)]
pub enum ParquetVirtualColumn {
    /// Absolute row number within the parquet file (arrow-rs `RowNumber`).
    RowNumber(FieldRef),
}

impl ParquetVirtualColumn {
    pub fn field(&self) -> &FieldRef {
        match self {
            Self::RowNumber(field) => field,
        }
    }
}

impl From<ParquetVirtualColumn> for FieldRef {
    fn from(col: ParquetVirtualColumn) -> Self {
        match col {
            ParquetVirtualColumn::RowNumber(field) => field,
        }
    }
}

impl TryFrom<&FieldRef> for ParquetVirtualColumn {
    type Error = DataFusionError;

    fn try_from(field: &FieldRef) -> Result<Self> {
        let Some(name) = field.extension_type_name() else {
            return not_impl_err!(
                "Virtual column '{}' is missing an Arrow extension type; \
                 supported extension types: [{}]",
                field.name(),
                ROW_NUMBER_EXTENSION_NAME
            );
        };
        match name {
            n if n == ROW_NUMBER_EXTENSION_NAME => Ok(Self::RowNumber(Arc::clone(field))),
            other => not_impl_err!(
                "Virtual column '{}' uses unsupported Arrow extension type '{}'; \
                 supported types: [{}].",
                field.name(),
                other,
                ROW_NUMBER_EXTENSION_NAME
            ),
        }
    }
}
