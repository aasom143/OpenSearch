/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.iceberg.action;

import org.opensearch.action.ActionType;

/**
 * Action type for syncing an index to Iceberg catalog across all nodes.
 */
public class SyncIcebergAction extends ActionType<SyncIcebergResponse> {
    
    public static final SyncIcebergAction INSTANCE = new SyncIcebergAction();
    public static final String NAME = "indices:admin/iceberg/sync";
    
    private SyncIcebergAction() {
        super(NAME, SyncIcebergResponse::new);
    }
}
