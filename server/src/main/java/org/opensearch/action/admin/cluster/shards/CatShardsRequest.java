/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.rest.action.admin.cluster.ClusterAdminTask;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.search.SearchService.NO_TIMEOUT;

/**
 * A request of _cat/shards.
 *
 * @opensearch.api
 */
public class CatShardsRequest extends ClusterManagerNodeReadRequest<CatShardsRequest> {

    private String[] indices;
    private TimeValue cancelAfterTimeInterval = NO_TIMEOUT;

    public CatShardsRequest() {}

    public CatShardsRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public void setClusterManagerNodeTimeout(TimeValue timeout) {
        this.clusterManagerNodeTimeout = timeout;
    }

    public TimeValue getClusterManagerNodeTimeout() {
        return this.clusterManagerNodeTimeout;
    }

    public void setLocal(boolean local) {
        this.local = local;
    }

    public boolean getLocal() {
        return this.local;
    }

    public void setIndices(String[] indices) {
        this.indices = indices;
    }

    public String[] getIndices() {
        return this.indices;
    }

    public void setCancelAfterTimeInterval(TimeValue timeout) {
        this.cancelAfterTimeInterval = timeout;
    }

    @Override
    public ClusterAdminTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new ClusterAdminTask(id, type, action, parentTaskId, headers, this.cancelAfterTimeInterval);
    }
}