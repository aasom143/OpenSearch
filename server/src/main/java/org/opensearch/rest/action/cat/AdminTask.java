/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.cat;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.CancellableTask;

import java.util.Map;

import static org.opensearch.search.SearchService.NO_TIMEOUT;
@PublicApi(since = "1.0.0")
public class AdminTask extends CancellableTask {

    public AdminTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        this(id, type, action, parentTaskId, headers, NO_TIMEOUT);
    }

    public AdminTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers, TimeValue cancelAfterTimeInterval) {
        super(id, type, action, null, parentTaskId, headers, cancelAfterTimeInterval);
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }
}
