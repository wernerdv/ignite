/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Exchange task to handle node leave for WAL state manager.
 */
public class WalStateNodeLeaveExchangeTask implements CachePartitionExchangeWorkerTask {
    /** Security context in which current task must be executed. */
    private final @Nullable SecurityContext secCtx;

    /** Node that has left the grid. */
    private final ClusterNode node;

    /**
     * Constructor.
     *
     * @param secCtx Security context in which current task must be executed.
     * @param node Node that has left the grid.
     */
    public WalStateNodeLeaveExchangeTask(@Nullable SecurityContext secCtx, ClusterNode node) {
        assert node != null;

        this.secCtx = secCtx;
        this.node = node;
    }

    /**
     * @return Node that has left the grid.
     */
    public ClusterNode node() {
        return node;
    }

    /** {@inheritDoc} */
    @Override public boolean skipForExchangeMerge() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public @Nullable SecurityContext securityContext() {
        return secCtx;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WalStateNodeLeaveExchangeTask.class, this);
    }
}
