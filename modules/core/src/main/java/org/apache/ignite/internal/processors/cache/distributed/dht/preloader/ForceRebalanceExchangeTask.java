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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import org.apache.ignite.internal.processors.cache.CachePartitionExchangeWorkerTask;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class ForceRebalanceExchangeTask implements CachePartitionExchangeWorkerTask {
    /** Security context in which current task must be executed. */
    private final @Nullable SecurityContext secCtx;

    /** */
    private final GridDhtPartitionExchangeId exchId;

    /** */
    private final GridCompoundFuture<Boolean, Boolean> forcedRebFut;

    /**
     * @param secCtx Security context in which current task must be executed.
     * @param exchId Exchange ID.
     * @param forcedRebFut Rebalance future.
     */
    public ForceRebalanceExchangeTask(
        @Nullable SecurityContext secCtx,
        GridDhtPartitionExchangeId exchId,
        GridCompoundFuture<Boolean, Boolean> forcedRebFut
    ) {
        assert exchId != null;
        assert forcedRebFut != null;

        this.secCtx = secCtx;
        this.exchId = exchId;
        this.forcedRebFut = forcedRebFut;
    }

    /** {@inheritDoc} */
    @Override public boolean skipForExchangeMerge() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public @Nullable SecurityContext securityContext() {
        return secCtx;
    }

    /**
     * @return Exchange ID.
     */
    public GridDhtPartitionExchangeId exchangeId() {
        return exchId;
    }

    /**
     * @return Rebalance future.
     */
    public GridCompoundFuture<Boolean, Boolean> forcedRebalanceFuture() {
        return forcedRebFut;
    }
}
