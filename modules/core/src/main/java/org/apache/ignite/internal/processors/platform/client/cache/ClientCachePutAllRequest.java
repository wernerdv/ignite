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

package org.apache.ignite.internal.processors.platform.client.cache;

import java.util.Map;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryReaderEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.tx.ClientTxAwareRequest;

/**
 * PutAll request.
 */
public class ClientCachePutAllRequest extends ClientCacheDataRequest implements ClientTxAwareRequest {
    /** Map. */
    private final Map<Object, Object> map;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientCachePutAllRequest(BinaryReaderEx reader) {
        super(reader);

        int cnt = reader.readInt();

        Object[] keys = new Object[cnt];
        Object[] vals = new Object[cnt];

        for (int i = 0; i < cnt; i++) {
            keys[i] = reader.readObjectDetached();
            vals[i] = reader.readObjectDetached();
        }

        map = new ImmutableArrayMap<>(keys, vals);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        cache(ctx).putAll(map);

        return super.process(ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync(ClientConnectionContext ctx) {
        // Every cache data request on the transactional cache can lock the thread, even with implicit transaction.
        return cacheDescriptor(ctx).cacheConfiguration().getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<ClientResponse> processAsync(ClientConnectionContext ctx) {
        return chainFuture(cache(ctx).putAllAsync(map), v -> new ClientResponse(requestId()));
    }
}
