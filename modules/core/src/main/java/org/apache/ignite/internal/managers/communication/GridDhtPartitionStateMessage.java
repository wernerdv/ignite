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

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Message for {@link GridDhtPartitionState}.
 * Consistency between code-to-value and value-to-code conversions must be provided.
 */
public class GridDhtPartitionStateMessage implements Message {
    /** Type code. */
    public static final short TYPE_CODE = 515;

    /** Partition state. */
    @Nullable private GridDhtPartitionState state;

    /** Code of partition state. */
    @Order(0)
    private byte code = -1;

    /** Constructor. */
    public GridDhtPartitionStateMessage() {
        // No-op.
    }

    /** Constructor. */
    public GridDhtPartitionStateMessage(@Nullable GridDhtPartitionState state) {
        this.state = state;
        code = encode(state);
    }

    /** @param state Partition state to encode. */
    private static byte encode(@Nullable GridDhtPartitionState state) {
        if (state == null)
            return -1;

        switch (state) {
            case MOVING: return 0;
            case OWNING: return 1;
            case RENTING: return 2;
            case EVICTED: return 3;
            case LOST: return 4;
        }

        throw new IllegalArgumentException("Unknown partition state: " + state);
    }

    /** @param code Code of partition state to decode. */
    @Nullable private static GridDhtPartitionState decode(short code) {
        switch (code) {
            case -1: return null;
            case 0: return GridDhtPartitionState.MOVING;
            case 1: return GridDhtPartitionState.OWNING;
            case 2: return GridDhtPartitionState.RENTING;
            case 3: return GridDhtPartitionState.EVICTED;
            case 4: return GridDhtPartitionState.LOST;
        }

        throw new IllegalArgumentException("Unknown partition state code: " + code);
    }

    /** @param code Code of partition state. */
    public void code(byte code) {
        this.code = code;
        state = decode(code);
    }

    /** @return Code of partition state. */
    public byte code() {
        return code;
    }

    /** @return Partition state value. */
    public GridDhtPartitionState value() {
        return state;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
