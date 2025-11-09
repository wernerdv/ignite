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

import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.LOST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.RENTING;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** */
public class GridDhtPartitionStateMessageTest {
    /** */
    @Test
    public void testPartitionStateCode() {
        assertEquals(-1, new GridDhtPartitionStateMessage(null).code());
        assertEquals(0, new GridDhtPartitionStateMessage(MOVING).code());
        assertEquals(1, new GridDhtPartitionStateMessage(OWNING).code());
        assertEquals(2, new GridDhtPartitionStateMessage(RENTING).code());
        assertEquals(3, new GridDhtPartitionStateMessage(EVICTED).code());
        assertEquals(4, new GridDhtPartitionStateMessage(LOST).code());

        for (GridDhtPartitionState state : GridDhtPartitionState.values()) {
            assertTrue(new GridDhtPartitionStateMessage(state).code() >= 0);
            assertTrue(new GridDhtPartitionStateMessage(state).code() < 5);
        }
    }

    /** */
    @Test
    public void testPartitionStateFromCode() {
        GridDhtPartitionStateMessage msg = new GridDhtPartitionStateMessage(null);

        msg.code((byte)-1);
        assertNull(msg.value());

        msg.code((byte)0);
        assertSame(MOVING, msg.value());

        msg.code((byte)1);
        assertSame(OWNING, msg.value());

        msg.code((byte)2);
        assertSame(RENTING, msg.value());

        msg.code((byte)3);
        assertSame(EVICTED, msg.value());

        msg.code((byte)4);
        assertSame(LOST, msg.value());

        Throwable t = assertThrowsWithCause(() -> msg.code((byte)5), IllegalArgumentException.class);
        assertEquals("Unknown partition state code: 5", t.getMessage());
    }

    /** */
    @Test
    public void testConversionConsistency() {
        for (GridDhtPartitionState state : F.concat(GridDhtPartitionState.values(), (GridDhtPartitionState)null)) {
            GridDhtPartitionStateMessage msg = new GridDhtPartitionStateMessage(state);

            assertEquals(state, msg.value());

            GridDhtPartitionStateMessage newMsg = new GridDhtPartitionStateMessage();
            newMsg.code(msg.code());

            assertEquals(msg.value(), newMsg.value());
        }
    }
}
