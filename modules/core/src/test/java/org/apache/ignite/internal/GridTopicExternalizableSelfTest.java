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

package org.apache.ignite.internal;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.GridTestIoUtils;
import org.apache.ignite.IgniteExternalizableAbstractTest;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Grid topic externalization test.
 */
public class GridTopicExternalizableSelfTest extends IgniteExternalizableAbstractTest {
    /** */
    private static final IgniteUuid A_IGNITE_UUID = IgniteUuid.randomUuid();

    /** */
    private static final UUID AN_UUID = UUID.randomUUID();

    /** */
    private static final long A_LONG = Long.MAX_VALUE;

    /** */
    private static final String A_STRING = "test_test_test_test_test_test_test_test_test_test_test_test_test_test";

    /** */
    private static final int AN_INT = Integer.MAX_VALUE;

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSerializationTopicCreatedByIgniteUuid() throws Exception {
        for (Marshaller marsh : getMarshallers()) {
            info("Test GridTopic externalization [marshaller=" + marsh + ']');

            for (GridTopic topic : GridTopic.values()) {
                Externalizable msgOut = (Externalizable)topic.topic(A_IGNITE_UUID);

                assertEquals(msgOut, GridTestIoUtils.externalize(msgOut, marsh));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSerializationTopicCreatedByIgniteUuidAndUUID() throws Exception {
        for (Marshaller marsh : getMarshallers()) {
            info("Test GridTopic externalization [marshaller=" + marsh + ']');

            for (GridTopic topic : GridTopic.values()) {
                Externalizable msgOut = (Externalizable)topic.topic(A_IGNITE_UUID, AN_UUID);

                assertEquals(msgOut, GridTestIoUtils.externalize(msgOut, marsh));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSerializationTopicCreatedByIgniteUuidAndLong() throws Exception {
        for (Marshaller marsh : getMarshallers()) {
            info("Test GridTopic externalization [marshaller=" + marsh + ']');

            for (GridTopic topic : GridTopic.values()) {
                Externalizable msgOut = (Externalizable)topic.topic(A_IGNITE_UUID, A_LONG);

                assertEquals(msgOut, GridTestIoUtils.externalize(msgOut, marsh));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSerializationTopicCreatedByStringAndUUIDAndLong() throws Exception {
        for (Marshaller marsh : getMarshallers()) {
            info("Test GridTopic externalization [marshaller=" + marsh + ']');

            for (GridTopic topic : GridTopic.values()) {
                Externalizable msgOut = (Externalizable)topic.topic(A_STRING, AN_UUID, A_LONG);

                assertEquals(msgOut, GridTestIoUtils.externalize(msgOut, marsh));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSerializationTopicCreatedByString() throws Exception {
        for (Marshaller marsh : getMarshallers()) {
            info("Test GridTopic externalization [marshaller=" + marsh + ']');

            for (GridTopic topic : GridTopic.values()) {
                Externalizable msgOut = (Externalizable)topic.topic(A_STRING);

                assertEquals(msgOut, GridTestIoUtils.externalize(msgOut, marsh));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSerializationTopicCreatedByStringAndIntAndLong() throws Exception {
        for (Marshaller marsh : getMarshallers()) {
            info("Test GridTopic externalization [marshaller=" + marsh + ']');

            for (GridTopic topic : GridTopic.values()) {
                Externalizable msgOut = new T5(topic, UUID.nameUUIDFromBytes(A_STRING.getBytes(UTF_8)), AN_INT, A_LONG);

                assertEquals(msgOut, GridTestIoUtils.externalize(msgOut, marsh));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSerializationTopicCreatedByStrinAndLong() throws Exception {
        for (Marshaller marsh : getMarshallers()) {
            info("Test GridTopic externalization [marshaller=" + marsh + ']');

            for (GridTopic topic : GridTopic.values()) {
                Externalizable msgOut = (Externalizable)topic.topic(A_STRING, A_LONG);

                assertEquals(msgOut, GridTestIoUtils.externalize(msgOut, marsh));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSerializationTopicCreatedByStringAndUUIDAndIntAndLong() throws Exception {
        for (Marshaller marsh : getMarshallers()) {
            info("Test GridTopic externalization [marshaller=" + marsh + ']');

            for (GridTopic topic : GridTopic.values()) {
                Externalizable msgOut = new T7(topic, UUID.nameUUIDFromBytes(A_STRING.getBytes(UTF_8)), AN_UUID, AN_INT, A_LONG);

                assertEquals(msgOut, GridTestIoUtils.externalize(msgOut, marsh));
            }
        }
    }

    /** */
    private static class T5 implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private GridTopic topic;

        /** */
        private UUID id1;

        /** */
        private int id2;

        /** */
        private long id3;

        /**
         * No-arg constructor needed for {@link Serializable}.
         */
        public T5() {
            // No-op.
        }

        /**
         * @param topic Topic.
         * @param id1 ID1.
         * @param id2 ID2.
         * @param id3 ID3.
         */
        private T5(GridTopic topic, UUID id1, int id2, long id3) {
            this.topic = topic;
            this.id1 = id1;
            this.id2 = id2;
            this.id3 = id3;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return topic.ordinal() + id1.hashCode() + id2 + Long.hashCode(id3);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj.getClass() == T5.class) {
                T5 that = (T5)obj;

                return topic == that.topic && id1.equals(that.id1) && id2 == that.id2 && id3 == that.id3;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(topic.ordinal());
            U.writeUuid(out, id1);
            out.writeInt(id2);
            out.writeLong(id3);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException {
            topic = GridTopic.fromOrdinal(in.readByte());
            id1 = U.readUuid(in);
            id2 = in.readInt();
            id3 = in.readLong();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(T5.class, this);
        }
    }

    /** */
    private static class T7 implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private GridTopic topic;

        /** */
        private UUID id1;

        /** */
        private UUID id2;

        /** */
        private int id3;

        /** */
        private long id4;

        /**
         * No-arg constructor needed for {@link Serializable}.
         */
        public T7() {
            // No-op.
        }

        /**
         * @param topic Topic.
         * @param id1 ID1.
         * @param id2 ID2.
         * @param id3 ID3.
         * @param id4 ID4.
         */
        private T7(GridTopic topic, UUID id1, UUID id2, int id3, long id4) {
            this.topic = topic;
            this.id1 = id1;
            this.id2 = id2;
            this.id3 = id3;
            this.id4 = id4;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return topic.ordinal() + id1.hashCode() + id2.hashCode() + id3 + Long.hashCode(id4);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj.getClass() == T7.class) {
                T7 that = (T7)obj;

                return topic == that.topic && id1.equals(that.id1) && id2.equals(that.id2) && id3 == that.id3 && id4 == that.id4;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(topic.ordinal());
            U.writeUuid(out, id1);
            U.writeUuid(out, id2);
            out.writeInt(id3);
            out.writeLong(id4);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException {
            topic = GridTopic.fromOrdinal(in.readByte());
            id1 = U.readUuid(in);
            id2 = U.readUuid(in);
            id3 = in.readInt();
            id4 = in.readLong();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(T7.class, this);
        }
    }
}
