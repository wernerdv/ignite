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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.Nullable;

/** Message wrapper for grid topic. */
public class GridTopicMessage implements MarshallableMessage {
    /** Topic. */
    private Object topic;

    /** Serialized {@link #topic}. */
    @Order(0)
    byte[] topicBytes;

    /** Topic ordinal. */
    @Order(1)
    int ord = -1;

    /** */
    @Order(2)
    boolean needCustomClsLdr;

    /** Constructor for {@link CoreMessagesProvider}. */
    public GridTopicMessage() {
        // No-op.
    }

    /** Constructor. */
    public GridTopicMessage(Object topic) {
        this(topic, false);
    }

    /**
     * @param topic Topic.
     * @param needCustomClsLdr Flag indicating whether to use a special class loader for unmarshalling.
     */
    public GridTopicMessage(Object topic, boolean needCustomClsLdr) {
        this.topic = topic;
        this.needCustomClsLdr = needCustomClsLdr;

        if (topic instanceof GridTopic)
            ord = ((Enum<GridTopic>)topic).ordinal();
    }

    /** @return Topic object or null. */
    public static @Nullable Object topic(GridTopicMessage msg) {
        return msg == null ? null : msg.topic();
    }

    /** @return Topic ordinal. */
    public static int ordinal(GridTopicMessage msg) {
        return msg == null ? -1 : msg.ordinal();
    }

    /** @return Topic. */
    public Object topic() {
        return topic;
    }

    /** @return Ordinal value from enum or negative number in case when it's not an GridTopic type. */
    private int ordinal() {
        return ord;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        if (ord < 0 && topic != null)
            topicBytes = U.marshal(marsh, topic);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        if (needCustomClsLdr)
            return;

        if (ord < 0 && topicBytes != null) {
            topic = U.unmarshal(marsh, topicBytes, clsLdr);

            topicBytes = null;
        }
        else if (ord >= 0)
            topic = GridTopic.fromOrdinal(ord);
    }

    /**
     * Unmarshals topic using given marshaller and user-defined class loader.
     *
     * @param marsh Marshaller.
     * @param customClsLdr Custom class loader.
     */
    public void unmarshal(Marshaller marsh, ClassLoader customClsLdr) throws IgniteCheckedException {
        if (ord < 0 && topicBytes != null) {
            topic = U.unmarshal(marsh, topicBytes, customClsLdr);

            topicBytes = null;
        }
        else if (ord >= 0)
            topic = GridTopic.fromOrdinal(ord);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTopicMessage.class, this);
    }
}
