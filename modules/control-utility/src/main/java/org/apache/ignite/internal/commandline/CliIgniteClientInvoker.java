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

package org.apache.ignite.internal.commandline;

import java.util.Collection;
import java.util.function.Consumer;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.client.thin.TcpIgniteClient;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.BeforeNodeStartCommand;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandInvoker;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.MANAGEMENT_CLIENT_ATTR;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLIENT_LISTENER_PORT;

/**
 * Adapter of new management API command for {@code control.sh} execution flow.
 */
public class CliIgniteClientInvoker<A extends IgniteDataTransferObject> extends CommandInvoker<A> implements AutoCloseable {
    /** Client configuration. */
    private final ClientConfiguration cfg;

    /** Client. */
    private IgniteClient client;

    /** @param cmd Command to execute. */
    public CliIgniteClientInvoker(Command<A, ?> cmd, A arg, ClientConfiguration cfg) {
        super(cmd, arg, null);

        this.cfg = cfg;
    }

    /** {@inheritDoc} */
    @Override protected ClusterNode defaultNode() {
        String[] addr = cfg.getAddresses()[0].split(":");

        String host = addr[0];
        String port = addr[1];

        Collection<ClusterNode> nodes = igniteClient().cluster().nodes();

        return F.find(nodes, U.oldest(nodes, null), node ->
            (node.hostNames().contains(host) || node.addresses().contains(host))
                && port.equals(node.attribute(CLIENT_LISTENER_PORT).toString()));
    }

    /** {@inheritDoc} */
    @Override protected IgniteClient igniteClient() {
        if (client == null) {
            cfg.setUserAttributes(F.asMap(MANAGEMENT_CLIENT_ATTR, Boolean.TRUE.toString()));
            cfg.setAutoBinaryConfigurationEnabled(false);

            client = Ignition.startClient(cfg);
        }

        return client;
    }

    /** @return Message text to show user for. {@code null} means that confirmantion is not required. */
    public String confirmationPrompt() {
        return cmd.confirmationPrompt(arg);
    }

    /** */
    public <R> R invokeBeforeNodeStart(Consumer<String> printer) throws Exception {
        return ((BeforeNodeStartCommand<A, R>)cmd).execute((TcpIgniteClient)igniteClient(), arg, printer);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        U.closeQuiet(client);
    }
}
