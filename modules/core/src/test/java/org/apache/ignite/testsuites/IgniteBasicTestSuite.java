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

package org.apache.ignite.testsuites;

import org.apache.ignite.GridSuppressedExceptionSelfTest;
import org.apache.ignite.internal.ClusterGroupHostsSelfTest;
import org.apache.ignite.internal.ClusterGroupSelfTest;
import org.apache.ignite.internal.GridFailFastNodeFailureDetectionSelfTest;
import org.apache.ignite.internal.GridLifecycleAwareSelfTest;
import org.apache.ignite.internal.GridLifecycleBeanSelfTest;
import org.apache.ignite.internal.GridMBeansTest;
import org.apache.ignite.internal.GridMbeansMiscTest;
import org.apache.ignite.internal.GridNodeMetricsLogSelfTest;
import org.apache.ignite.internal.GridProjectionForCachesSelfTest;
import org.apache.ignite.internal.GridReduceSelfTest;
import org.apache.ignite.internal.GridReleaseTypeSelfTest;
import org.apache.ignite.internal.GridSelfTest;
import org.apache.ignite.internal.GridStartStopSelfTest;
import org.apache.ignite.internal.GridStopWithCancelSelfTest;
import org.apache.ignite.internal.GridStopWithCollisionSpiTest;
import org.apache.ignite.internal.IgniteDiscoveryMassiveNodeFailTest;
import org.apache.ignite.internal.IgniteLocalNodeMapBeforeStartTest;
import org.apache.ignite.internal.IgniteSlowClientDetectionSelfTest;
import org.apache.ignite.internal.TransactionsMXBeanImplTest;
import org.apache.ignite.internal.codegen.IgniteDataTransferObjectProcessorTest;
import org.apache.ignite.internal.codegen.MessageProcessorTest;
import org.apache.ignite.internal.managers.communication.CompressedMessageTest;
import org.apache.ignite.internal.managers.communication.DefaultEnumMapperTest;
import org.apache.ignite.internal.managers.communication.ErrorMessageSelfTest;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoveryMessageSerializationTest;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentV2Test;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentV2TestNoOptimizations;
import org.apache.ignite.internal.processors.affinity.GridAffinityProcessorRendezvousSelfTest;
import org.apache.ignite.internal.processors.affinity.GridHistoryAffinityAssignmentTest;
import org.apache.ignite.internal.processors.affinity.GridHistoryAffinityAssignmentTestNoOptimization;
import org.apache.ignite.internal.processors.cache.GridLocalIgniteSerializationTest;
import org.apache.ignite.internal.processors.cache.IgniteMarshallerCacheConcurrentReadWriteTest;
import org.apache.ignite.internal.processors.cache.SetTxTimeoutOnPartitionMapExchangeTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.EvictPartitionInLogTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.PartitionEvictionOrderTest;
import org.apache.ignite.internal.processors.cache.query.continuous.DiscoveryDataDeserializationFailureHanderTest;
import org.apache.ignite.internal.processors.closure.GridClosureProcessorRemoteTest;
import org.apache.ignite.internal.processors.closure.GridClosureProcessorSelfTest;
import org.apache.ignite.internal.processors.closure.GridClosureSerializationTest;
import org.apache.ignite.internal.processors.continuous.GridEventConsumeSelfTest;
import org.apache.ignite.internal.processors.continuous.GridMessageListenSelfTest;
import org.apache.ignite.internal.processors.odbc.ClientListenerMetricsTest;
import org.apache.ignite.internal.processors.odbc.ClientSessionOutboundQueueLimitTest;
import org.apache.ignite.internal.processors.odbc.OdbcConfigurationValidationSelfTest;
import org.apache.ignite.internal.processors.odbc.OdbcEscapeSequenceSelfTest;
import org.apache.ignite.internal.processors.odbc.SqlListenerUtilsTest;
import org.apache.ignite.internal.product.GridProductVersionSelfTest;
import org.apache.ignite.internal.util.nio.IgniteExceptionInNioWorkerSelfTest;
import org.apache.ignite.messaging.GridMessagingNoPeerClassLoadingSelfTest;
import org.apache.ignite.messaging.GridMessagingSelfTest;
import org.apache.ignite.messaging.IgniteMessagingSendAsyncTest;
import org.apache.ignite.messaging.IgniteMessagingWithClientTest;
import org.apache.ignite.spi.ExponentialBackoffTimeoutStrategyTest;
import org.apache.ignite.spi.GridSpiLocalHostInjectionTest;
import org.apache.ignite.spi.GridTcpSpiForwardingSelfTest;
import org.apache.ignite.spi.discovery.AuthenticationRestartTest;
import org.apache.ignite.spi.discovery.DiscoverySpiDataExchangeTest;
import org.apache.ignite.spi.discovery.FilterDataForClientNodeDiscoveryTest;
import org.apache.ignite.spi.discovery.IgniteClientReconnectEventHandlingTest;
import org.apache.ignite.spi.discovery.IgniteDiscoveryCacheReuseSelfTest;
import org.apache.ignite.spi.discovery.LongClientConnectToClusterTest;
import org.apache.ignite.spi.discovery.datacenter.MultiDataCenterClientRoutingTest;
import org.apache.ignite.spi.discovery.datacenter.MultiDataCenterDeploymentTest;
import org.apache.ignite.spi.discovery.tcp.DiscoveryClientSocketTest;
import org.apache.ignite.spi.discovery.tcp.DiscoveryUnmarshalVulnerabilityTest;
import org.apache.ignite.spi.discovery.tcp.IgniteClientConnectSslTest;
import org.apache.ignite.spi.discovery.tcp.IgniteClientConnectTest;
import org.apache.ignite.spi.discovery.tcp.IgniteClientReconnectMassiveShutdownSslTest;
import org.apache.ignite.spi.discovery.tcp.IgniteClientReconnectMassiveShutdownTest;
import org.apache.ignite.spi.discovery.tcp.IgniteMetricsOverflowTest;
import org.apache.ignite.spi.discovery.tcp.MultiDataCenterRingTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoveryMarshallerCheckSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiCoordinatorChangeTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiFailureTimeoutSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiMulticastTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoveryUnresolvedHostTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryClientSuspensionSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryConcurrentStartTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryCoordinatorFailureTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryDeadNodeAddressResolvingTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryFailedJoinTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryIpFinderCleanerTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryIpFinderFailureTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryMdcSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryMetricsWarnLogTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryMultiThreadedTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryNetworkIssuesTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryNodeAttributesUpdateOnReconnectTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryNodeConfigConsistentIdSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryNodeConsistentIdSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryNodeJoinAndFailureTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryPendingMessageDeliveryMdcReversedTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryPendingMessageDeliveryMdcTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryPendingMessageDeliveryTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryReconnectUnstableTopologyTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryRestartTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySegmentationPolicyTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySnapshotHistoryTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiConfigSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiFailureTimeoutSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiMBeanTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiReconnectDelayTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiSslSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiStartStopSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiWildcardSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySslSecuredUnsecuredTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySslSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySslTrustedSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySslTrustedUntrustedTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryWithAddressFilterTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryWithWrongServerTest;
import org.apache.ignite.spi.discovery.tcp.TestMetricUpdateFailure;
import org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc.TcpDiscoveryJdbcIpFinderSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinderSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinderSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinderDnsResolveTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinderSelfTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTestSelfTest;
import org.apache.ignite.testframework.junits.multijvm.JavaVersionCommandParserTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Basic test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IgniteMarshallerSelfTestSuite.class,
    IgniteLangSelfTestSuite.class,
    IgniteUtilSelfTestSuite.class,

    IgniteKernalSelfTestSuite.class,
    IgniteStartUpTestSuite.class,
    IgniteExternalizableSelfTestSuite.class,
    IgniteP2PSelfTestSuite.class,
    IgniteCacheP2pUnmarshallingErrorTestSuite.class,
    IgniteStreamSelfTestSuite.class,

    IgnitePlatformsTestSuite.class,

    GridSelfTest.class,
    GridCommonAbstractTestSelfTest.class,
    ClusterGroupHostsSelfTest.class,
    IgniteMessagingWithClientTest.class,
    IgniteMessagingSendAsyncTest.class,

    ClusterGroupSelfTest.class,
    GridMessagingSelfTest.class,
    GridMessagingNoPeerClassLoadingSelfTest.class,

    GridReleaseTypeSelfTest.class,
    GridProductVersionSelfTest.class,
    GridAffinityAssignmentV2Test.class,
    GridAffinityAssignmentV2TestNoOptimizations.class,
    GridHistoryAffinityAssignmentTest.class,
    GridHistoryAffinityAssignmentTestNoOptimization.class,
    GridAffinityProcessorRendezvousSelfTest.class,
    GridClosureProcessorSelfTest.class,
    GridClosureProcessorRemoteTest.class,
    GridClosureSerializationTest.class,
    GridStartStopSelfTest.class,
    GridProjectionForCachesSelfTest.class,
    GridSpiLocalHostInjectionTest.class,
    GridLifecycleBeanSelfTest.class,
    GridStopWithCancelSelfTest.class,
    GridStopWithCollisionSpiTest.class,
    GridReduceSelfTest.class,
    GridEventConsumeSelfTest.class,
    GridSuppressedExceptionSelfTest.class,
    GridLifecycleAwareSelfTest.class,
    GridMessageListenSelfTest.class,
    GridFailFastNodeFailureDetectionSelfTest.class,
    IgniteSlowClientDetectionSelfTest.class,
    IgniteMarshallerCacheConcurrentReadWriteTest.class,
    GridNodeMetricsLogSelfTest.class,
    GridLocalIgniteSerializationTest.class,
    GridMBeansTest.class,
    GridMbeansMiscTest.class,
    TransactionsMXBeanImplTest.class,
    SetTxTimeoutOnPartitionMapExchangeTest.class,
    DiscoveryDataDeserializationFailureHanderTest.class,

    EvictPartitionInLogTest.class,
    PartitionEvictionOrderTest.class,

    IgniteExceptionInNioWorkerSelfTest.class,
    IgniteLocalNodeMapBeforeStartTest.class,

    ClientListenerMetricsTest.class,
    OdbcConfigurationValidationSelfTest.class,
    OdbcEscapeSequenceSelfTest.class,
    SqlListenerUtilsTest.class,
    JavaVersionCommandParserTest.class,
    ClientSessionOutboundQueueLimitTest.class,

    MessageProcessorTest.class,
    ErrorMessageSelfTest.class,
    DefaultEnumMapperTest.class,
    IgniteDataTransferObjectProcessorTest.class,
    CompressedMessageTest.class,
    TcpDiscoveryVmIpFinderDnsResolveTest.class,
    TcpDiscoveryVmIpFinderSelfTest.class,
    TcpDiscoverySharedFsIpFinderSelfTest.class,
    TcpDiscoveryJdbcIpFinderSelfTest.class,
    TcpDiscoveryMulticastIpFinderSelfTest.class,
    TcpDiscoveryIpFinderCleanerTest.class,

    TcpDiscoverySelfTest.class,
    TcpDiscoverySpiSelfTest.class,
    TcpDiscoverySpiSslSelfTest.class,
    TcpDiscoverySpiWildcardSelfTest.class,
    TcpDiscoverySpiFailureTimeoutSelfTest.class,
    TcpDiscoverySpiMBeanTest.class,
    TcpDiscoverySpiStartStopSelfTest.class,
    TcpDiscoverySpiConfigSelfTest.class,
    TcpDiscoverySnapshotHistoryTest.class,
    TcpDiscoveryNodeJoinAndFailureTest.class,

    GridTcpSpiForwardingSelfTest.class,

    ExponentialBackoffTimeoutStrategyTest.class,

    TcpClientDiscoverySpiSelfTest.class,
    LongClientConnectToClusterTest.class,
    TcpClientDiscoveryMarshallerCheckSelfTest.class,
    TcpClientDiscoverySpiCoordinatorChangeTest.class,
    TcpClientDiscoverySpiMulticastTest.class,
    TcpClientDiscoverySpiFailureTimeoutSelfTest.class,
    TcpClientDiscoveryUnresolvedHostTest.class,

    TcpDiscoveryNodeConsistentIdSelfTest.class,
    TcpDiscoveryNodeConfigConsistentIdSelfTest.class,

    TcpDiscoveryRestartTest.class,
    TcpDiscoveryMultiThreadedTest.class,
    TcpDiscoveryMetricsWarnLogTest.class,
    TcpDiscoveryConcurrentStartTest.class,

    TcpDiscoverySegmentationPolicyTest.class,

    TcpDiscoveryNodeAttributesUpdateOnReconnectTest.class,
    AuthenticationRestartTest.class,

    TcpDiscoveryWithWrongServerTest.class,

    TcpDiscoveryWithAddressFilterTest.class,

    TcpDiscoverySpiReconnectDelayTest.class,

    TcpDiscoveryNetworkIssuesTest.class,

    IgniteDiscoveryMassiveNodeFailTest.class,
    TcpDiscoveryCoordinatorFailureTest.class,

    TestMetricUpdateFailure.class,

    // Client connect.
    IgniteClientConnectTest.class,
    IgniteClientConnectSslTest.class,
    IgniteClientReconnectMassiveShutdownTest.class,
    IgniteClientReconnectMassiveShutdownSslTest.class,
    TcpDiscoveryClientSuspensionSelfTest.class,
    IgniteClientReconnectEventHandlingTest.class,

    TcpDiscoveryFailedJoinTest.class,

    // SSL.
    TcpDiscoverySslSelfTest.class,
    TcpDiscoverySslTrustedSelfTest.class,
    TcpDiscoverySslSecuredUnsecuredTest.class,
    TcpDiscoverySslTrustedUntrustedTest.class,
    // Disco cache reuse.
    IgniteDiscoveryCacheReuseSelfTest.class,

    DiscoveryUnmarshalVulnerabilityTest.class,

    FilterDataForClientNodeDiscoveryTest.class,

    TcpDiscoveryPendingMessageDeliveryTest.class,

    TcpDiscoveryReconnectUnstableTopologyTest.class,

    IgniteMetricsOverflowTest.class,

    DiscoveryClientSocketTest.class,

    DiscoverySpiDataExchangeTest.class,

    TcpDiscoveryIpFinderFailureTest.class,

    TcpDiscoveryDeadNodeAddressResolvingTest.class,

    // MDC.
    TcpDiscoveryMdcSelfTest.class,
    TcpDiscoveryPendingMessageDeliveryMdcTest.class,
    TcpDiscoveryPendingMessageDeliveryMdcReversedTest.class,
    MultiDataCenterDeploymentTest.class,
    MultiDataCenterRingTest.class,
    MultiDataCenterClientRoutingTest.class,

    IgniteDiscoveryMessageSerializationTest.class
})
public class IgniteBasicTestSuite {
}
