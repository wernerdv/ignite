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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.WalStateManager.WALDisableContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFoldersResolver;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.Files.walkFileTree;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointMarkersStorage.CP_FILE_NAME_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.partitionFileName;
import static org.apache.ignite.testframework.GridTestUtils.setFieldValue;

/***
 *
 */
@RunWith(Parameterized.class)
public class IgniteNodeStoppedDuringDisableWALTest extends GridCommonAbstractTest {
    /** Crash point. */
    private NodeStopPoint nodeStopPoint;

    /**
     * Default constructor to avoid BeforeFirstAndAfterLastTestRule.
     */
    private IgniteNodeStoppedDuringDisableWALTest() {
    }

    /**
     * @param nodeStopPoint Crash point.
     */
    public IgniteNodeStoppedDuringDisableWALTest(NodeStopPoint nodeStopPoint) {
        this.nodeStopPoint = nodeStopPoint;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        cfg.setAutoActivationEnabled(false);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * Test checks that after WAL is globally disabled and node is stopped, persistent store is cleaned properly after node restart.
     *
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        testStopNodeWithDisableWAL(nodeStopPoint);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @param nodeStopPoint Stop point.
     * @throws Exception If failed.
     */
    private void testStopNodeWithDisableWAL(NodeStopPoint nodeStopPoint) throws Exception {
        log.info("Start test crash " + nodeStopPoint);

        IgniteEx ig0 = startGrid(0);

        GridCacheSharedContext<Object, Object> sharedCtx = ig0.context().cache().context();

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)sharedCtx.database();
        IgniteWriteAheadLogManager WALmgr = sharedCtx.wal();

        WALDisableContext walDisableCtx = new WALDisableContext(dbMgr, sharedCtx.pageStore(), log) {
            @Override protected void writeMetaStoreDisableWALFlag() throws IgniteCheckedException {
                if (nodeStopPoint == NodeStopPoint.BEFORE_WRITE_KEY_TO_META_STORE)
                    failNode(nodeStopPoint);

                super.writeMetaStoreDisableWALFlag();

                if (nodeStopPoint == NodeStopPoint.AFTER_WRITE_KEY_TO_META_STORE)
                    failNode(nodeStopPoint);
            }

            @Override protected void removeMetaStoreDisableWALFlag() throws IgniteCheckedException {
                if (nodeStopPoint == NodeStopPoint.AFTER_CHECKPOINT_AFTER_ENABLE_WAL)
                    failNode(nodeStopPoint);

                super.removeMetaStoreDisableWALFlag();

                if (nodeStopPoint == NodeStopPoint.AFTER_REMOVE_KEY_TO_META_STORE)
                    failNode(nodeStopPoint);
            }

            @Override protected void disableWAL(boolean disable) throws IgniteCheckedException {
                if (disable) {
                    if (nodeStopPoint == NodeStopPoint.AFTER_CHECKPOINT_BEFORE_DISABLE_WAL)
                        failNode(nodeStopPoint);

                    super.disableWAL(disable);

                    if (nodeStopPoint == NodeStopPoint.AFTER_DISABLE_WAL)
                        failNode(nodeStopPoint);

                }
                else {
                    super.disableWAL(disable);

                    if (nodeStopPoint == NodeStopPoint.AFTER_ENABLE_WAL)
                        failNode(nodeStopPoint);
                }
            }
        };

        setFieldValue(sharedCtx.walState(), "walDisableContext", walDisableCtx);

        setFieldValue(WALmgr, "walDisableContext", walDisableCtx);

        ig0.context().internalSubscriptionProcessor().registerMetastorageListener(walDisableCtx);

        ig0.cluster().state(ClusterState.ACTIVE);

        try (IgniteDataStreamer<Integer, Integer> st = ig0.dataStreamer(DEFAULT_CACHE_NAME)) {
            st.allowOverwrite(true);

            for (int i = 0; i < GridTestUtils.SF.apply(10_000); i++)
                st.addData(i, -i);
        }

        boolean fail = false;

        try (WALIterator it = sharedCtx.wal().replay(null)) {
            dbMgr.applyUpdatesOnRecovery(it, (ptr, rec) -> true, (entry) -> true);
        }
        catch (IgniteCheckedException e) {
            if (nodeStopPoint.needCleanUp)
                fail = true;
        }

        Assert.assertEquals(nodeStopPoint.needCleanUp, fail);

        Ignite ig1 = startGrid(0);

        String msg = nodeStopPoint.toString();

        int pageSize = ig1.configuration().getDataStorageConfiguration().getPageSize();

        if (nodeStopPoint.needCleanUp) {
            PdsFoldersResolver foldersResolver = ((IgniteEx)ig1).context().pdsFolderResolver();

            File root = foldersResolver.fileTree().root();
            String metastorage = foldersResolver.fileTree().metaStorage().getName();

            walkFileTree(root.toPath(), new SimpleFileVisitor<Path>() {
                @Override public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                    String name = path.toFile().getName();

                    String filePath = path.toString();

                    String parentDirName = path.toFile().getParentFile().getName();

                    if (parentDirName.equals(metastorage))
                        return CONTINUE;

                    if (NodeFileTree.walSegment(path.toFile()) || NodeFileTree.walTmpSegment(path.toFile()))
                        return CONTINUE;

                    boolean failed = false;

                    if (name.endsWith(NodeFileTree.TMP_SUFFIX))
                        failed = true;

                    if (CP_FILE_NAME_PATTERN.matcher(name).matches())
                        failed = true;

                    if (NodeFileTree.partitionFile(path.toFile()) && path.toFile().length() > pageSize)
                        failed = true;

                    if (name.equals(partitionFileName(INDEX_PARTITION)) && path.toFile().length() > pageSize)
                        failed = true;

                    if (failed)
                        fail(msg + " " + filePath + " " + path.toFile().length());

                    return CONTINUE;
                }
            });
        }
    }

    /**
     * @param nodeStopPoint Stop point.
     * @throws IgniteCheckedException Always throws exception.
     */
    private void failNode(NodeStopPoint nodeStopPoint) throws IgniteCheckedException {
        stopGrid(0, true);

        throw new IgniteCheckedException(nodeStopPoint.toString());
    }

    /**
     * @return Node stop point.
     */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> providedTestData() {
        return Arrays.stream(NodeStopPoint.values()).map(it -> new Object[] {it}).collect(Collectors.toList());
    }

    /**
     * Crash point.
     */
    private enum NodeStopPoint {
        /** */
        BEFORE_WRITE_KEY_TO_META_STORE(false),

        /** */
        AFTER_WRITE_KEY_TO_META_STORE(true),

        /** */
        AFTER_CHECKPOINT_BEFORE_DISABLE_WAL(true),

        /** */
        AFTER_DISABLE_WAL(true),

        /** */
        AFTER_ENABLE_WAL(true),

        /** */
        AFTER_CHECKPOINT_AFTER_ENABLE_WAL(true),

        /** */
        AFTER_REMOVE_KEY_TO_META_STORE(false);

        /** Clean up flag. */
        private final boolean needCleanUp;

        /** */
        NodeStopPoint(boolean up) {
            needCleanUp = up;
        }
    }
}
