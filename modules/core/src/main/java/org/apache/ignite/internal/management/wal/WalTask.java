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

package org.apache.ignite.internal.management.wal;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.management.wal.WalPrintCommand.WalPrintCommandArg;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

/**
 * Performs WAL cleanup clusterwide.
 */
@GridInternal
public class WalTask extends VisorMultiNodeTask<WalDeleteCommandArg, WalTaskResult, Collection<String>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** WAL archive file filter. */
    private static final FileFilter WAL_ARCHIVE_FILE_FILTER = new FileFilter() {
        @Override public boolean accept(File file) {
            return NodeFileTree.walSegment(file) || NodeFileTree.walCompactedSegment(file);
        }
    };

    /** {@inheritDoc} */
    @Override protected WalJob job(WalDeleteCommandArg arg) {
        return new WalJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<WalDeleteCommandArg> arg) {
        Collection<org.apache.ignite.cluster.ClusterNode> srvNodes = ignite.cluster().forServers().nodes();
        Collection<UUID> ret = new ArrayList<>(srvNodes.size());

        WalDeleteCommandArg taskArg = arg.getArgument();

        Set<String> nodeIds = taskArg.consistentIds() != null
            ? new HashSet<>(Arrays.asList(arg.getArgument().consistentIds()))
            : null;

        if (nodeIds == null) {
            for (org.apache.ignite.cluster.ClusterNode node : srvNodes)
                ret.add(node.id());
        }
        else {
            for (org.apache.ignite.cluster.ClusterNode node : srvNodes) {
                if (nodeIds.contains(node.consistentId().toString()))
                    ret.add(node.id());
            }
        }

        return ret;
    }

    /** {@inheritDoc} */
    @Nullable @Override protected WalTaskResult reduce0(List<ComputeJobResult> results) throws IgniteException {
        Map<String, Exception> exRes = U.newHashMap(0);
        Map<String, Collection<String>> res = U.newHashMap(results.size());
        Map<String, ClusterNode> nodesInfo = U.newHashMap(results.size());

        for (ComputeJobResult result: results) {
            org.apache.ignite.cluster.ClusterNode node = result.getNode();

            String nodeId = node.consistentId().toString();

            if (result.getException() != null)
                exRes.put(nodeId, result.getException());
            else if (result.getData() != null) {
                Collection<String> data = result.getData();

                if (data != null)
                    res.put(nodeId, data);
            }

            nodesInfo.put(nodeId, new ClusterNode(node));
        }

        return new WalTaskResult(res, exRes, nodesInfo);
    }

    /**
     * Performs WAL cleanup per node.
     */
    private static class WalJob extends VisorJob<WalDeleteCommandArg, Collection<String>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Auto injected logger */
        @LoggerResource
        private transient IgniteLogger log;

        /** Node file tree. */
        private transient NodeFileTree ft;

        /**
         *  @param arg WAL task argument.
         *  @param debug Debug flag.
         */
        public WalJob(WalDeleteCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Nullable @Override protected Collection<String> run(@Nullable WalDeleteCommandArg arg) throws IgniteException {
            try {
                GridKernalContext cctx = ignite.context();
                ft = ignite.context().pdsFolderResolver().fileTree();

                GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)cctx.cache().context().database();
                FileWriteAheadLogManager wal = (FileWriteAheadLogManager)cctx.cache().context().wal();

                if (dbMgr == null || arg == null || wal == null)
                    return null;

                if (arg instanceof WalPrintCommandArg)
                    return getUnusedWalSegments(dbMgr, wal);
                else
                    return deleteUnusedWalSegments(dbMgr, wal);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to perform WAL task", e);

                throw new IgniteException("Failed to perform WAL task", e);
            }
        }

        /**
         * Get unused wal segments.
         *
         * @param  wal Database manager.
         * @return {@link Collection<String>} of absolute paths of unused WAL segments.
         * @throws IgniteCheckedException if failed.
         */
        Collection<String> getUnusedWalSegments(
            GridCacheDatabaseSharedManager dbMgr,
            FileWriteAheadLogManager wal
        ) throws IgniteCheckedException {
            WALPointer lowBoundForTruncate = dbMgr.checkpointHistory().firstCheckpointPointer();

            if (lowBoundForTruncate == null)
                return Collections.emptyList();

            int maxIdx = resolveMaxReservedIndex(wal, lowBoundForTruncate);

            File[] walFiles = getWalArchiveDir().listFiles(WAL_ARCHIVE_FILE_FILTER);

            Collection<String> res = new ArrayList<>(walFiles != null && walFiles.length > 0 ? walFiles.length - 1 : 0);

            if (walFiles != null && walFiles.length > 0) {
                sortWalFiles(walFiles);

                // Obtain index of last archived WAL segment, it will not be deleted.
                long lastArchIdx = ft.walSegmentIndex(walFiles[walFiles.length - 1].toPath());

                for (File f : walFiles) {
                    long fileIdx = ft.walSegmentIndex(f.toPath());

                    if (fileIdx < maxIdx && fileIdx < lastArchIdx)
                        res.add(f.getAbsolutePath());
                    else
                        break;
                }
            }

            return res;
        }

        /**
         * Delete unused wal segments.
         *
         * @param dbMgr Database manager.
         * @return {@link Collection<String>} of deleted WAL segment's files.
         * @throws IgniteCheckedException if failed.
         */
        Collection<String> deleteUnusedWalSegments(
            GridCacheDatabaseSharedManager dbMgr,
            FileWriteAheadLogManager wal
        ) throws IgniteCheckedException {
            WALPointer lowBoundForTruncate = dbMgr.checkpointHistory().firstCheckpointPointer();

            if (lowBoundForTruncate == null)
                return Collections.emptyList();

            int maxIdx = resolveMaxReservedIndex(wal, lowBoundForTruncate);

            File[] walFiles = getWalArchiveDir().listFiles(WAL_ARCHIVE_FILE_FILTER);

            dbMgr.onWalTruncated(lowBoundForTruncate);

            int num = wal.truncate(lowBoundForTruncate);

            if (walFiles != null) {
                sortWalFiles(walFiles);

                Collection<String> res = new ArrayList<>(num);

                for (File walFile: walFiles) {
                    if (ft.walSegmentIndex(walFile.toPath()) < maxIdx && num > 0)
                        res.add(walFile.getAbsolutePath());
                    else
                        break;

                    num--;
                }

                return res;
            }
            else
                return Collections.emptyList();

        }

        /**
         *
         */
        private int resolveMaxReservedIndex(FileWriteAheadLogManager wal, WALPointer lowBoundForTruncate) {
            int resCnt = wal.reserved(null, lowBoundForTruncate);

            long highIdx = lowBoundForTruncate.index();

            return (int)(highIdx - resCnt + 1);
        }

        /**
         * Get WAL archive directory from configuration.
         *
         * @return WAL archive directory.
         * @throws IgniteCheckedException if failed.
         */
        private File getWalArchiveDir() throws IgniteCheckedException {
            if (!ft.walArchive().exists())
                throw new IgniteCheckedException("WAL archive directory does not exists" + ft.walArchive().getAbsolutePath());

            return ft.walArchive();
        }

        /**
         * Sort WAL files according their indices.
         *
         * @param files Array of WAL segment files.
         */
        private void sortWalFiles(File[] files) {
            Arrays.sort(files, new Comparator<File>() {
                @Override public int compare(File o1, File o2) {
                    return Long.compare(ft.walSegmentIndex(o1.toPath()), ft.walSegmentIndex(o2.toPath()));
                }
            });
        }
    }
}
