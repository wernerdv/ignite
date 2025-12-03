/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test that verifies correct exchange and rebalancing behavior with a large number of caches.
 */
public class LargeCacheExchangeTest extends GridCommonAbstractTest {
    /** Number of initial server nodes. */
    private static final int SRV_CNT = 3;

    /** Number of caches to create. */
    private static final int CACHE_CNT = 500;

    /** Number of partitions per cache. */
    private static final int PART_CNT = 1000;

    /** Number of entries per partition. */
    private static final int ENTRIES_PER_PART = 30;

    /** Number of backups. */
    private static final int BACKUPS = 2;

    /** */
    private static final boolean FILL_EACH_PARTITION = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        DataRegionConfiguration regCfg = new DataRegionConfiguration()
                .setName("test_region")
                .setInitialSize(1024L * 1024 * 1024)     // 1 GB initial
                .setMaxSize(10L * 1024 * 1024 * 1024)    // 10 GB max
                .setPersistenceEnabled(false);

        dsCfg.setDefaultDataRegionConfiguration(regCfg);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 120 * 60 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected long getPartitionMapExchangeTimeout() {
        return 120 * 60 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Test logic:
     * - Start SRV_CNT server nodes.
     * - Create CACHE_CNT partitioned transactional caches with PART_CNT partitions each.
     * - Write ENTRIES_PER_PART entries into each partition.
     * - Start a new server node.
     * - Wait for partition map exchange.
     * - Stop old server node.
     * - Wait for partition map exchange.
     * - Restart old server node.
     * - Wait for partition map exchange.
     */
    @Test
    public void testLargeCachesWithServerJoinLeft() throws Exception {
        for (int i = 0; i < SRV_CNT; i++)
            startGrid(i);

        IgniteEx ignite0 = grid(0);

        ignite0.cluster().active(true);

        System.out.println(">>> Started initial 3 server nodes.");

        List<String> cacheNames = new ArrayList<>();

        for (int i = 0; i < CACHE_CNT; i++) {
            String cacheName = "cache-" + i;

            cacheNames.add(cacheName);

            CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(cacheName)
                .setCacheMode(CacheMode.PARTITIONED)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setWriteSynchronizationMode(FULL_SYNC)
                .setAffinity(new RendezvousAffinityFunction(false, PART_CNT))
                .setBackups(BACKUPS);

            ignite0.getOrCreateCache(ccfg);
        }

        System.out.println(">>> Created " + CACHE_CNT + " caches with " + PART_CNT + " partitions each.");

        if (FILL_EACH_PARTITION) {
            for (String cacheName : cacheNames) {
                IgniteCache<Object, Object> cache = ignite0.cache(cacheName);
                RendezvousAffinityFunction aff = (RendezvousAffinityFunction)cache.getConfiguration(CacheConfiguration.class).getAffinity();

                for (int part = 0; part < PART_CNT; part++) {
                    List<Integer> keys = new ArrayList<>(ENTRIES_PER_PART);

                    int key = part * 1_000_000;

                    while (keys.size() < ENTRIES_PER_PART) {
                        if (aff.partition(key) == part) {
                            System.out.println(">>> Adding key=" + key + " for cache=" + cacheName + ", partition=" + part);

                            keys.add(key);
                        }

                        key++;
                    }

                    for (int k : keys)
                        cache.put(k, "val-" + key);
                }

                System.out.println(">>> Wrote " + ENTRIES_PER_PART + " entries into cache=" + cacheName);
            }
        }

        for (String cacheName : cacheNames) {
            IgniteCache<Object, Object> cache = ignite0.cache(cacheName);

            for (int i = 0; i < 500; i++)
                cache.put(i, "val-" + i);
        }

        System.out.println(">>> Finished writing data into all caches.");

        startGrid(SRV_CNT);

        awaitPartitionMapExchange();

        System.out.println(">>> Partition map exchange completed after new server join.");

        stopGrid(1);

        System.out.println(">>> Partition map exchange completed after stop old server.");

        awaitPartitionMapExchange();

        startGrid(1);

        awaitPartitionMapExchange();

        System.out.println(">>> Partition map exchange completed after restart old server.");
    }

    /**
     * Оценивает размер Map<Integer, GridDhtPartitionFullMap> parts в GridDhtPartitionsFullMessage
     * после сериализации (U.marshal), но до сжатия (U.zip).
     *
     * @param cacheCnt Количество кешей
     * @param partsCntPerCache Количество партиций в кеше
     * @param backupFactor Фактор бэкапирования
     * @param srvCnt Число серверных узлов в кластере
     * @return Оценка размера в байтах
     */
    public static long estimatePartsSize(
        int cacheCnt,
        int partsCntPerCache,
        int backupFactor,
        int srvCnt
    ) {
        // 36 bytes:
        // - 32 bytes - GridDhtPartitionFullMap metadata: UUID (16 bytes) + nodeOrder (8 bytes) + updateSeq (8 bytes)
        // - 4 bytes - size of GridDhtPartitionFullMap

        // 56 bytes:
        // - 16 bytes - node UUID key
        // - 40 bytes - GridDhtPartitionMap metadata: UUID (16 bytes) + updateSeq (8 bytes) + size of GridPartitionStateMap (4 bytes)
        //              + topology version (8 bytes) + minor version (4 bytes)

        // 3 bytes for each entry in GridPartitionStateMap:
        // - 1 byte - state ordinal (byte)
        // - 2 bytes - partition ID (short)

        long res = cacheCnt * (36 + 56L * srvCnt + 3L * partsCntPerCache * (backupFactor + 1));

        System.out.println(">>> Estimated size of parts full map: " + res + " bytes [cacheCnt=" + cacheCnt + ", partsCntPerCache=" + partsCntPerCache +
            ", srvCnt=" + srvCnt + ", backupFactor=" + backupFactor + "]");

        return res;
    }

    /** */
    public static void main(String[] args) {
        estimatePartsSize(500, 1000, 2, 3);
        estimatePartsSize(700, 1000, 2, 3);
        estimatePartsSize(1000, 1000, 2, 3);
        estimatePartsSize(2000, 1000, 2, 3);
        estimatePartsSize(3000, 1000, 2, 3);
        estimatePartsSize(5000, 1000, 2, 3);

        System.out.println();

        estimatePartsSize(500, 1024, 2, 5);
        estimatePartsSize(1000, 1024, 2, 5);
        estimatePartsSize(3000, 1024, 2, 5);

        System.out.println();

        estimatePartsSize(500, 2048, 2, 5);
        estimatePartsSize(1000, 2048, 2, 5);
        estimatePartsSize(3000, 2048, 2, 5);

        System.out.println();

        estimatePartsSize(500, 2048, 2, 10);
        estimatePartsSize(1000, 2048, 2, 10);
        estimatePartsSize(3000, 2048, 2, 10);
    }
}
