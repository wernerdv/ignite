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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheLockCandidates;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheCompoundIdentityFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheVersionedFuture;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheMappedVersion;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockCancelledException;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.GridDhtColocatedLockFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.ClusterNodeFunc;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_PRELOAD;
import static org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_DHT_LOCK_MAP;

/**
 * Cache lock future.
 */
public final class GridDhtLockFuture extends GridCacheCompoundIdentityFuture<Boolean>
    implements GridCacheVersionedFuture<Boolean>, GridDhtFuture<Boolean>, GridCacheMappedVersion {
    /** */
    private static final long serialVersionUID = 0L;

    /** Tracing span. */
    private Span span;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /** Logger. */
    private static IgniteLogger msgLog;

    /** Cache registry. */
    @GridToStringExclude
    private final GridCacheContext<?, ?> cctx;

    /** Near node ID. */
    private final UUID nearNodeId;

    /** Near lock version. */
    private final GridCacheVersion nearLockVer;

    /** Topology version. */
    private final AffinityTopologyVersion topVer;

    /** Thread. */
    private final long threadId;

    /**
     * Keys locked so far.
     * <p>
     * Thread created this object iterates over entries and tries to lock each of them.
     * If it finds some entry already locked by another thread it registers callback which will be executed
     * by the thread owning the lock.
     * <p>
     * Thus access to this collection must be synchronized except cases
     * when this object is yet local to the thread created it.
     */
    @GridToStringExclude
    private final List<GridDhtCacheEntry> entries;

    /** DHT mappings. */
    private final Map<ClusterNode, List<GridDhtCacheEntry>> dhtMap =
        new ConcurrentHashMap<>();

    /** Future ID. */
    private final IgniteUuid futId;

    /** Lock version. */
    private GridCacheVersion lockVer;

    /** Read flag. */
    private final boolean read;

    /** Error. */
    private Throwable err;

    /** Timed out flag. */
    private volatile boolean timedOut;

    /** Timeout object. */
    @GridToStringExclude
    private LockTimeoutObject timeoutObj;

    /** Lock timeout. */
    private final long timeout;

    /** Transaction. */
    private final GridDhtTxLocalAdapter tx;

    /** All replies flag. */
    private boolean mapped;

    /** */
    private Collection<Integer> invalidParts;

    /** Trackable flag. */
    private boolean trackable = true;

    /** Pending locks. */
    private final Collection<KeyCacheObject> pendingLocks;

    /** TTL for create operation. */
    private final long createTtl;

    /** TTL for read operation. */
    private final long accessTtl;

    /** Need return value flag. */
    private final boolean needReturnVal;

    /** Skip store flag. */
    private final boolean skipStore;

    /** Keep binary. */
    private final boolean keepBinary;

    /**
     * @param cctx Cache context.
     * @param nearNodeId Near node ID.
     * @param nearLockVer Near lock version.
     * @param topVer Topology version.
     * @param cnt Number of keys to lock.
     * @param read Read flag.
     * @param needReturnVal Need return value flag.
     * @param timeout Lock acquisition timeout.
     * @param tx Transaction.
     * @param threadId Thread ID.
     * @param accessTtl TTL for read operation.
     * @param skipStore Skip store flag.
     */
    public GridDhtLockFuture(
        GridCacheContext<?, ?> cctx,
        UUID nearNodeId,
        GridCacheVersion nearLockVer,
        @NotNull AffinityTopologyVersion topVer,
        int cnt,
        boolean read,
        boolean needReturnVal,
        long timeout,
        GridDhtTxLocalAdapter tx,
        long threadId,
        long createTtl,
        long accessTtl,
        boolean skipStore,
        boolean keepBinary) {
        super(CU.boolReducer());

        assert nearNodeId != null;
        assert nearLockVer != null;
        assert topVer.topologyVersion() > 0;
        assert tx == null || timeout >= 0;

        this.cctx = cctx;
        this.nearNodeId = nearNodeId;
        this.nearLockVer = nearLockVer;
        this.topVer = topVer;
        this.read = read;
        this.needReturnVal = needReturnVal;
        this.timeout = timeout;
        this.tx = tx;
        this.createTtl = createTtl;
        this.accessTtl = accessTtl;
        this.skipStore = skipStore;
        this.keepBinary = keepBinary;

        if (tx != null)
            tx.topologyVersion(topVer);

        assert tx == null || threadId == tx.threadId();

        this.threadId = threadId;

        if (tx != null)
            lockVer = tx.xidVersion();
        else {
            lockVer = cctx.mvcc().mappedVersion(nearLockVer);

            if (lockVer == null)
                lockVer = nearLockVer;
        }

        futId = IgniteUuid.randomUuid();

        entries = new ArrayList<>(cnt);
        pendingLocks = U.newHashSet(cnt);

        if (log == null) {
            msgLog = cctx.shared().txLockMessageLogger();
            log = U.logger(cctx.kernalContext(), logRef, GridDhtLockFuture.class);
        }

        if (tx != null) {
            while (true) {
                IgniteInternalFuture<?> fut = tx.lockFut;

                if (fut != null) {
                    if (fut == GridDhtTxLocalAdapter.ROLLBACK_FUT)
                        onError(tx.timedOut() ? tx.timeoutException() : tx.rollbackException());
                    else {
                        // Wait for collocated lock future.
                        assert fut instanceof GridDhtColocatedLockFuture : fut;

                        // Terminate this future if parent(collocated) future is terminated by rollback.
                        fut.listen(() -> {
                            try {
                                fut.get();
                            }
                            catch (IgniteCheckedException e) {
                                onError(e);
                            }
                        });
                    }

                    return;
                }

                if (tx.updateLockFuture(null, this))
                    return;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> invalidPartitions() {
        return invalidParts == null ? Collections.emptyList() : invalidParts;
    }

    /**
     * @param cacheCtx Cache context.
     * @param invalidPart Partition to retry.
     */
    void addInvalidPartition(GridCacheContext<?, ?> cacheCtx, int invalidPart) {
        if (invalidParts == null)
            invalidParts = new HashSet<>();

        invalidParts.add(invalidPart);

        // Register invalid partitions with transaction.
        if (tx != null)
            tx.addInvalidPartition(cacheCtx.cacheId(), invalidPart);

        if (log.isDebugEnabled())
            log.debug("Added invalid partition to future [invalidPart=" + invalidPart + ", fut=" + this + ']');
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return lockVer;
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return trackable;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        trackable = false;
    }

    /**
     * @return Entries.
     */
    public Collection<GridDhtCacheEntry> entries() {
        return F.view(entries, F.notNull());
    }

    /**
     * Need of synchronization here is explained in the field's {@link GridDhtLockFuture#entries} comment.
     *
     * @return Copy of entries collection.
     */
    private synchronized Collection<GridDhtCacheEntry> entriesCopy() {
        return new ArrayList<>(entries());
    }

    /**
     * @return Future ID.
     */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Near lock version.
     */
    public GridCacheVersion nearLockVersion() {
        return nearLockVer;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheVersion mappedVersion() {
        return tx == null ? nearLockVer : null;
    }

    /**
     * @return {@code True} if transaction is not {@code null}.
     */
    private boolean inTx() {
        return tx != null;
    }

    /**
     * @return {@code True} if transaction is implicit.
     */
    private boolean implicitSingle() {
        return tx != null && tx.implicitSingle();
    }

    /**
     * @return {@code True} if transaction is not {@code null} and has invalidate flag set.
     */
    private boolean isInvalidate() {
        return tx != null && tx.isInvalidate();
    }

    /**
     * @return Transaction isolation or {@code null} if no transaction.
     */
    @Nullable private TransactionIsolation isolation() {
        return tx == null ? null : tx.isolation();
    }

    /**
     * Adds entry to future.
     *
     * @param entry Entry to add.
     * @return Lock candidate.
     * @throws GridCacheEntryRemovedException If entry was removed.
     * @throws GridDistributedLockCancelledException If lock is canceled.
     */
    @Nullable public GridCacheMvccCandidate addEntry(GridDhtCacheEntry entry)
        throws GridCacheEntryRemovedException, GridDistributedLockCancelledException {
        if (log.isDebugEnabled())
            log.debug("Adding entry: " + entry);

        if (entry == null)
            return null;

        // Check if the future is timed out.
        if (timedOut)
            return null;

        // Add local lock first, as it may throw GridCacheEntryRemovedException.
        GridCacheMvccCandidate c = entry.addDhtLocal(
            nearNodeId,
            nearLockVer,
            topVer,
            threadId,
            lockVer,
            null,
            timeout,
            /*reenter*/false,
            inTx(),
            implicitSingle(),
            false
        );

        if (c == null && timeout < 0) {
            if (log.isDebugEnabled())
                log.debug("Failed to acquire lock with negative timeout: " + entry);

            onFailed();

            return null;
        }

        synchronized (this) {
            entries.add(c == null || c.reentry() ? null : entry);

            if (c != null && !c.reentry())
                pendingLocks.add(entry.key());
        }

        // Double check if the future has already timed out.
        if (timedOut) {
            entry.removeLock(lockVer);

            return null;
        }

        return c;
    }

    /**
     * Undoes all locks.
     *
     * @param dist If {@code true}, then remove locks from remote nodes as well.
     */
    private void undoLocks(boolean dist) {
        // Transactions will undo during rollback.
        Collection<GridDhtCacheEntry> entriesCp = entriesCopy();

        if (dist && tx == null) {
            cctx.dhtTx().removeLocks(nearNodeId, lockVer, F.viewReadOnly(entriesCp,
                (C1<GridDhtCacheEntry, KeyCacheObject>)GridCacheMapEntry::key), false);
        }
        else {
            if (tx != null) {
                if (tx.setRollbackOnly()) {
                    if (log.isDebugEnabled())
                        log.debug("Marked transaction as rollback only because locks could not be acquired: " + tx);
                }
                else if (log.isDebugEnabled())
                    log.debug("Transaction was not marked rollback-only while locks were not acquired: " + tx);
            }

            for (GridCacheEntryEx e : entriesCp) {
                try {
                    e.removeLock(lockVer);
                }
                catch (GridCacheEntryRemovedException ignored) {
                    while (true) {
                        try {
                            e = cctx.cache().peekEx(e.key());

                            if (e != null)
                                e.removeLock(lockVer);

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignore) {
                            if (log.isDebugEnabled())
                                log.debug("Attempted to remove lock on removed entry (will retry) [ver=" +
                                    lockVer + ", entry=" + e + ']');
                        }
                    }
                }
            }
        }
    }

    /**
     *
     */
    private void onFailed() {
        undoLocks(false);

        onComplete(false, false, true);
    }

    /**
     * @param nodeId Left node ID
     * @return {@code True} if node was in the list.
     */
    @Override public boolean onNodeLeft(UUID nodeId) {
        boolean found = false;

        for (IgniteInternalFuture<?> fut : futures()) {
            MiniFuture f = (MiniFuture)fut;

            if (f.node().id().equals(nodeId)) {
                f.onResult();

                found = true;
            }
        }

        return found;
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    void onResult(UUID nodeId, GridDhtLockResponse res) {
        if (!isDone()) {
            MiniFuture mini = miniFuture(res.miniId());

            if (mini != null) {
                assert mini.node().id().equals(nodeId);

                mini.onResult(res);

                return;
            }

            U.warn(msgLog, "DHT lock fut, failed to find mini future [txId=" + nearLockVer +
                ", dhtTxId=" + lockVer +
                ", inTx=" + inTx() +
                ", node=" + nodeId +
                ", res=" + res +
                ", fut=" + this + ']');
        }
    }

    /**
     * Finds pending mini future by the given mini ID.
     *
     * @param miniId Mini ID to find.
     * @return Mini future.
     */
    private MiniFuture miniFuture(IgniteUuid miniId) {
        // We iterate directly over the futs collection here to avoid copy.
        compoundsReadLock();

        try {
            int size = futuresCountNoLock();

            // Avoid iterator creation.
            for (int i = 0; i < size; i++) {
                MiniFuture mini = (MiniFuture)future(i);

                if (mini.futureId().equals(miniId)) {
                    if (!mini.isDone())
                        return mini;
                    else
                        return null;
                }
            }
        }
        finally {
            compoundsReadUnlock();
        }

        return null;
    }

    /**
     * Sets all local locks as ready. After local locks are acquired, lock requests will be sent to remote nodes.
     * Thus, no reordering will occur for remote locks as they are added after local locks are acquired.
     */
    private void readyLocks() {
        if (log.isDebugEnabled())
            log.debug("Marking local locks as ready for DHT lock future: " + this);

        for (int i = 0; i < entries.size(); i++) {
            while (true) {
                GridDistributedCacheEntry entry = entries.get(i);

                if (entry == null)
                    break; // While.

                try {
                    CacheLockCandidates owners = entry.readyLock(lockVer);

                    if (timeout < 0) {
                        if (owners == null || !owners.hasCandidate(lockVer)) {
                            // We did not send any requests yet.
                            onFailed();

                            return;
                        }
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Current lock owners [entry=" + entry +
                            ", owners=" + owners +
                            ", fut=" + this + ']');
                    }

                    break; // Inner while loop.
                }
                // Possible in concurrent cases, when owner is changed after locks
                // have been released or cancelled.
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to ready lock because entry was removed (will renew).");

                    entries.set(i, (GridDhtCacheEntry)cctx.cache().entryEx(entry.key(), topVer));
                }
            }
        }
    }

    /**
     * @param t Error.
     */
    public void onError(Throwable t) {
        synchronized (this) {
            if (err != null)
                return;

            err = t;
        }

        onComplete(false, false, true);
    }

    /**
     * Callback for whenever entry lock ownership changes.
     *
     * @param entry Entry whose lock ownership changed.
     */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        if (isDone() || (inTx() && (tx.remainingTime() == -1 || tx.isRollbackOnly())))
            return false; // Check other futures.

        if (log.isDebugEnabled())
            log.debug("Received onOwnerChanged() callback [entry=" + entry + ", owner=" + owner + "]");

        if (owner != null && owner.version().equals(lockVer)) {
            boolean isEmpty;

            synchronized (this) {
                if (!pendingLocks.remove(entry.key()))
                    return false;

                isEmpty = pendingLocks.isEmpty();
            }

            if (isEmpty)
                map(entries());

            return true;
        }

        return false;
    }

    /**
     * @return {@code True} if locks have been acquired.
     */
    private synchronized boolean checkLocks() {
        return pendingLocks.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        if (onCancelled())
            onComplete(false, false, true);

        return isCancelled();
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Boolean success, @Nullable Throwable err) {
        try (TraceSurroundings ignored = MTC.support(span)) {
            // Protect against NPE.
            if (success == null) {
                assert err != null;

                success = false;
            }

            assert err == null || !success;
            assert !success || (initialized() && !hasPending()) : "Invalid done callback [success=" + success +
                ", fut=" + this + ']';

            if (log.isDebugEnabled())
                log.debug("Received onDone(..) callback [success=" + success + ", err=" + err + ", fut=" + this + ']');

            // If locks were not acquired yet, delay completion.
            if (isDone() || (err == null && success && !checkLocks()))
                return false;

            synchronized (this) {
                if (this.err == null)
                    this.err = err;
            }

            return onComplete(success, err instanceof NodeStoppingException, true);
        }
    }

    /**
     * Completeness callback.
     *
     * @param success {@code True} if lock was acquired.
     * @param stopping {@code True} if node is stopping.
     * @param unlock {@code True} if locks should be released.
     * @return {@code True} if complete by this operation.
     */
    private synchronized boolean onComplete(boolean success, boolean stopping, boolean unlock) {
        if (log.isDebugEnabled())
            log.debug("Received onComplete(..) callback [success=" + success + ", fut=" + this + ']');

        if (!success && !stopping && unlock)
            undoLocks(true);

        boolean set = false;

        if (tx != null) {
            cctx.tm().txContext(tx);

            set = cctx.tm().setTxTopologyHint(tx.topologyVersionSnapshot());

            if (success)
                tx.clearLockFuture(this);
        }

        try {
            if (err == null && !stopping)
                loadMissingFromStore();
        }
        finally {
            if (set)
                cctx.tm().setTxTopologyHint(null);
        }

        if (super.onDone(success, err)) {
            if (log.isDebugEnabled())
                log.debug("Completing future: " + this);

            // Clean up.
            cctx.mvcc().removeVersionedFuture(this);

            if (timeoutObj != null)
                cctx.time().removeTimeoutObject(timeoutObj);

            return true;
        }

        return false;
    }

    /**
     *
     */
    public void map() {
        try (TraceSurroundings ignored =
                 MTC.supportContinual(span = cctx.kernalContext().tracing().create(TX_DHT_LOCK_MAP, MTC.span()))) {
            if (F.isEmpty(entries)) {
                onComplete(true, false, true);

                return;
            }

            readyLocks();

            if (timeout > 0 && !isDone()) { // Prevent memory leak if future is completed by call to readyLocks.
                timeoutObj = new LockTimeoutObject();

                cctx.time().addTimeoutObject(timeoutObj);
            }
        }
    }

    /**
     *
     * @return {@code True} if future is done.
     */
    private boolean checkDone() {
        if (isDone()) {
            if (log.isDebugEnabled())
                log.debug("Mapping won't proceed because future is done: " + this);

            return true;
        }

        return false;
    }

    /**
     * @param entries Entries.
     */
    private void map(Iterable<GridDhtCacheEntry> entries) {
        synchronized (this) {
            if (mapped)
                return;

            mapped = true;
        }

        try {
            if (log.isDebugEnabled())
                log.debug("Mapping entry for DHT lock future: " + this);

            // Assign keys to primary nodes.
            for (GridDhtCacheEntry entry : entries) {
                try {
                    while (true) {
                        try {
                            cctx.dhtMap(
                                nearNodeId,
                                topVer,
                                entry,
                                tx == null ? lockVer : null,
                                log,
                                dhtMap,
                                null);

                            GridCacheMvccCandidate cand = entry.candidate(lockVer);

                            // Possible in case of lock cancellation.
                            if (cand == null) {
                                onFailed();

                                // Will mark initialized in finally block.
                                return;
                            }

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignore) {
                            if (log.isDebugEnabled())
                                log.debug("Got removed entry when mapping DHT lock future (will retry): " + entry);

                            entry = cctx.dht().entryExx(entry.key(), topVer);
                        }
                    }
                }
                catch (GridDhtInvalidPartitionException e) {
                    assert false : "DHT lock should never get invalid partition [err=" + e + ", fut=" + this + ']';
                }
            }

            if (checkDone())
                return;

            if (log.isDebugEnabled())
                log.debug("Mapped DHT lock future [dhtMap=" + ClusterNodeFunc.nodeIds(dhtMap.keySet()) +
                    ", dhtLockFut=" + this + ']');

            long timeout = inTx() ? tx.remainingTime() : this.timeout;

            synchronized (this) { // Prevents entry removal on concurrent rollback.
                if (checkDone())
                    return;

                // Create mini futures.
                for (Map.Entry<ClusterNode, List<GridDhtCacheEntry>> mapped : dhtMap.entrySet()) {
                    ClusterNode n = mapped.getKey();

                    List<GridDhtCacheEntry> dhtMapping = mapped.getValue();

                    int cnt = F.size(dhtMapping);

                    if (cnt > 0) {
                        assert !n.id().equals(cctx.localNodeId());

                        if (inTx() && tx.remainingTime() == -1)
                            return;

                        MiniFuture fut = new MiniFuture(n, dhtMapping);

                        GridDhtLockRequest req = new GridDhtLockRequest(
                            cctx.cacheId(),
                            nearNodeId,
                            inTx() ? tx.nearXidVersion() : null,
                            threadId,
                            futId,
                            fut.futureId(),
                            lockVer,
                            topVer,
                            inTx(),
                            read,
                            isolation(),
                            isInvalidate(),
                            timeout,
                            cnt,
                            inTx() ? tx.size() : cnt,
                            inTx() ? tx.taskNameHash() : 0,
                            read ? accessTtl : -1L,
                            skipStore,
                            cctx.store().configured(),
                            keepBinary,
                            cctx.deploymentEnabled(),
                            inTx() ? tx.label() : null);

                        try {
                            for (ListIterator<GridDhtCacheEntry> it = dhtMapping.listIterator(); it.hasNext(); ) {
                                GridDhtCacheEntry e = it.next();

                                boolean needVal;

                                try {
                                    // Must unswap entry so that isNewLocked returns correct value.
                                    e.unswap(false);

                                    needVal = e.isNewLocked();

                                    if (needVal) {
                                        List<ClusterNode> owners = cctx.topology().owners(e.partition(),
                                            tx != null ? tx.topologyVersion() : cctx.affinity().affinityTopologyVersion());

                                        // Do not preload if local node is partition owner.
                                        if (owners.contains(cctx.localNode()))
                                            needVal = false;
                                    }

                                    // Skip entry if it is not new and is not present in updated mapping.
                                    if (tx != null && !needVal)
                                        continue;

                                    boolean invalidateRdr = e.readerId(n.id()) != null;

                                    req.addDhtKey(e.key(), invalidateRdr);

                                    if (needVal) {
                                        // Mark last added key as needed to be preloaded.
                                        req.markLastKeyForPreload();

                                        if (tx != null) {
                                            IgniteTxEntry txEntry = tx.entry(e.txKey());

                                            // NOOP entries will be sent to backups on prepare step.
                                            if (txEntry.op() == GridCacheOperation.READ)
                                                txEntry.op(GridCacheOperation.NOOP);
                                        }
                                    }

                                    GridCacheMvccCandidate added = e.candidate(lockVer);

                                    assert added != null;
                                    assert added.dhtLocal();

                                    if (added.ownerVersion() != null)
                                        req.owned(e.key(), added.ownerVersion());
                                }
                                catch (GridCacheEntryRemovedException ex) {
                                    assert false : "Entry cannot become obsolete when DHT local candidate is added " +
                                        "[e=" + e + ", ex=" + ex + ']';
                                }
                            }

                            if (!F.isEmpty(req.keys())) {
                                if (tx != null)
                                    tx.addLockTransactionNode(n);

                                add(fut); // Append new future.

                                cctx.io().send(n, req, cctx.ioPolicy());

                                if (msgLog.isDebugEnabled()) {
                                    msgLog.debug("DHT lock fut, sent request [txId=" + nearLockVer +
                                        ", dhtTxId=" + lockVer +
                                        ", inTx=" + inTx() +
                                        ", nodeId=" + n.id() + ']');
                                }
                            }
                        }
                        catch (IgniteCheckedException e) {
                            // Fail the whole thing.
                            if (e instanceof ClusterTopologyCheckedException)
                                fut.onResult();
                            else {
                                if (msgLog.isDebugEnabled()) {
                                    msgLog.debug("DHT lock fut, failed to send request [txId=" + nearLockVer +
                                        ", dhtTxId=" + lockVer +
                                        ", inTx=" + inTx() +
                                        ", node=" + n.id() +
                                        ", err=" + e + ']');
                                }

                                fut.onResult(e);
                            }
                        }
                    }
                }
            }
        }
        finally {
            markInitialized();
        }
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return futId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        Collection<String> futs = F.viewReadOnly(futures(), (IgniteInternalFuture<?> f) -> {
            MiniFuture m = (MiniFuture)f;

            return "[node=" + m.node().id() + ", loc=" + m.node().isLocal() + ", done=" + f.isDone() + "]";
        });

        Collection<KeyCacheObject> locks;

        synchronized (this) {
            locks = new HashSet<>(pendingLocks);
        }

        return S.toString(GridDhtLockFuture.class, this,
            "innerFuts", futs,
            "pendingLocks", locks,
            "super", super.toString());
    }

    /**
     *
     */
    private void loadMissingFromStore() {
        if (!skipStore && (read || cctx.loadPreviousValue()) && cctx.readThrough() && (needReturnVal || read)) {
            final Map<KeyCacheObject, GridDhtCacheEntry> loadMap = new LinkedHashMap<>();

            final GridCacheVersion ver = version();

            for (GridDhtCacheEntry entry : entries) {
                try {
                    entry.unswap(false);

                    if (!entry.hasValue())
                        loadMap.put(entry.key(), entry);
                }
                catch (GridCacheEntryRemovedException e) {
                    assert false : "Should not get removed exception while holding lock on entry " +
                        "[entry=" + entry + ", e=" + e + ']';
                }
                catch (IgniteCheckedException e) {
                    onDone(e);

                    return;
                }
            }

            try {
                cctx.store().loadAll(
                    null,
                    loadMap.keySet(),
                    (KeyCacheObject key, Object val) -> {
                        // No value loaded from store.
                        if (val == null)
                            return;

                        GridDhtCacheEntry entry0 = loadMap.get(key);

                        try {
                            CacheObject val0 = cctx.toCacheObject(val);

                            long ttl = createTtl;
                            long expireTime;

                            if (ttl == CU.TTL_ZERO)
                                expireTime = CU.expireTimeInPast();
                            else {
                                if (ttl == CU.TTL_NOT_CHANGED)
                                    ttl = CU.TTL_ETERNAL;

                                expireTime = CU.toExpireTime(ttl);
                            }

                            entry0.initialValue(val0,
                                ver,
                                ttl,
                                expireTime,
                                false,
                                topVer,
                                GridDrType.DR_LOAD,
                                true,
                                false);
                        }
                        catch (GridCacheEntryRemovedException e) {
                            assert false : "Should not get removed exception while holding lock on entry " +
                                "[entry=" + entry0 + ", e=" + e + ']';
                        }
                        catch (IgniteCheckedException e) {
                            onDone(e);
                        }
                    });
            }
            catch (IgniteCheckedException e) {
                onDone(e);
            }
        }
    }

    /**
     * Lock request timeout object.
     */
    private class LockTimeoutObject extends GridTimeoutObjectAdapter {
        /**
         * Default constructor.
         */
        LockTimeoutObject() {
            super(timeout);
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            long longOpsDumpTimeout = cctx.tm().longOperationsDumpTimeout();

            synchronized (GridDhtLockFuture.this) {
                if (log.isDebugEnabled() || timeout >= longOpsDumpTimeout) {
                    String msg = dumpPendingLocks();

                    if (log.isDebugEnabled())
                        log.debug(msg);
                    else
                        log.warning(msg);
                }

                timedOut = true;

                // Stop locks and responses processing.
                pendingLocks.clear();

                clear();
            }

            boolean releaseLocks = !(inTx() && cctx.tm().deadlockDetectionEnabled());

            onComplete(false, false, releaseLocks);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LockTimeoutObject.class, this);
        }

        /**
         * NB! Should be called in synchronized block on {@link GridDhtLockFuture} instance.
         *
         * @return String representation of pending locks.
         */
        private String dumpPendingLocks() {
            StringBuilder sb = new StringBuilder();

            sb.append("Transaction tx=").append(tx.getClass().getSimpleName());
            sb.append(" [xid=").append(tx.xid());
            sb.append(", xidVer=").append(tx.xidVersion());
            sb.append(", nearXid=").append(tx.nearXidVersion().asIgniteUuid());
            sb.append(", nearXidVer=").append(tx.nearXidVersion());
            sb.append(", nearNodeId=").append(tx.nearNodeId());
            sb.append(", label=").append(tx.label());
            sb.append("] timed out, can't acquire lock for ");

            Iterator<KeyCacheObject> locks = pendingLocks.iterator();

            boolean found = false;

            while (!found && locks.hasNext()) {
                KeyCacheObject key = locks.next();

                GridCacheEntryEx entry = cctx.cache().entryEx(key, topVer);

                while (true) {
                    try {
                        Collection<GridCacheMvccCandidate> candidates = entry.localCandidates();

                        for (GridCacheMvccCandidate candidate : candidates) {
                            IgniteInternalTx itx = cctx.tm().tx(candidate.version());

                            if (itx != null && candidate.owner() && !candidate.version().equals(tx.xidVersion())) {
                                sb.append("key=").append(key).append(", owner=");
                                sb.append("[xid=").append(itx.xid()).append(", ");
                                sb.append("xidVer=").append(itx.xidVersion()).append(", ");
                                sb.append("nearXid=").append(itx.nearXidVersion().asIgniteUuid()).append(", ");
                                sb.append("nearXidVer=").append(itx.nearXidVersion()).append(", ");
                                sb.append("label=").append(itx.label()).append(", ");
                                sb.append("nearNodeId=").append(candidate.otherNodeId()).append("]");
                                sb.append(", queueSize=").append(candidates.isEmpty() ? 0 : candidates.size() - 1);

                                found = true;

                                break;
                            }
                        }

                        break;
                    }
                    catch (GridCacheEntryRemovedException e) {
                        entry = cctx.cache().entryEx(key, topVer);
                    }
                }
            }

            return sb.toString();
        }
    }

    /**
     * Mini-future for get operations. Mini-futures are only waiting on a single
     * node as opposed to multiple nodes.
     */
    private class MiniFuture extends GridFutureAdapter<Boolean> {
        /** */
        private final IgniteUuid futId = IgniteUuid.randomUuid();

        /** Node. */
        @GridToStringExclude
        private final ClusterNode node;

        /** DHT mapping. */
        @GridToStringInclude
        private final List<GridDhtCacheEntry> dhtMapping;

        /**
         * @param node Node.
         * @param dhtMapping Mapping.
         */
        MiniFuture(ClusterNode node, List<GridDhtCacheEntry> dhtMapping) {
            assert node != null;

            this.node = node;
            this.dhtMapping = dhtMapping;
        }

        /**
         * @return Future ID.
         */
        IgniteUuid futureId() {
            return futId;
        }

        /**
         * @return Node ID.
         */
        public ClusterNode node() {
            return node;
        }

        /**
         * @param e Error.
         */
        void onResult(Throwable e) {
            if (log.isDebugEnabled())
                log.debug("Failed to get future result [fut=" + this + ", err=" + e + ']');

            // Fail.
            onDone(e);
        }

        /**
         */
        void onResult() {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("DHT lock fut, mini future node left [txId=" + nearLockVer +
                    ", dhtTxId=" + lockVer +
                    ", inTx=" + inTx() +
                    ", node=" + node.id() + ']');
            }

            if (tx != null)
                tx.removeMapping(node.id());

            onDone(true);
        }

        /**
         * @param res Result callback.
         */
        void onResult(GridDhtLockResponse res) {
            if (res.error() != null)
                // Fail the whole compound future.
                onError(res.error());
            else {
                Collection<Integer> invalidParts = res.invalidPartitions();

                // Removing mappings for invalid partitions.
                if (!F.isEmpty(invalidParts)) {
                    for (Iterator<GridDhtCacheEntry> it = dhtMapping.iterator(); it.hasNext();) {
                        GridDhtCacheEntry entry = it.next();

                        if (invalidParts.contains(entry.partition())) {
                            it.remove();

                            if (log.isDebugEnabled())
                                log.debug("Removed mapping for entry [nodeId=" + node.id() + ", entry=" + entry +
                                    ", fut=" + GridDhtLockFuture.this + ']');

                            if (tx != null)
                                tx.removeDhtMapping(node.id(), entry);
                            else
                                entry.removeMapping(lockVer, node);
                        }
                    }

                    if (dhtMapping.isEmpty())
                        dhtMap.remove(node);
                }

                boolean replicate = cctx.isDrEnabled();

                boolean rec = cctx.events().isRecordable(EVT_CACHE_REBALANCE_OBJECT_LOADED);

                GridCacheAdapter<?, ?> cache0 = cctx.cache();

                if (cache0.isNear())
                    cache0 = ((GridNearCacheAdapter)cache0).dht();

                synchronized (GridDhtLockFuture.this) { // Prevents entry re-creation on concurrent rollback.
                    if (checkDone())
                        return;

                    for (GridCacheEntryInfo info : res.preloadEntries()) {
                        try {
                            GridCacheEntryEx entry = cache0.entryEx(info.key(), topVer);

                            cctx.shared().database().checkpointReadLock();

                            try {
                                if (entry.initialValue(info.value(),
                                    info.version(),
                                    info.ttl(),
                                    info.expireTime(),
                                    true,
                                    topVer,
                                    replicate ? DR_PRELOAD : DR_NONE,
                                    false,
                                    false)) {
                                    if (rec && !entry.isInternal())
                                        cctx.events().addEvent(entry.partition(), entry.key(), cctx.localNodeId(), null,
                                            null, null, EVT_CACHE_REBALANCE_OBJECT_LOADED, info.value(), true, null,
                                            false, null, null, false);
                                }
                            }
                            finally {
                                cctx.shared().database().checkpointReadUnlock();
                            }
                        }
                        catch (IgniteCheckedException e) {
                            onDone(e);

                            return;
                        }
                        catch (GridCacheEntryRemovedException e) {
                            assert false : "Entry cannot become obsolete when DHT local candidate is added " +
                                "[e=" + e + ", ex=" + e + ']';
                        }
                    }
                }

                // Finish mini future.
                onDone(true);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, "nodeId", node.id(), "super", super.toString());
        }
    }
}
