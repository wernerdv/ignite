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

package org.apache.ignite.internal.processors.task;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeExecutionRejectedException;
import org.apache.ignite.compute.ComputeJobSibling;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskMapAsync;
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.internal.ComputeTaskInternalFuture;
import org.apache.ignite.internal.GridJobExecuteResponse;
import org.apache.ignite.internal.GridJobSiblingImpl;
import org.apache.ignite.internal.GridJobSiblingsRequest;
import org.apache.ignite.internal.GridJobSiblingsResponse;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTaskCancelRequest;
import org.apache.ignite.internal.GridTaskNameHashKey;
import org.apache.ignite.internal.GridTaskSessionImpl;
import org.apache.ignite.internal.GridTaskSessionRequest;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteDeploymentCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.compute.ComputeTaskCancelledCheckedException;
import org.apache.ignite.internal.events.ManagementTaskEvent;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.managers.systemview.walker.ComputeTaskViewWalker;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.processors.job.ComputeJobStatusEnum;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.platform.compute.PlatformFullTask;
import org.apache.ignite.internal.processors.task.monitor.ComputeGridMonitor;
import org.apache.ignite.internal.processors.task.monitor.ComputeTaskStatus;
import org.apache.ignite.internal.processors.task.monitor.ComputeTaskStatusSnapshot;
import org.apache.ignite.internal.util.GridConcurrentFactory;
import org.apache.ignite.internal.util.GridSpinReadWriteLock;
import org.apache.ignite.internal.util.lang.GridPeerDeployAware;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.systemview.view.ComputeTaskView;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyMap;
import static org.apache.ignite.events.EventType.EVT_MANAGEMENT_TASK_STARTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_TASK_SESSION_ATTR_SET;
import static org.apache.ignite.internal.GridTopic.TOPIC_JOB_SIBLINGS;
import static org.apache.ignite.internal.GridTopic.TOPIC_TASK;
import static org.apache.ignite.internal.GridTopic.TOPIC_TASK_CANCEL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isPersistenceEnabled;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.SYS_METRICS;
import static org.apache.ignite.internal.processors.security.SecurityUtils.securitySubjectId;
import static org.apache.ignite.internal.processors.task.TaskExecutionOptions.options;
import static org.apache.ignite.internal.util.lang.ClusterNodeFunc.nodeIds;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_EXECUTE;

/**
 * This class defines task processor.
 */
public class GridTaskProcessor extends GridProcessorAdapter implements IgniteChangeGlobalStateSupport {
    /** */
    public static final String TASKS_VIEW = "tasks";

    /** */
    public static final String TASKS_VIEW_DESC = "Running compute tasks";

    /** Total executed tasks metric name. */
    public static final String TOTAL_EXEC_TASKS = "TotalExecutedTasks";

    /** Wait for 5 seconds to allow discovery to take effect (best effort). */
    private static final long DISCO_TIMEOUT = 5000;

    /** */
    private final Marshaller marsh;

    /** */
    private final ConcurrentMap<IgniteUuid, GridTaskWorker<?, ?>> tasks = GridConcurrentFactory.newMap();

    /** */
    private boolean stopping;

    /** */
    private boolean waiting;

    /** */
    private final GridLocalEventListener discoLsnr;

    /** Total executed tasks metric. */
    private final LongAdderMetric execTasks;

    /** */
    private final GridSpinReadWriteLock lock = new GridSpinReadWriteLock();

    /** Internal metadata cache. */
    private volatile IgniteInternalCache<GridTaskNameHashKey, String> tasksMetaCache;

    /** */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    /**
     * {@code true} if local node has persistent region in configuration and is not a client.
     */
    private final boolean isPersistenceEnabled;

    /**
     * Task statuses update monitors.
     * Guarded by {@link #lock}.
     */
    private final Collection<ComputeGridMonitor> taskStatusMonitors = ConcurrentHashMap.newKeySet();

    /**
     * Snapshots of task statuses.
     * Mapping: {@link ComputeTaskSession#getId} -> task status.
     * Guarded by {@link #lock}.
     */
    private final ConcurrentMap<IgniteUuid, ComputeTaskStatusSnapshot> taskStatusSnapshots = new ConcurrentHashMap<>();

    /**
     * @param ctx Kernal context.
     */
    public GridTaskProcessor(GridKernalContext ctx) {
        super(ctx);

        marsh = ctx.marshaller();

        discoLsnr = new TaskDiscoveryListener();

        MetricRegistryImpl sysreg = ctx.metric().registry(SYS_METRICS);

        execTasks = sysreg.longAdderMetric(TOTAL_EXEC_TASKS, "Total executed tasks.");

        ctx.systemView().registerView(TASKS_VIEW, TASKS_VIEW_DESC,
            new ComputeTaskViewWalker(),
            tasks.entrySet(),
            e -> new ComputeTaskView(e.getKey(), e.getValue()));

        isPersistenceEnabled = !ctx.clientNode() && isPersistenceEnabled(ctx.config());
    }

    /** {@inheritDoc} */
    @Override public void start() {
        ctx.event().addLocalEventListener(discoLsnr, EVT_NODE_FAILED, EVT_NODE_LEFT);

        ctx.io().addMessageListener(TOPIC_JOB_SIBLINGS, new JobSiblingsMessageListener());
        ctx.io().addMessageListener(TOPIC_TASK_CANCEL, new TaskCancelMessageListener());
        ctx.io().addMessageListener(TOPIC_TASK, new JobMessageListener(true));

        ctx.internalSubscriptionProcessor().registerGlobalStateListener(this);

        if (log.isDebugEnabled())
            log.debug("Started task processor.");
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        if (!active)
            return;

        tasksMetaCache = ctx.security().enabled() ? ctx.cache().utilityCache() : null;

        startLatch.countDown();
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        IgniteClientDisconnectedCheckedException err = disconnectedError(reconnectFut);

        for (GridTaskWorker<?, ?> worker : tasks.values())
            worker.finishTask(null, err, false, false);
    }

    /**
     * @param reconnectFut Reconnect future.
     * @return Client disconnected exception.
     */
    private IgniteClientDisconnectedCheckedException disconnectedError(@Nullable IgniteFuture<?> reconnectFut) {
        return new IgniteClientDisconnectedCheckedException(
            reconnectFut != null ? reconnectFut : ctx.cluster().clientReconnectFuture(),
            "Failed to execute task, client node disconnected.");
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        boolean interrupted = false;

        while (true) {
            try {
                if (lock.tryWriteLock(1, TimeUnit.SECONDS))
                    break;
                else {
                    LT.warn(log, "Still waiting to acquire write lock on stop");

                    U.sleep(50);
                }
            }
            catch (IgniteInterruptedCheckedException | InterruptedException e) {
                LT.warn(log, "Stopping thread was interrupted while waiting for write lock (will wait anyway)");

                interrupted = true;
            }
        }

        try {
            stopping = true;

            waiting = !cancel;
        }
        finally {
            lock.writeUnlock();

            if (interrupted)
                Thread.currentThread().interrupt();
        }

        startLatch.countDown();

        int size = tasks.size();

        if (size > 0) {
            if (cancel)
                U.warn(log, "Will cancel unfinished tasks due to stopping of the grid [cnt=" + size + "]");
            else
                U.warn(log, "Will wait for all job responses from worker nodes before stopping grid.");

            for (GridTaskWorker<?, ?> task : tasks.values()) {
                if (!cancel) {
                    try {
                        task.getTaskFuture().get();
                    }
                    catch (ComputeTaskCancelledCheckedException e) {
                        U.warn(log, e.getMessage());
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Task failed: " + task, e);
                    }
                }
                else {
                    for (ClusterNode node : ctx.discovery().nodes(task.getSession().getTopology())) {
                        if (ctx.localNodeId().equals(node.id()))
                            ctx.job().masterLeaveLocal(task.getSession().getId());
                    }

                    task.cancel();

                    Throwable ex =
                        new ComputeTaskCancelledCheckedException("Task cancelled due to stopping of the grid: " + task);

                    task.finishTask(null, ex, false, false);
                }
            }

            U.join(tasks.values(), log);
        }

        // Remove discovery and message listeners.
        ctx.event().removeLocalEventListener(discoLsnr);

        ctx.io().removeMessageListener(TOPIC_JOB_SIBLINGS);
        ctx.io().removeMessageListener(TOPIC_TASK_CANCEL);

        // Set waiting flag to false to make sure that we do not get
        // listener notifications any more.
        if (!cancel) {
            lock.writeLock();

            try {
                waiting = false;
            }
            finally {
                lock.writeUnlock();
            }
        }

        assert tasks.isEmpty();

        if (log.isDebugEnabled())
            log.debug("Finished executing task processor onKernalStop() callback.");
    }

    /**
     * @return Task metadata cache.
     */
    private IgniteInternalCache<GridTaskNameHashKey, String> taskMetaCache() {
        assert ctx.security().enabled();

        if (tasksMetaCache == null)
            U.awaitQuiet(startLatch);

        return tasksMetaCache;
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        if (log.isDebugEnabled())
            log.debug("Stopped task processor.");
    }

    /**
     * Gets currently used deployments.
     *
     * @return Currently used deployments.
     */
    public Collection<GridDeployment> getUsedDeployments() {
        return F.viewReadOnly(tasks.values(), new C1<GridTaskWorker<?, ?>, GridDeployment>() {
            @Override public GridDeployment apply(GridTaskWorker<?, ?> w) {
                return w.getDeployment();
            }
        });
    }

    /**
     * Gets currently used deployments mapped by task name or aliases.
     *
     * @return Currently used deployments.
     */
    public Map<String, GridDeployment> getUsedDeploymentMap() {
        Map<String, GridDeployment> deps = new HashMap<>();

        for (GridTaskWorker w : tasks.values()) {
            GridTaskSessionImpl ses = w.getSession();

            deps.put(ses.getTaskClassName(), w.getDeployment());

            if (ses.getTaskName() != null && ses.getTaskClassName().equals(ses.getTaskName()))
                deps.put(ses.getTaskName(), w.getDeployment());
        }

        return deps;
    }

    /**
     * @param taskCls Task class.
     * @param arg Optional execution argument.
     * @return Task future.
     * @param <T> Task argument type.
     * @param <R> Task return value type.
     */
    public <T, R> ComputeTaskInternalFuture<R> execute(Class<? extends ComputeTask<T, R>> taskCls, @Nullable T arg) {
        return execute(taskCls, arg, options());
    }

    /**
     * @param taskCls Task class.
     * @param arg Optional execution argument.
     * @param opts Task execution options.
     * @return Task future.
     * @param <T> Task argument type.
     * @param <R> Task return value type.
     */
    public <T, R> ComputeTaskInternalFuture<R> execute(
        Class<? extends ComputeTask<T, R>> taskCls, 
        @Nullable T arg,
        TaskExecutionOptions opts
    ) {
        assert taskCls != null;

        lock.readLock();

        try {
            if (stopping)
                throw new IllegalStateException("Failed to execute task due to grid shutdown: " + taskCls);

            return startTask(null, taskCls, null, IgniteUuid.fromUuid(ctx.localNodeId()), arg, opts);
        }
        finally {
            lock.readUnlock();
        }
    }

    /**
     * @param task Actual task.
     * @param arg Optional task argument.
     * @return Task future.
     * @param <T> Task argument type.
     * @param <R> Task return value type.
     */
    public <T, R> ComputeTaskInternalFuture<R> execute(ComputeTask<T, R> task, @Nullable T arg) {
        return execute(task, arg, options());
    }

    /**
     * @param task Actual task.
     * @param arg Optional task argument.
     * @param opts Task execution options.
     * @return Task future.
     * @param <T> Task argument type.
     * @param <R> Task return value type.
     */
    public <T, R> ComputeTaskInternalFuture<R> execute(ComputeTask<T, R> task, @Nullable T arg, TaskExecutionOptions opts) {
        lock.readLock();

        try {
            if (stopping)
                throw new IllegalStateException("Failed to execute task due to grid shutdown: " + task);

            return startTask(null, null, task, IgniteUuid.fromUuid(ctx.localNodeId()), arg, opts);
        }
        finally {
            lock.readUnlock();
        }
    }

    /**
     * Resolves task name by task name hash.
     *
     * @param taskNameHash Task name hash.
     * @return Task name or {@code null} if not found.
     */
    public String resolveTaskName(int taskNameHash) {
        assert !isPersistenceEnabled || !ctx.cache().context().database().checkpointLockIsHeldByThread() :
            "Resolving a task name should not be executed under the checkpoint lock.";

        if (taskNameHash == 0)
            return null;

        assert ctx.security().enabled();

        try {
            return taskMetaCache().localPeek(
                new GridTaskNameHashKey(taskNameHash), null);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param taskName Task name.
     * @param arg Optional execution argument.
     * @return Task future.
     * @param <T> Task argument type.
     * @param <R> Task return value type.
     */
    public <T, R> ComputeTaskInternalFuture<R> execute(String taskName, @Nullable T arg) {
        return execute(taskName, arg, options());
    }

    /**
     * @param taskName Task name.
     * @param arg Optional execution argument.
     * @param opts Task execution options.
     * @return Task future.
     * @param <T> Task argument type.
     * @param <R> Task return value type.
     */
    public <T, R> ComputeTaskInternalFuture<R> execute(String taskName, @Nullable T arg, @Nullable TaskExecutionOptions opts) {
        assert taskName != null;

        lock.readLock();

        try {
            if (stopping)
                throw new IllegalStateException("Failed to execute task due to grid shutdown: " + taskName);

            return startTask(taskName, null, null, IgniteUuid.fromUuid(ctx.localNodeId()), arg, opts);
        }
        finally {
            lock.readUnlock();
        }
    }

    /**
     * @param taskName Task name.
     * @param taskCls Task class.
     * @param task Task.
     * @param arg Optional task argument.
     * @param opts Task execution options.
     * @return Task future.
     */
    private <T, R> ComputeTaskInternalFuture<R> startTask(
        @Nullable String taskName,
        @Nullable Class<?> taskCls,
        @Nullable ComputeTask<T, R> task,
        IgniteUuid sesId,
        @Nullable T arg,
        TaskExecutionOptions opts
    ) {
        assert sesId != null;

        authorizeUserTask(taskName, taskCls, task, opts);

        assert opts.timeout() >= 0;

        long timeout0 = opts.timeout() == 0 ? Long.MAX_VALUE : opts.timeout();

        long startTime = U.currentTimeMillis();

        long endTime = timeout0 + startTime;

        // Account for overflow.
        if (endTime < 0)
            endTime = Long.MAX_VALUE;

        IgniteCheckedException deployEx = null;
        GridDeployment dep = null;

        // User provided task name.
        if (taskName != null) {
            assert taskCls == null;
            assert task == null;

            try {
                dep = ctx.deploy().getDeployment(taskName);

                if (dep == null)
                    throw new IgniteDeploymentCheckedException("Unknown task name or failed to auto-deploy " +
                        "task (was task (re|un)deployed?): " + taskName);

                IgniteBiTuple<Class<?>, Throwable> cls = dep.deployedClass(taskName);

                if (cls.get1() == null)
                    throw new IgniteDeploymentCheckedException("Unknown task name or failed to auto-deploy " +
                        "task (was task (re|un)deployed?) [taskName=" + taskName + ", dep=" + dep + ']', cls.get2());

                taskCls = cls.get1();

                if (!ComputeTask.class.isAssignableFrom(taskCls))
                    throw new IgniteCheckedException("Failed to auto-deploy task (deployed class is not a task) " +
                        "[taskName=" +
                        taskName + ", depCls=" + taskCls + ']');
            }
            catch (IgniteCheckedException e) {
                deployEx = e;
            }
        }
        // Deploy user task class.
        else if (taskCls != null) {
            assert task == null;

            try {
                // Implicit deploy.
                dep = ctx.deploy().deploy(taskCls, U.detectClassLoader(taskCls));

                if (dep == null)
                    throw new IgniteDeploymentCheckedException("Failed to auto-deploy task " +
                        "(was task (re|un)deployed?): " + taskCls);

                taskName = taskName(dep, taskCls, opts);
            }
            catch (IgniteCheckedException e) {
                taskName = taskCls.getName();

                deployEx = e;
            }
        }
        // Deploy user task.
        else if (task != null) {
            try {
                ClassLoader ldr;

                Class<?> cls;

                if (task instanceof GridPeerDeployAware) {
                    GridPeerDeployAware depAware = (GridPeerDeployAware)task;

                    cls = depAware.deployClass();
                    ldr = depAware.classLoader();

                    // Set proper class name to make peer-loading possible.
                    taskCls = cls;
                }
                else {
                    taskCls = task.getClass();

                    assert ComputeTask.class.isAssignableFrom(taskCls);

                    cls = task.getClass();
                    ldr = U.detectClassLoader(cls);
                }

                // Explicit deploy.
                dep = ctx.deploy().deploy(cls, ldr);

                if (dep == null)
                    throw new IgniteDeploymentCheckedException("Failed to auto-deploy task " +
                        "(was task (re|un)deployed?): " + cls);

                taskName = taskName(dep, taskCls, opts);
            }
            catch (IgniteCheckedException e) {
                taskName = task.getClass().getName();

                deployEx = e;
            }
        }

        assert taskName != null;

        if (log.isDebugEnabled())
            log.debug("Task deployment: " + dep);

        boolean fullSup = (dep != null && taskCls != null &&
            dep.annotation(taskCls, ComputeTaskSessionFullSupport.class) != null) ||
            (task instanceof PlatformFullTask && ((PlatformFullTask)task).taskSessionFullSupport());

        Collection<UUID> top = null;

        final IgnitePredicate<ClusterNode> topPred = opts.projectionPredicate();

        if (topPred == null) {
            final Collection<ClusterNode> nodes = opts.projection();

            top = nodes != null ? nodeIds(nodes) : null;
        }

        boolean internal = false;

        if (dep == null || taskCls == null)
            assert deployEx != null;
        else
            internal = dep.internalTask(task, taskCls);

        // Creates task session with task name and task version.
        GridTaskSessionImpl ses = ctx.session().createTaskSession(
            sesId,
            ctx.localNodeId(),
            taskName,
            dep,
            taskCls == null ? null : taskCls.getName(),
            top,
            topPred,
            startTime,
            endTime,
            Collections.emptyList(),
            emptyMap(),
            fullSup,
            internal,
            opts.executor(),
            ctx.security().securityContext()
        );

        ComputeTaskInternalFuture<R> fut = new ComputeTaskInternalFuture<>(ses, ctx);

        IgniteCheckedException securityEx = null;

        if (ctx.security().enabled() && deployEx == null && !dep.internalTask(task, taskCls)) {
            try {
                saveTaskMetadata(taskName);
            }
            catch (IgniteCheckedException e) {
                securityEx = e;
            }
        }

        if (deployEx == null && securityEx == null) {
            if (dep == null || !dep.acquire())
                handleException(new IgniteDeploymentCheckedException("Task not deployed: " + ses.getTaskName()), fut);
            else {
                GridTaskWorker<?, ?> taskWorker = new GridTaskWorker<>(
                    ctx,
                    arg,
                    ses,
                    fut,
                    taskCls,
                    task,
                    dep,
                    new TaskEventListener(),
                    opts,
                    securitySubjectId(ctx));

                GridTaskWorker<?, ?> taskWorker0 = tasks.putIfAbsent(sesId, taskWorker);

                assert taskWorker0 == null : "Session ID is not unique: " + sesId;

                if (ctx.event().isRecordable(EVT_MANAGEMENT_TASK_STARTED) && dep.visorManagementTask(task, taskCls)) {
                    VisorTaskArgument visorTaskArg = (VisorTaskArgument)arg;

                    Event evt = new ManagementTaskEvent(
                        ctx.discovery().localNode(),
                        visorTaskArg != null && visorTaskArg.getArgument() != null
                            ? visorTaskArg.getArgument().toString() : "[]",
                        EVT_MANAGEMENT_TASK_STARTED,
                        ses.getId(),
                        taskName,
                        taskCls == null ? null : taskCls.getName(),
                        false,
                        securitySubjectId(ctx),
                        visorTaskArg
                    );

                    ctx.event().record(evt);
                }

                if (!ctx.clientDisconnected()) {
                    if (dep.annotation(taskCls, ComputeTaskMapAsync.class) != null) {
                        try {
                            // Start task execution in another thread.
                            if (opts.isSystemTask())
                                ctx.pools().getSystemExecutorService().execute(taskWorker);
                            else
                                ctx.pools().getExecutorService().execute(taskWorker);
                        }
                        catch (RejectedExecutionException e) {
                            tasks.remove(sesId);

                            release(dep);

                            handleException(new ComputeExecutionRejectedException("Failed to execute task " +
                                "due to thread pool execution rejection: " + taskName, e), fut);
                        }
                    }
                    else
                        taskWorker.run();
                }
                else
                    taskWorker.finishTask(null, disconnectedError(null));
            }
        }
        else {
            if (deployEx != null)
                handleException(deployEx, fut);
            else
                handleException(securityEx, fut);
        }

        return fut;
    }

    /**
     * @param sesId Task's session id.
     * @return A {@link ComputeTaskInternalFuture} instance or {@code null} if no such task found.
     */
    @Nullable public <R> ComputeTaskInternalFuture<R> taskFuture(IgniteUuid sesId) {
        GridTaskWorker<?, ?> taskWorker = tasks.get(sesId);

        return taskWorker != null ? (ComputeTaskInternalFuture<R>)taskWorker.getTaskFuture() : null;
    }

    /**
     * @return Active task futures.
     */
    @SuppressWarnings("unchecked")
    public <R> Map<IgniteUuid, ComputeTaskFuture<R>> taskFutures() {
        Map<IgniteUuid, ComputeTaskFuture<R>> res = U.newHashMap(tasks.size());

        for (GridTaskWorker taskWorker : tasks.values()) {
            ComputeTaskInternalFuture<R> fut = taskWorker.getTaskFuture();

            res.put(fut.getTaskSession().getId(), fut.publicFuture());
        }

        return res;
    }

    /**
     * Gets task name for a task class. It firstly checks
     * {@link @ComputeTaskName} annotation, then thread context
     * map. If both are empty, class name is returned.
     *
     * @param dep Deployment.
     * @param cls Class.
     * @param opts Task execution options.
     * @return Task name.
     * @throws IgniteCheckedException If {@link @ComputeTaskName} annotation is found, but has empty value.
     */
    private String taskName(GridDeployment dep, Class<?> cls, TaskExecutionOptions opts) throws IgniteCheckedException {
        assert dep != null;
        assert cls != null;
        assert opts != null;

        String taskName;

        ComputeTaskName ann = dep.annotation(cls, ComputeTaskName.class);

        if (ann != null) {
            taskName = ann.value();

            if (F.isEmpty(taskName))
                throw new IgniteCheckedException("Task name specified by @ComputeTaskName annotation" +
                    " cannot be empty for class: " + cls);
        }
        else
            taskName = opts.name().orElse(cls.getName());

        return taskName;
    }

    /**
     * Saves task name metadata to utility cache.
     *
     * @param taskName Task name.
     * @throws IgniteCheckedException If failed.
     */
    private void saveTaskMetadata(String taskName) throws IgniteCheckedException {
        assert ctx.security().enabled();

        int nameHash = taskName.hashCode();

        // 0 is reserved for no task.
        if (nameHash == 0)
            nameHash = 1;

        GridTaskNameHashKey key = new GridTaskNameHashKey(nameHash);

        IgniteInternalCache<GridTaskNameHashKey, String> tasksMetaCache = taskMetaCache();

        String existingName = tasksMetaCache.get(key);

        if (existingName == null)
            existingName = tasksMetaCache.getAndPutIfAbsent(key, taskName);

        if (existingName != null && !Objects.equals(existingName, taskName))
            throw new IgniteCheckedException("Task name hash collision for security-enabled node " +
                "[taskName=" + taskName +
                ", existing taskName=" + existingName + ']');
    }

    /**
     * @param dep Deployment to release.
     */
    private void release(GridDeployment dep) {
        assert dep != null;

        dep.release();

        if (dep.obsolete())
            ctx.resource().onUndeployed(dep);
    }

    /**
     * @param ex Exception.
     * @param fut Task future.
     * @param <R> Result type.
     */
    private <R> void handleException(Throwable ex, ComputeTaskInternalFuture<R> fut) {
        assert ex != null;
        assert fut != null;

        fut.onDone(ex);
    }

    /**
     * @param ses Task session.
     * @param attrs Attributes.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void setAttributes(GridTaskSessionImpl ses, Map<?, ?> attrs) throws IgniteCheckedException {
        long timeout = ses.getEndTime() - U.currentTimeMillis();

        if (timeout <= 0) {
            U.warn(log, "Task execution timed out (remote session attributes won't be set): " + ses);

            return;
        }

        // If setting from task or future.
        if (log.isDebugEnabled())
            log.debug("Setting session attribute(s) from task or future: " + ses);

        sendSessionAttributes(attrs, ses);
    }

    /**
     * This method will make the best attempt to send attributes to all jobs.
     *
     * @param attrs Deserialized session attributes.
     * @param ses Task session.
     * @throws IgniteCheckedException If send to any of the jobs failed.
     */
    @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter"})
    private void sendSessionAttributes(Map<?, ?> attrs, GridTaskSessionImpl ses)
        throws IgniteCheckedException {
        assert attrs != null;
        assert ses != null;

        Collection<ComputeJobSibling> siblings = ses.getJobSiblings();

        GridIoManager commMgr = ctx.io();

        long timeout = ses.getEndTime() - U.currentTimeMillis();

        if (timeout <= 0) {
            U.warn(log, "Session attributes won't be set due to task timeout: " + attrs);

            return;
        }

        Set<UUID> rcvrs = new HashSet<>();

        UUID locNodeId = ctx.localNodeId();

        synchronized (ses) {
            if (ses.isClosed()) {
                if (log.isDebugEnabled())
                    log.debug("Setting session attributes on closed session (will ignore): " + ses);

                return;
            }

            ses.setInternal(attrs);

            // Do this inside of synchronization block, so every message
            // ID will be associated with a certain session state.
            for (ComputeJobSibling s : siblings) {
                GridJobSiblingImpl sib = (GridJobSiblingImpl)s;

                UUID nodeId = sib.nodeId();

                if (!nodeId.equals(locNodeId) && !sib.isJobDone())
                    rcvrs.add(nodeId);
            }
        }

        if (ctx.event().isRecordable(EVT_TASK_SESSION_ATTR_SET)) {
            Event evt = new TaskEvent(
                ctx.discovery().localNode(),
                "Changed attributes: " + attrs,
                EVT_TASK_SESSION_ATTR_SET,
                ses.getId(),
                ses.getTaskName(),
                ses.getTaskClassName(),
                false,
                null);

            ctx.event().record(evt);
        }

        notifyTaskStatusMonitors(ComputeTaskStatus.snapshot(ses), false);

        IgniteCheckedException ex = null;

        // Every job gets an individual message to keep track of ghost requests.
        for (ComputeJobSibling s : ses.getJobSiblings()) {
            GridJobSiblingImpl sib = (GridJobSiblingImpl)s;

            UUID nodeId = sib.nodeId();

            if (locNodeId.equals(nodeId)) {
                // Local job notification.
                ctx.job().onChangeTaskAttributes(ses.getId(), s.getJobId(), attrs);
            }
            else if (rcvrs.remove(nodeId)) {
                // Pair can be null if job is finished.
                ClusterNode node = ctx.discovery().node(nodeId);

                // Check that node didn't change (it could happen in case of failover).
                if (node != null) {
                    boolean loc = node.id().equals(ctx.localNodeId()) && !ctx.config().isMarshalLocalJobs();

                    GridTaskSessionRequest req = new GridTaskSessionRequest(
                        ses.getId(),
                        s.getJobId(),
                        loc ? null : U.marshal(marsh, attrs),
                        attrs
                    );

                    // Make sure to go through IO manager always, since order
                    // should be preserved here.
                    try {
                        commMgr.sendOrderedMessage(
                            node,
                            sib.jobTopic(),
                            req,
                            SYSTEM_POOL,
                            timeout,
                            false);
                    }
                    catch (IgniteCheckedException e) {
                        node = e instanceof ClusterTopologyCheckedException ? null : ctx.discovery().node(nodeId);

                        if (node != null) {
                            try {
                                // Since communication on remote node may stop before
                                // we get discovery notification, we give ourselves the
                                // best effort to detect it.
                                Thread.sleep(DISCO_TIMEOUT);
                            }
                            catch (InterruptedException ignore) {
                                U.warn(log, "Got interrupted while sending session attributes.");
                            }

                            node = ctx.discovery().node(nodeId);
                        }

                        String err = "Failed to send session attribute request message to node " +
                            "(normal case if node left grid) [node=" + node + ", req=" + req + ']';

                        if (node != null)
                            U.warn(log, err);
                        else if (log.isDebugEnabled())
                            log.debug(err);

                        if (ex == null)
                            ex = e;
                    }
                }
            }
        }

        if (ex != null)
            throw ex;
    }

    /**
     * @param nodeId Node ID.
     * @param msg Execute response message.
     */
    public void processJobExecuteResponse(UUID nodeId, GridJobExecuteResponse msg) {
        assert nodeId != null;
        assert msg != null;

        lock.readLock();

        try {
            GridTaskWorker<?, ?> task = tasks.get(msg.getSessionId());

            if (stopping && !waiting) {
                U.warn(log, "Received job execution response while stopping grid (will ignore): " + msg
                    + tryResolveTaskName(task));

                return;
            }

            if (task == null) {
                if (log.isDebugEnabled())
                    log.debug("Received job execution response for unknown task (was task already reduced?): " + msg);

                return;
            }

            if (log.isDebugEnabled())
                log.debug("Received grid job response message [msg=" + msg + ", nodeId=" + nodeId + ']');

            task.onResponse(msg);
        }
        finally {
            lock.readUnlock();
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Task session request.
     */
    private void processTaskSessionRequest(UUID nodeId, GridTaskSessionRequest msg) {
        assert nodeId != null;
        assert msg != null;

        lock.readLock();

        try {
            GridTaskWorker<?, ?> task = tasks.get(msg.getSessionId());

            if (stopping && !waiting) {
                U.warn(log, "Received task session request while stopping grid (will ignore): " + msg
                    + tryResolveTaskName(task));

                return;
            }

            if (task == null) {
                if (log.isDebugEnabled())
                    log.debug("Received task session request for unknown task (was task already reduced?): " + msg);

                return;
            }

            boolean loc = ctx.localNodeId().equals(nodeId) && !ctx.config().isMarshalLocalJobs();

            Map<?, ?> attrs = loc ? msg.getAttributes() :
                U.<Map<?, ?>>unmarshal(marsh, msg.getAttributesBytes(),
                    U.resolveClassLoader(task.getTask().getClass().getClassLoader(), ctx.config()));

            GridTaskSessionImpl ses = task.getSession();

            sendSessionAttributes(attrs, ses);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to deserialize session request: " + msg, e);
        }
        finally {
            lock.readUnlock();
        }
    }

    /**
     * Handles user cancellation.
     *
     * @param sesId Session ID.
     * @return Whether task was cancelled by this call.
     */
    public boolean cancel(IgniteUuid sesId) {
        assert sesId != null;

        lock.readLock();

        try {
            GridTaskWorker<?, ?> task = tasks.get(sesId);

            if (stopping && !waiting) {
                U.warn(log, "Attempt to cancel task while stopping grid (will ignore): " + sesId
                    + tryResolveTaskName(task));

                return false;
            }

            if (task == null) {
                if (log.isDebugEnabled())
                    log.debug("Attempt to cancel unknown task (was task already reduced?): " + sesId);

                return false;
            }

            return task.cancelTask();
        }
        finally {
            lock.readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        onKernalStart(true);
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        // No-op.
    }

    /**
     * Resets processor metrics.
     */
    public void resetMetrics() {
        execTasks.reset();
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> Task processor memory stats [igniteInstanceName=" + ctx.igniteInstanceName() + ']');
        X.println(">>>  tasksSize: " + tasks.size());
    }

    /**
     * Listener for individual task events.
     */
    private class TaskEventListener implements GridTaskEventListener {
        /** */
        private final GridMessageListener msgLsnr = new JobMessageListener(false);

        /** {@inheritDoc} */
        @Override public void onTaskStarted(GridTaskWorker<?, ?> worker) {
            // Register for timeout notifications.
            if (worker.endTime() < Long.MAX_VALUE)
                ctx.timeout().addTimeoutObject(worker);

            GridTaskSessionImpl ses = worker.getSession();

            notifyTaskStatusMonitors(ComputeTaskStatus.snapshot(ses), false);
        }

        /** {@inheritDoc} */
        @Override public void onJobsMapped(GridTaskWorker<?, ?> worker) {
            GridTaskSessionImpl ses = worker.getSession();

            notifyTaskStatusMonitors(ComputeTaskStatus.snapshot(ses), false);
        }

        /** {@inheritDoc} */
        @Override public void onJobSend(GridTaskWorker<?, ?> worker, GridJobSiblingImpl sib) {
            if (worker.getSession().isFullSupport())
                // Listener is stateless, so same listener can be reused for all jobs.
                ctx.io().addMessageListener(sib.taskTopic(), msgLsnr);
        }

        /** {@inheritDoc} */
        @Override public void onJobFailover(GridTaskWorker<?, ?> worker, GridJobSiblingImpl sib, UUID nodeId) {
            GridIoManager ioMgr = ctx.io();

            // Remove message ID registration and old listener.
            if (worker.getSession().isFullSupport()) {
                ioMgr.removeMessageListener(sib.taskTopic(), msgLsnr);

                synchronized (worker.getSession()) {
                    // Reset ID on sibling prior to sending request.
                    sib.nodeId(nodeId);
                }

                // Register new listener on new topic.
                ioMgr.addMessageListener(sib.taskTopic(), msgLsnr);
            }
            else {
                // Update node ID only in case attributes are not enabled.
                synchronized (worker.getSession()) {
                    // Reset ID on sibling prior to sending request.
                    sib.nodeId(nodeId);
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void onJobFinished(GridTaskWorker<?, ?> worker, GridJobSiblingImpl sib) {
            // Mark sibling finished for the purpose of setting session attributes.
            synchronized (worker.getSession()) {
                sib.onJobDone();
            }
        }

        /** {@inheritDoc} */
        @Override public void onTaskFinished(GridTaskWorker<?, ?> worker, @Nullable Throwable err) {
            GridTaskSessionImpl ses = worker.getSession();

            if (ses.isFullSupport()) {
                synchronized (worker.getSession()) {
                    worker.getSession().onClosed();
                }

                ctx.checkpoint().onSessionEnd(ses, false);

                // Delete session altogether.
                ctx.session().removeSession(ses.getId());
            }

            boolean rmv = tasks.remove(worker.getTaskSessionId(), worker);

            assert rmv;

            // Unregister from timeout notifications.
            if (worker.endTime() < Long.MAX_VALUE)
                ctx.timeout().removeTimeoutObject(worker);

            release(worker.getDeployment());

            if (!worker.isInternal())
                execTasks.increment();

            // Unregister job message listener from all job topics.
            if (ses.isFullSupport()) {
                try {
                    for (ComputeJobSibling sibling : worker.getSession().getJobSiblings()) {
                        GridJobSiblingImpl s = (GridJobSiblingImpl)sibling;

                        ctx.io().removeMessageListener(s.taskTopic(), msgLsnr);
                    }
                }
                catch (IgniteException e) {
                    U.error(log, "Failed to unregister job communication message listeners and counters.", e);
                }
            }

            if (ctx.performanceStatistics().enabled()) {
                ctx.performanceStatistics().task(
                    ses.getId(),
                    ses.getTaskName(),
                    ses.getStartTime(),
                    U.currentTimeMillis() - ses.getStartTime(),
                    worker.affPartId());
            }

            notifyTaskStatusMonitors(ComputeTaskStatus.onFinishTask(worker.getSession(), err), true);
        }
    }

    /**
     * Handles job execution responses and session requests.
     */
    private class JobMessageListener implements GridMessageListener {
        /** */
        private final boolean jobResOnly;

        /**
         * @param jobResOnly {@code True} if this listener is allowed to process
         *      job responses only (for tasks with disabled sessions).
         */
        private JobMessageListener(boolean jobResOnly) {
            this.jobResOnly = jobResOnly;
        }

        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
            if (msg instanceof GridJobExecuteResponse)
                processJobExecuteResponse(nodeId, (GridJobExecuteResponse)msg);
            else if (jobResOnly)
                U.warn(log, "Received message of type other than job response: " + msg);
            else if (msg instanceof GridTaskSessionRequest)
                processTaskSessionRequest(nodeId, (GridTaskSessionRequest)msg);
            else
                U.warn(log, "Received message of unknown type: " + msg);
        }
    }

    /**
     * Listener to node discovery events.
     */
    private class TaskDiscoveryListener implements GridLocalEventListener {
        /** {@inheritDoc} */
        @Override public void onEvent(Event evt) {
            assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT;

            final UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

            ctx.closure().runLocalSafe(new GridPlainRunnable() {
                @Override public void run() {
                    if (!lock.tryReadLock())
                        return;

                    try {
                        for (GridTaskWorker<?, ?> task : tasks.values())
                            task.onNodeLeft(nodeId);
                    }
                    finally {
                        lock.readUnlock();
                    }
                }
            }, false);
        }
    }

    /**
     *
     */
    private class JobSiblingsMessageListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
            if (!(msg instanceof GridJobSiblingsRequest)) {
                U.warn(log, "Received unexpected message instead of siblings request: " + msg);

                return;
            }

            lock.readLock();

            try {
                GridJobSiblingsRequest req = (GridJobSiblingsRequest)msg;

                GridTaskWorker<?, ?> worker = tasks.get(req.sessionId());

                if (stopping && !waiting) {
                    U.warn(log, "Received job siblings request while stopping grid (will ignore): " + msg
                        + tryResolveTaskName(worker));

                    return;
                }

                Collection<ComputeJobSibling> siblings;

                if (worker != null) {
                    try {
                        siblings = worker.getSession().getJobSiblings();
                    }
                    catch (IgniteException e) {
                        U.error(log, "Failed to get job siblings [request=" + msg +
                            ", ses=" + worker.getSession() + ']', e);

                        siblings = null;
                    }
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Received job siblings request for unknown or finished task (will ignore): " + msg);

                    siblings = null;
                }

                try {
                    Object topic = req.topic();

                    if (topic == null) {
                        assert req.topicBytes() != null;

                        topic = U.unmarshal(marsh, req.topicBytes(), U.resolveClassLoader(ctx.config()));
                    }

                    boolean loc = ctx.localNodeId().equals(nodeId);

                    ctx.io().sendToCustomTopic(nodeId, topic,
                        new GridJobSiblingsResponse(
                            loc ? siblings : null,
                            loc ? null : U.marshal(marsh, siblings)),
                        SYSTEM_POOL);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to send job sibling response.", e);
                }
            }
            finally {
                lock.readUnlock();
            }
        }
    }

    /**
     * Listener for task cancel requests.
     */
    private class TaskCancelMessageListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
            assert msg != null;

            if (!(msg instanceof GridTaskCancelRequest)) {
                U.warn(log, "Received unexpected message instead of task cancel request: " + msg);

                return;
            }

            GridTaskCancelRequest req = (GridTaskCancelRequest)msg;

            lock.readLock();

            try {
                GridTaskWorker<?, ?> gridTaskWorker = tasks.get(req.sessionId());

                if (stopping && !waiting) {
                    U.warn(log, "Received task cancel request while stopping grid (will ignore): " + msg
                        + tryResolveTaskName(gridTaskWorker));

                    return;
                }

                if (gridTaskWorker != null) {
                    try {
                        gridTaskWorker.getTaskFuture().cancel();
                    }
                    catch (IgniteCheckedException e) {
                        log.warning("Failed to cancel task: " + gridTaskWorker.getTask(), e);
                    }
                }
            }
            finally {
                lock.readUnlock();
            }
        }
    }

    /**
     * Tries to get task name in appended form(after ', ').
     * If cannot take task name - returns empty String.
     *
     * @param task Task to get name.
     * @return Task name or empty string.
     */
    @NotNull private static String tryResolveTaskName(@Nullable GridTaskWorker<?, ?> task) {
        return task != null && task.getSession() != null
            ? ", task name: " + task.getSession().getTaskName()
            : "";
    }

    /**
     * Subscription to update the status of tasks.
     *
     * <p>NOTE: {@link ComputeGridMonitor#processStatusSnapshots} will be called only on subscription,
     * then only {@link ComputeGridMonitor#processStatusChange} will be called.
     *
     * @param monitor Task status update monitor.
     * @throws NodeStoppingException If the node is stopped.
     */
    public void listenStatusUpdates(ComputeGridMonitor monitor) throws NodeStoppingException {
        lock.writeLock();

        try {
            if (stopping)
                throw new NodeStoppingException("Failed to add monitor due to grid shutdown: " + monitor);

            taskStatusMonitors.add(monitor);

            try {
                monitor.processStatusSnapshots(taskStatusSnapshots.values());
            }
            catch (Throwable t) {
                log.error("Error processing snapshots of task statuses: " + monitor, t);
            }
        }
        finally {
            lock.writeUnlock();
        }
    }

    /**
     * Unsubscribe to update the status of tasks.
     *
     * @param monitor Task status update monitor.
     */
    public void stopListenStatusUpdates(ComputeGridMonitor monitor) {
        lock.writeLock();

        try {
            taskStatusMonitors.remove(monitor);
        }
        finally {
            lock.writeUnlock();
        }
    }

    /**
     * Guarded by {@link #lock} for atomic update of the {@link #taskStatusSnapshots}
     * and notifying {@link #taskStatusMonitors} about task changes.
     *
     * @param snapshotChanges Changes to task status.
     * @param remove {@code True} if it is necessary to remove the {@code snapshotChanges} from
     *      {@link #taskStatusSnapshots}, otherwise it will be updated.
     */
    private void notifyTaskStatusMonitors(ComputeTaskStatusSnapshot snapshotChanges, boolean remove) {
        lock.readLock();

        try {
            if (remove)
                taskStatusSnapshots.remove(snapshotChanges.sessionId());
            else
                taskStatusSnapshots.put(snapshotChanges.sessionId(), snapshotChanges);

            for (ComputeGridMonitor monitor : taskStatusMonitors) {
                try {
                    monitor.processStatusChange(snapshotChanges);
                }
                catch (Throwable t) {
                    log.error("Error processing task status diff: " + monitor, t);
                }
            }
        }
        finally {
            lock.readUnlock();
        }
    }

    /**
     * Collects statistics on jobs locally, only for those jobs that have
     * already sent a response or are being executed locally.
     *
     * @param sesId Task session ID.
     * @return Job statistics for the task. Mapping: Job status -> count of jobs.
     */
    public Map<ComputeJobStatusEnum, Long> jobStatuses(IgniteUuid sesId) {
        GridTaskWorker<?, ?> taskWorker = tasks.get(sesId);

        if (taskWorker == null)
            return emptyMap();
        else
            return taskWorker.jobStatuses();
    }

    /** */
    private void authorizeUserTask(
        @Nullable String taskName,
        @Nullable Class<?> taskCls,
        @Nullable ComputeTask<?, ?> task,
        TaskExecutionOptions opts
    ) {
        taskCls = resolveTaskClass(taskName, taskCls, task);

        if (taskCls == null || !ctx.security().isSystemType(taskCls)) {
            assert opts.isPublicRequest();

            ctx.security().authorize(taskCls == null ? taskName : taskCls.getName(), TASK_EXECUTE);
        }
    }

    /**
     * @return Class of the task which is about to execute. {@code null} means that the user is requesting a task
     * execution by its name, and  corresponding to this name task class was not found by the default classloader on
     * the local node.
     */
    private Class<?> resolveTaskClass(@Nullable String taskName, @Nullable Class<?> taskCls, @Nullable ComputeTask<?, ?> task) {
        if (taskCls != null)
            return taskCls;

        if (task != null)
            return task.getClass();

        if (taskName != null) {
            try {
                return U.forName(taskName, U.gridClassLoader());
            }
            catch (ClassNotFoundException ignored) {
                // No-op.
            }
        }

        return null;
    }
}
