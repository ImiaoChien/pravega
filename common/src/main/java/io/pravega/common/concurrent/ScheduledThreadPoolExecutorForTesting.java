package io.pravega.common.concurrent;

import io.pravega.common.util.ReusableLatch;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class ScheduledThreadPoolExecutorForTesting extends ScheduledThreadPoolExecutor {
    private static boolean go = true;
    private boolean shutdown = false;
    private int waiting = 0;
    private static final AtomicLong sequencer = new AtomicLong(); // not sure if we need this cuz we don't need to compare runnables (?)
    private static final Map<Runnable, Runnable> childToParent = new HashMap<>();
    private static final List<PausableTask> enqueuedTasks = new ArrayList<>();
    private static PausableTask current;
    private long seed;

    /**
     * Creates a new {@code ScheduledThreadPoolExecutorForTesting} with the
     * given core pool size.
     *
     * @param corePoolSize the number of threads to keep in the pool, even
     *                     if they are idle, unless {@code allowCoreThreadTimeOut} is set
     * @throws IllegalArgumentException if {@code corePoolSize < 0}
     */
    public ScheduledThreadPoolExecutorForTesting(int corePoolSize, long seed) {
        super(corePoolSize);
        this.seed = seed;
    }

    /**
     * Creates a new {@code ScheduledThreadPoolExecutorForTesting} with the
     * given initial parameters.
     *
     * @param corePoolSize  the number of threads to keep in the pool, even
     *                      if they are idle, unless {@code allowCoreThreadTimeOut} is set
     * @param threadFactory the factory to use when the executor
     *                      creates a new thread
     * @throws IllegalArgumentException if {@code corePoolSize < 0}
     * @throws NullPointerException     if {@code threadFactory} is null
     */
    public ScheduledThreadPoolExecutorForTesting(int corePoolSize, long seed,
                                                 ThreadFactory threadFactory) {
        super(corePoolSize, threadFactory);
        this.seed = seed;
    }

    /**
     * Creates a new {@code ScheduledThreadPoolExecutorForTesting} with the
     * given initial parameters.
     *
     * @param corePoolSize the number of threads to keep in the pool, even
     *                     if they are idle, unless {@code allowCoreThreadTimeOut} is set
     * @param handler      the handler to use when execution is blocked
     *                     because the thread bounds and queue capacities are reached
     * @throws IllegalArgumentException if {@code corePoolSize < 0}
     * @throws NullPointerException     if {@code handler} is null
     */
    public ScheduledThreadPoolExecutorForTesting(int corePoolSize, long seed,
                                                 RejectedExecutionHandler handler) {
        super(corePoolSize, handler);
        this.seed = seed;
    }

    /**
     * Creates a new {@code ScheduledThreadPoolExecutorForTesting} with the
     * given initial parameters.
     *
     * @param corePoolSize  the number of threads to keep in the pool, even
     *                      if they are idle, unless {@code allowCoreThreadTimeOut} is set
     * @param threadFactory the factory to use when the executor
     *                      creates a new thread
     * @param handler       the handler to use when execution is blocked
     *                      because the thread bounds and queue capacities are reached
     * @throws IllegalArgumentException if {@code corePoolSize < 0}
     * @throws NullPointerException     if {@code threadFactory} or
     *                                  {@code handler} is null
     */
    public ScheduledThreadPoolExecutorForTesting(int corePoolSize, long seed,
                                                 ThreadFactory threadFactory,
                                                 RejectedExecutionHandler handler) {
        super(corePoolSize, threadFactory, handler);
        this.seed = seed;
    }

    public void execute(Runnable command) {
        this.schedule(command, 1L, TimeUnit.NANOSECONDS);
    }

    public Future<?> submit(Runnable task) {
        return this.schedule(task, 1L, TimeUnit.NANOSECONDS);
    }

    public <T> Future<T> submit(Runnable task, T result) {
        return this.schedule(Executors.callable(task, result), 1L, TimeUnit.NANOSECONDS);
    }

    public <T> Future<T> submit(Callable<T> task) {
        return this.schedule(task, 1L, TimeUnit.NANOSECONDS);
    }


    protected <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task) {
        return new PausableTask<V>(task, current);
    }

    protected <V> RunnableScheduledFuture<V> decorateTask(Callable<V> callable, RunnableScheduledFuture<V> task) {
        return new PausableTask<V>(task, current);
    }

    private class PausableTask<V> implements RunnableScheduledFuture<V> {
        RunnableScheduledFuture<V> task;
        private boolean started;
        private boolean ready;
        ReusableLatch lock = new ReusableLatch();
        PausableTask current;

        PausableTask(RunnableScheduledFuture<V> task, PausableTask current) {
            this.current = current;
            this.task = task;
        }

        public void pause() throws InterruptedException {
            waiting ++;
            lock.await();
        }

        @Override
        public boolean isPeriodic() {
            return task.isPeriodic();
        }

        @Override
        public long getDelay(TimeUnit timeUnit) {
            return task.getDelay(timeUnit);
        }

        @Override
        public int compareTo(Delayed delayed) {
            return task.compareTo(delayed);
        }

        @Override
        public void run() {
            if (!lock.isReleased() && started) {
                lock.release();
            } else {
                started = true;
                task.run();
            }
        }

        @Override
        public boolean cancel(boolean b) {
            return task.cancel(b);
        }

        @Override
        public boolean isCancelled() {
            return task.isCancelled();
        }

        @Override
        public boolean isDone() {
            return task.isDone();
        }

        @Override
        public V get() throws ExecutionException, InterruptedException {
            while (!ready) {}

            if (current != null) {
                waiting ++;
                enqueuedTasks.remove(current);
                childToParent.put(this, current);

                if (enqueuedTasks.size() > 0) randomTask();
                else go = true;

                current.lock.await();
            }

            return task.get();
        }

        @Override
        public V get(long l, TimeUnit timeUnit) throws ExecutionException, InterruptedException, TimeoutException {
            while (!ready) {}

            if (current != null) {
                waiting ++;
                enqueuedTasks.remove(current);
                childToParent.put(this, current);

                if (enqueuedTasks.size() > 0) randomTask();
                else go = true;

                current.lock.await();
            }
            return task.get(l, timeUnit);
        }
    }

    /**
     * @param t the thread that will run task {@code r}
     * @param r the task that will be executed
     */
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        PausableTask task = (PausableTask) r;

        synchronized (enqueuedTasks) { enqueuedTasks.add(task); }

        if (!go) {
            try {
                waiting ++;
                task.ready = true;
                task.lock.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            current = task;
            go = false;
            task.ready = true;
        }

    }

    /**
     * @param r the runnable that has completed
     * @param t the exception that caused termination, or null if
     *          execution completed normally
     */
    protected void afterExecute(Runnable r, Throwable t) {
        while (waiting < getActiveCount() - 1) { }
        synchronized (enqueuedTasks) {
            super.afterExecute(r, t);
            enqueuedTasks.remove((PausableTask) r);
            synchronized (childToParent) {
                if (childToParent.containsKey(r)) {
                    enqueuedTasks.add((PausableTask) childToParent.get(r));
                    childToParent.remove(r);
                }
            }

            if (enqueuedTasks.isEmpty()) go = true;
            else randomTask();
        }
    }

    /**
     * @function
     *      let one thread run if there are threads waiting
     *      update current running thread (current field)
     *      shutdown if we are done with the last thread avaiable
     *
     */
    private void randomTask() {
        Random ran = new Random(seed);
        Set<PausableTask> set = new TreeSet<>();
        set.addAll(enqueuedTasks);
        enqueuedTasks.clear();
        enqueuedTasks.addAll(set);
        for (int i = 0; i < enqueuedTasks.size(); i++) {
            int n = ran.nextInt(enqueuedTasks.size());
            PausableTask temp = enqueuedTasks.get(n);
            enqueuedTasks.set(n, enqueuedTasks.get(i));
            enqueuedTasks.set(i, temp);
        }
        synchronized (current) {
            if (enqueuedTasks.size() > 0) {
                current = enqueuedTasks.get(0);
                waiting --;
                enqueuedTasks.get(0).lock.release();
            } else if (shutdown && getActiveCount() == 0) {
                super.shutdown();
            }
        }
    }

    public void shutdown() {
        try {
            awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (getQueue().size() == 0 && getActiveCount() != 0) shutdown = true;
        else super.shutdown();
    }
}
