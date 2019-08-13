package io.pravega.common.concurrent;

import java.util.*;
import java.util.concurrent.*;

public class ScheduledThreadPoolExecutorForTesting extends ScheduledThreadPoolExecutor {
    private static List<Runnable> tasks;
    private List<Runnable> halts = new ArrayList<>();
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
        tasks = new LinkedList<>();
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
        tasks = new LinkedList<>();
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
        tasks = new LinkedList<>();
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
        tasks = new LinkedList<>();
        this.seed = seed;
    }

    /**
     * @param t the thread that will run task {@code r}
     * @param r the task that will be executed
     */
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
    }

    /**
     * @param r the runnable that has completed
     * @param t the exception that caused termination, or null if
     *          execution completed normally
     */
    protected void afterExecute(Runnable r, Throwable t) {
        synchronized (this) {
            super.afterExecute(r, t);
            // if that unlocks a halting thread
            // unhalt the halting thread and add no task to the queue
            randomTask();
        }
    }

    public ScheduledFuture<?> schedule(Runnable var1, long var2, TimeUnit var4) {
        if (getActiveCount() - halts.size() == 1) {
            // halt the current running thread
        }
        ScheduledFuture<?> current = super.schedule(var1, var2, var4);

        randomTask();
        return current;
    }

    public <V> ScheduledFuture<V> schedule(Callable<V> var1, long var2, TimeUnit var4) {
        ScheduledFuture<V> current = super.schedule(var1, var2, var4);
        randomTask();
        return current;
    }

    public ScheduledFuture<?> scheduleAtFixedRate(Runnable var1, long var2, long var4, TimeUnit var6) {
        ScheduledFuture<?> current = super.scheduleAtFixedRate(var1, var2, var4, var6);
        randomTask();
        return current;
    }

    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable var1, long var2, long var4, TimeUnit var6) {
        ScheduledFuture<?> current = super.scheduleAtFixedRate(var1, var2, var4, var6);
        randomTask();
        return current;
    }


    private void randomTask() {
        BlockingQueue<Runnable> queue = getQueue();
        queue.drainTo(tasks);
        Random ran = new Random(seed);
        for (int i = 0; i < tasks.size(); i++) {
            int n = ran.nextInt(tasks.size());
            Runnable temp = tasks.get(n);
            tasks.set(n, tasks.get(i));
            tasks.set(i, temp);
        }
        if (tasks.size() > 0) queue.add(tasks.remove(0));
    }
}
