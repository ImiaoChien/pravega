/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 */
package io.pravega.common.concurrent;

import io.pravega.common.util.ReusableLatch;
import io.pravega.test.common.*;

import java.io.*;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.*;

import org.junit.*;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for the ExecutorServiceHelpers class.
 * NOTE: this class inherits from ThreadPooledTestSuite but does not set a custom ThreadPoolSize. The Default (0) indicates
 * we are using an InlineThreadPool, which is what all these tests rely on.
 */
public class ScheduledThreadPoolExecutorForTestingTests  extends ThreadPooledTestSuite {
    ScheduledExecutorService ses;
    public TemporaryFolder folder;

    @Before
    @Test
    public void setUp() {
        try {
            folder = new TemporaryFolder();
            Map<String, String> env = System.getenv();
            Field field = env.getClass().getDeclaredField("m");
            field.setAccessible(true);
            ((Map<String, String>) field.get(env)).put("TEST", "ORDER");
            ses = ExecutorServiceHelpers.newScheduledThreadPool(1, "pool");
        } catch (Exception e) {
            System.out.println("Error");
        }
    }

    /**
     * Tests when calling one task method.
     */
    @Test(timeout = 5000)
    public void testBasicOneTask() throws InterruptedException {
        final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        final PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outContent));

        Runnable task2 = () -> System.out.print("task2 ");
        System.out.print("task1 ");
        ses.schedule(task2, 1, TimeUnit.MICROSECONDS);
        waitWithReusableLatch();
        ses.shutdown();
        assertEquals(outContent.toString(), "task1 task2 ");
        System.setOut(originalOut);
    }

    /**
     * Tests when calling multiple tasks.
     */
    @Test
    public void testBasicMultipleTask() throws InterruptedException {
        final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        final PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outContent));

        Runnable task1 = () -> System.out.print("task1 ");
        Runnable task2 = () -> System.out.print("task2 ");
        Runnable task3 = () -> System.out.print("task3 ");
        ses.schedule(task1, 1, TimeUnit.MICROSECONDS);
        ses.schedule(task2, 1, TimeUnit.MICROSECONDS);
        ses.schedule(task3, 1, TimeUnit.MICROSECONDS);

        waitWithReusableLatch();
        ses.shutdown();
        assertEquals("task1 task2 task3 ", outContent.toString());
        String rs = outContent.toString();
        System.setOut(originalOut);
    }

    /**
     * Tests the execute() method.
     */
    @Test(timeout = 50000)
    public void testTaskCallTask() throws IllegalMonitorStateException {
        final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        final PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outContent));

        Runnable task1 = ()  -> {
            System.out.print("task1 ");
            Runnable task2 = () -> System.out.print("task2 ");
            ses.schedule(task2, 1, TimeUnit.NANOSECONDS);
            Runnable task3 = () -> System.out.print("task3 ");
            ses.schedule(task3, 1, TimeUnit.NANOSECONDS);
            Runnable task4 = () -> System.out.print("task4 ");
            ses.schedule(task4, 1, TimeUnit.NANOSECONDS);
        };

        ses.schedule(task1, 1, TimeUnit.NANOSECONDS);
        waitWithReusableLatch();
        assertEquals("task1 task3 task2 task4 ", outContent.toString());
        ses.shutdown();
        System.setOut(originalOut);
    }

    /**
     * Tests when calling one task method.
     */
    @Test(timeout = 5000)
    public void testCallTestsNested() {
        final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        final PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outContent));

        Runnable task1 = ()  -> {
            System.out.print("task1 ");
            Runnable task2 = () -> {
                System.out.print("task2 ");
                Runnable task2_1 = () -> System.out.print("task2_1 ");
                ses.schedule(task2_1, 1, TimeUnit.NANOSECONDS);
                Runnable task2_2 = () -> System.out.print("task2_2 ");
                ses.schedule(task2_2, 1, TimeUnit.NANOSECONDS);
                Runnable task2_3 = () -> System.out.print("task2_3 ");
                ses.schedule(task2_3, 1, TimeUnit.NANOSECONDS);
            };
            ses.schedule(task2, 1, TimeUnit.NANOSECONDS);
            Runnable task3 = () -> {
                System.out.print("task3 ");
                Runnable task3_1 = () -> System.out.print("task3_1 ");
                ses.schedule(task3_1, 1, TimeUnit.NANOSECONDS);
                Runnable task3_2 = () -> System.out.print("task3_2 ");
                ses.schedule(task3_2, 1, TimeUnit.NANOSECONDS);
                Runnable task3_3 = () -> System.out.print("task3_3 ");
                ses.schedule(task3_3, 1, TimeUnit.NANOSECONDS);
            };
            ses.schedule(task3, 1, TimeUnit.NANOSECONDS);
            Runnable task4 = () -> {
                System.out.print("task4 ");
                Runnable task4_1 = () -> System.out.print("task4_1 ");
                ses.schedule(task4_1, 1, TimeUnit.NANOSECONDS);
                Runnable task4_2 = () -> System.out.print("task4_2 ");
                ses.schedule(task4_2, 1, TimeUnit.NANOSECONDS);
                Runnable task4_3 = () -> System.out.print("task4_3 ");
                ses.schedule(task4_3, 1, TimeUnit.NANOSECONDS);
            };
            ses.schedule(task4, 1, TimeUnit.NANOSECONDS);
        };

        ses.schedule(task1, 1, TimeUnit.NANOSECONDS);
        waitWithReusableLatch();

        assertEquals("task1 task3 task3_1 task4 task4_1 task2 task4_2 task4_3 " +
                "task2_2 task2_1 task2_3 task3_2 task3_3 ", outContent.toString());
        System.setOut(originalOut);
    }

    /**
     * Tests when calling one task method.
     */
    // TODO
    @Test(timeout = 5000)
    public void testThrowExceptions() {
        try {
            final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
            final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
            final PrintStream originalOut = System.out;
            final PrintStream originalErr = System.err;
            System.setOut(new PrintStream(outContent));
            System.setErr(new PrintStream(errContent));



            assertEquals("task1 task3 task2 ", outContent.toString());
            System.setOut(originalOut);
            System.setErr(originalErr);
        } catch (Exception e) {
            System.out.println("EXCEPTION");
        }
    }

    /**
     * Tests when calling one task method.
     */
    // TODO
    @Test(timeout = 5000)
    public void testTimeOutTasks() {
        try {
            final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
            final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
            final PrintStream originalOut = System.out;
            final PrintStream originalErr = System.err;
            System.setOut(new PrintStream(outContent));
            System.setErr(new PrintStream(errContent));

            assertEquals("task1 task3 task2 ", outContent.toString());
            System.setOut(originalOut);
            System.setErr(originalErr);
        } catch (Exception e) {
            System.out.println("EXCEPTION");
        }
    }

    /**
     * Tests when calling one task method.
     */
    // TODO
    @Test //(timeout = 5000)
    public void testDependantTasks() {
        try {
            final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
            final PrintStream originalOut = System.out;
            System.setOut(new PrintStream(outContent));

            Runnable task1 = () -> {
                System.out.println("task1 ");
                Future<String> task2 = ses.submit(() -> {
                    System.out.println("task2_1 ");
                    return "task3 ";
                });
                Future<String> task3 = ses.submit(() -> {
                    System.out.println("task3_1 ");
                    return "task3 ";
                });

                try {
                    System.out.println(task3.get());
                    System.out.println(task2.get());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            };

            ses.schedule(task1, 1, TimeUnit.NANOSECONDS);
            waitWithReusableLatch();

            assertEquals("", outContent.toString());
            System.setOut(originalOut);
        } catch (Exception e) {
            System.out.println("EXCEPTION");
        }
    }

    public void waitWithReusableLatch() {
        ReusableLatch lock = new ReusableLatch();
        try {
            lock.await(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
}
