/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 */
package io.pravega.common.concurrent;

import io.pravega.test.common.*;

import java.io.*;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.*;

import org.junit.*;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for the ScheduledThreadPoolExecutorForTestingTests class.
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
            ((Map<String, String>) field.get(env)).put("DETERMINISTIC_TEST", "10");
            ses = ExecutorServiceHelpers.newScheduledThreadPool(10, "pool");
        } catch (Exception e) {
            System.out.println("Error");
        }
    }

    /**
     * Tests when calling one task method.
     */
    @Test
    public void testBasicOneTask() {
        final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        final PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outContent));

        Runnable task2 = () -> System.out.print("task2 ");
        System.out.print("task1 ");

        ses.schedule(task2, 1, TimeUnit.NANOSECONDS);

        ses.shutdown();
        assertEquals(outContent.toString(), "task1 task2 ");
        System.setOut(originalOut);
    }

    /**
     * Tests when calling multiple tasks.
     */
    @Test
    public void testBasicMultipleTask() {
        final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        final PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outContent));

        Runnable task1 = () -> System.out.print("task1 ");
        Runnable task2 = () -> System.out.print("task2 ");
        Runnable task3 = () -> System.out.print("task3 ");

        ses.schedule(task1, 1, TimeUnit.NANOSECONDS);
        ses.schedule(task2, 1, TimeUnit.NANOSECONDS);
        ses.schedule(task3, 1, TimeUnit.NANOSECONDS);

        ses.shutdown();
        assertEquals("task1 task2 task3 ", outContent.toString());
        String rs = outContent.toString();
        System.setOut(originalOut);
    }

    /**
     * Tests the execute() method.
     */
    @Test
    public void testTaskCallTask() throws IllegalMonitorStateException {

        final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        final PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outContent));

        Runnable task1 = ()  -> {
            System.out.print("task1 ");
            Runnable task2 = () -> System.out.print("task2 ");
            ses.execute(task2);
            Runnable task3 = () -> System.out.print("task3 ");
            ses.execute(task3);
            Runnable task4 = () -> System.out.print("task4 ");
            ses.execute(task4);
        };

        ses.schedule(task1, 1, TimeUnit.NANOSECONDS);
        ses.shutdown();
        assertEquals("task1 task4 task2 task3 ", outContent.toString());

        System.setOut(originalOut);
    }


    /**
     * Tests the execute() method.
     */
    @Test
    public void testTaskCallTaskSchedule() throws IllegalMonitorStateException {

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
        ses.shutdown();
        assertEquals("task1 task4 task2 task3 ", outContent.toString());

        System.setOut(originalOut);
    }



    /**
     * Tests when calling one task method.
     */
    @Test
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
                Runnable task2_4 = () -> System.out.print("task2_4 ");
                ses.schedule(task2_4, 1, TimeUnit.NANOSECONDS);
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
        ses.shutdown();

        assertEquals("task1 task4 task4_1 task4_2 task4_3 task2 task2_2 task2_3 task2_4 " +
                "task3 task3_2 task3_3 task2_1 task3_1 ", outContent.toString());
        System.setOut(originalOut);
    }

    /**
     * Tests when calling one task method.
     */
    @Test
    public void testDependantTasks() {
        try {

            final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
            final PrintStream originalOut = System.out;
            System.setOut(new PrintStream(outContent));

            Runnable task1 = () -> {
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
                Future<String> task3 = ses.submit(() -> {
                    System.out.print("task3_1 ");
                    return "task3 ";
                });
                Runnable task4 = () -> {
                    System.out.print("task4 ");
                    Runnable task4_1 = () -> System.out.print("task4_1 ");
                    ses.schedule(task4_1, 1, TimeUnit.NANOSECONDS);
                    Runnable task4_2 = () -> System.out.print("task4_2 ");
                    ses.schedule(task4_2, 1, TimeUnit.NANOSECONDS);
                    Runnable task4_3 = () -> System.out.print("task4_3 ");
                    ses.schedule(task4_3, 1, TimeUnit.NANOSECONDS);
                };

                try {
                    ses.submit(task2);
                    System.out.print(task3.get());
                    ses.submit(task4);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            };

            ses.schedule(task1, 1, TimeUnit.NANOSECONDS);
            ses.shutdown();

            assertEquals("task1 task3_1 task3 task2 task2_2 task2_3 task4 task4_2 task4_3 task2_1 task4_1 ",
                    outContent.toString());
            System.setOut(originalOut);
        } catch (Exception e) {
            System.out.println("EXCEPTION");
        }
    }

    /**
     * Tests when calling one task method.
     */
    @Test
    public void testDependantTasksBasic() {
        try {

            final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
            final PrintStream originalOut = System.out;
            System.setOut(new PrintStream(outContent));

            Runnable task1 = () -> {
                System.out.print("task1 ");
                Future<String> task3 = ses.submit(() -> {
                    System.out.print("task3_1 ");
                    return "task3 ";
                });

                try {
                    String rs = task3.get();
                    System.out.print(rs);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            };

            ses.schedule(task1, 1, TimeUnit.NANOSECONDS);
            ses.shutdown();
            assertEquals("task1 task3_1 task3 ", outContent.toString());
            System.setOut(originalOut);
        } catch (Exception e) {
            System.out.println("EXCEPTION");
        }
    }

    @Test
    public void delayShutdown() {
        final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        final PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outContent));

        Runnable task2 = () -> System.out.print("task2 ");
        System.out.print("task1 ");
        ses.schedule(task2, 1, TimeUnit.NANOSECONDS);

        ses.shutdown();
        assertEquals(outContent.toString(), "task1 task2 ");
        System.setOut(originalOut);
    }
}
