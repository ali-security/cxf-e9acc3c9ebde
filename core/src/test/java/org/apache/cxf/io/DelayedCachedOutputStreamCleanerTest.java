/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cxf.io;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cxf.Bus;
import org.apache.cxf.BusFactory;
import org.apache.cxf.bus.extension.ExtensionManagerBus;

import org.junit.After;
import org.junit.Test;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class DelayedCachedOutputStreamCleanerTest {
    private Bus bus;

    @After
    public void tearDown() {
        if (bus != null) {
            bus.shutdown(true);
            bus = null;
        }
    }
    
    @Test
    public void testNoop() {
        final Map<String, Object> properties = Collections.singletonMap(CachedConstants.CLEANER_DELAY_BUS_PROP, 0);
        bus = new ExtensionManagerBus(new HashMap<>(), properties);
        
        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class)); /* noop */
        
        assertNoopCleaner(cleaner);
    }
    
    @Test
    public void testForceClean() throws InterruptedException {
        bus = new ExtensionManagerBus();
        
        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class));
        
        final AtomicBoolean latch = new AtomicBoolean(false);
        final Closeable closeable = () -> latch.compareAndSet(false, true);
        cleaner.register(closeable);
        
        final DelayedCachedOutputStreamCleaner delayedCleaner = (DelayedCachedOutputStreamCleaner) cleaner;
        delayedCleaner.forceClean();
        
        // Await for Closeable::close to be called
        assertThat(latch.get(), is(true));
    }
    
    @Test
    public void testClean() throws InterruptedException {
        final AtomicInteger latch = new AtomicInteger();
        final Closeable closeable1 = () -> latch.incrementAndGet();
        final Closeable closeable2 = () -> latch.incrementAndGet();

        /* Delay of 2.5 seconds */
        final Map<String, Object> properties = Collections.singletonMap(CachedConstants.CLEANER_DELAY_BUS_PROP, 2500);
        bus = new ExtensionManagerBus(new HashMap<>(), properties);

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        cleaner.register(closeable1);
        cleaner.register(closeable2);

        // Await for Closeable::close to be called on schedule
        await().atMost(5, TimeUnit.SECONDS).untilAtomic(latch, equalTo(2));
        assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class));
    }
    
    @Test
    public void testForceCleanForEmpty() throws InterruptedException {
        bus = new ExtensionManagerBus();

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class));
        
        final AtomicBoolean latch = new AtomicBoolean(false);
        final Closeable closeable = () -> latch.compareAndSet(false, true);

        cleaner.register(closeable);
        cleaner.unregister(closeable);
        
        final DelayedCachedOutputStreamCleaner delayedCleaner = (DelayedCachedOutputStreamCleaner) cleaner;
        delayedCleaner.forceClean();
        
        // Closeable::close should not be called
        assertThat(latch.get(), is(false));
    }
    
    @Test
    public void testForceCleanException() throws InterruptedException {
        bus = new ExtensionManagerBus();

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class));
        
        final AtomicInteger latch = new AtomicInteger();
        final Closeable closeable2 = () -> latch.incrementAndGet();
        final Closeable closeable1 = () -> {
            latch.incrementAndGet();
            throw new IOException("Simulated");
        };
        cleaner.register(closeable1);
        cleaner.register(closeable2);

        final DelayedCachedOutputStreamCleaner delayedCleaner = (DelayedCachedOutputStreamCleaner) cleaner;
        delayedCleaner.forceClean();

        // Try to call force clean one more time
        delayedCleaner.forceClean();
        
        // Await for Closeable::close to be called
        assertThat(latch.get(), equalTo(2));
    }
    
    @Test
    public void testCleanOnShutdown() throws InterruptedException {
        /* Delay of 2.5 seconds */
        final Map<String, Object> properties = Collections.singletonMap(CachedConstants.CLEANER_DELAY_BUS_PROP, 2500);
        bus = new ExtensionManagerBus(new HashMap<>(), properties);

        final AtomicBoolean latch = new AtomicBoolean();
        final Closeable closeable = () -> latch.compareAndSet(false, true);

        bus.setProperty(CachedConstants.CLEANER_DELAY_BUS_PROP, 2500); /* 2.5 seconds */
        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        cleaner.register(closeable);

        // Closes the bus, the cleaner should cancel the internal timer(s)
        bus.shutdown(true);

        // The Closeable::close should be called on shutdown
        assertThat(latch.get(), is(true));
    }

    @Test
    public void testCleanOnShutdownDisabled() throws InterruptedException {
        /* Delay of 2.5 seconds */
        final Map<String, Object> properties = new HashMap<>();
        properties.put(CachedConstants.CLEANER_DELAY_BUS_PROP, 2500);
        properties.put(CachedConstants.CLEANER_CLEAN_ON_SHUTDOWN_BUS_PROP, false);
        bus = new ExtensionManagerBus(new HashMap<>(), properties);

        final AtomicBoolean latch = new AtomicBoolean();
        final Closeable closeable = () -> latch.compareAndSet(false, true);

        bus.setProperty(CachedConstants.CLEANER_DELAY_BUS_PROP, 2500); /* 2.5 seconds */
        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        cleaner.register(closeable);

        // Closes the bus, the cleaner should cancel the internal timer(s)
        bus.shutdown(true);

        // The Closeable::close should not be called since timer(s) is cancelled
        // (awaitility 3.0.0 has no during(), so wait past the delay and assert)
        TimeUnit.SECONDS.sleep(3);
        assertThat(latch.get(), is(false));
    }

    @Test
    public void testNegativeDelay() throws InterruptedException {
        final Map<String, Object> properties = Collections.singletonMap(CachedConstants.CLEANER_DELAY_BUS_PROP, -1);
        bus = new ExtensionManagerBus(new HashMap<>(), properties);

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class)); /* noop */

        assertNoopCleaner(cleaner);
    }

    @Test
    public void testTooSmallDelay() throws InterruptedException {
        final Map<String, Object> properties = Collections.singletonMap(CachedConstants.CLEANER_DELAY_BUS_PROP, 1500);
        bus = new ExtensionManagerBus(new HashMap<>(), properties);

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class)); /* noop */

        assertNoopCleaner(cleaner);
    }
    
    @Test
    public void testDelayedClean() throws InterruptedException {
        final AtomicInteger latch = new AtomicInteger();
        final Closeable closeable1 = () -> latch.incrementAndGet();
        final Closeable closeable2 = () -> latch.incrementAndGet();

        /* Delay of 5 seconds */
        final Map<String, Object> properties = Collections.singletonMap(CachedConstants.CLEANER_DELAY_BUS_PROP, 2500);
        bus = new ExtensionManagerBus(new HashMap<>(), properties);

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        cleaner.register(closeable1);

        Thread.sleep(2000);
        cleaner.register(closeable2);

        // Await for Closeable::close to be called on schedule
        await().atMost(3, TimeUnit.SECONDS).untilAtomic(latch, equalTo(1));

        // Await for Closeable::close to be called on schedule
        await().atMost(5, TimeUnit.SECONDS).untilAtomic(latch, equalTo(2));

        assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class));
    }

    @Test
    public void testCleanDoesNotCloseBeforeDelay() throws InterruptedException {
        bus = new ExtensionManagerBus(); /* default delay of 30 minutes */

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class));

        final AtomicBoolean latch = new AtomicBoolean(false);
        final Closeable closeable = () -> latch.compareAndSet(false, true);
        cleaner.register(closeable);

        // The delay is in milliseconds, the registered stream must not expire within few milliseconds
        Thread.sleep(200);
        cleaner.clean();

        assertThat(latch.get(), is(false));
        assertThat(cleaner.size(), equalTo(1));
    }

    @Test
    public void testUnclosedCachedOutputStreamTempFileIsCleaned() throws IOException {
        final Bus oldBus = BusFactory.getThreadDefaultBus(false);
        bus = new ExtensionManagerBus();
        BusFactory.setThreadDefaultBus(bus);
        try {
            final CachedOutputStream cos = new CachedOutputStream(16);
            cos.write(new byte[1024]); /* exceeds the threshold, backed by the temporary file */

            final File tempFile = cos.getTempFile();
            assertThat(tempFile, notNullValue());
            assertThat(tempFile.exists(), is(true));

            // The stream is never closed (leaked): the cleaner has to close it and delete the temporary file
            final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
            assertThat(cleaner.size(), equalTo(1));
            ((DelayedCachedOutputStreamCleaner) cleaner).forceClean();

            assertThat(tempFile.exists(), is(false));
            assertThat(cleaner.size(), equalTo(0));
        } finally {
            BusFactory.setThreadDefaultBus(oldBus);
        }
    }

    @Test
    public void testUnclosedInputStreamTempFileIsCleaned() throws IOException {
        final Bus oldBus = BusFactory.getThreadDefaultBus(false);
        bus = new ExtensionManagerBus();
        BusFactory.setThreadDefaultBus(bus);
        try {
            final CachedOutputStream cos = new CachedOutputStream(16);
            cos.write(new byte[1024]); /* exceeds the threshold, backed by the temporary file */

            final File tempFile = cos.getTempFile();
            assertThat(tempFile, notNullValue());

            // The input stream is never closed (leaked): it holds the temporary file even after
            // the output stream is closed
            final InputStream in = cos.getInputStream();
            assertThat(in, notNullValue());
            cos.close();
            assertThat(tempFile.exists(), is(true));

            final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
            ((DelayedCachedOutputStreamCleaner) cleaner).forceClean();

            assertThat(tempFile.exists(), is(false));
            assertThat(cleaner.size(), equalTo(0));
        } finally {
            BusFactory.setThreadDefaultBus(oldBus);
        }
    }

    @Test
    public void testUnclosedCachedOutputStreamTempFileIsCleanedOnShutdown() throws IOException {
        final Bus oldBus = BusFactory.getThreadDefaultBus(false);
        bus = new ExtensionManagerBus();
        BusFactory.setThreadDefaultBus(bus);
        try {
            final CachedOutputStream cos = new CachedOutputStream(16);
            cos.write(new byte[1024]); /* exceeds the threshold, backed by the temporary file */

            final File tempFile = cos.getTempFile();
            assertThat(tempFile, notNullValue());
            assertThat(tempFile.exists(), is(true));

            // The stream is never closed (leaked): the bus shutdown has to clean it up
            bus.shutdown(true);
            bus = null;

            assertThat(tempFile.exists(), is(false));
        } finally {
            BusFactory.setThreadDefaultBus(oldBus);
        }
    }

    @Test
    public void testClosedCachedOutputStreamIsUnregistered() throws IOException {
        final Bus oldBus = BusFactory.getThreadDefaultBus(false);
        bus = new ExtensionManagerBus();
        BusFactory.setThreadDefaultBus(bus);
        try {
            final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);

            final CachedOutputStream cos1 = new CachedOutputStream(16);
            cos1.write(new byte[1024]); /* exceeds the threshold, backed by the temporary file */
            try (InputStream in = cos1.getInputStream()) {
                assertThat(in, notNullValue());
            }
            cos1.close();
            assertThat(cleaner.size(), equalTo(0));

            final CachedOutputStream cos2 = new CachedOutputStream(16);
            cos2.write(new byte[1024]); /* exceeds the threshold, backed by the temporary file */
            final File tempFile = cos2.getTempFile();
            assertThat(tempFile, notNullValue());

            // Not backed by the temporary file anymore, should not be retained by the cleaner
            cos2.resetOut(new ByteArrayOutputStream(), true);
            assertThat(tempFile.exists(), is(false));
            assertThat(cleaner.size(), equalTo(0));
            cos2.close();
        } finally {
            BusFactory.setThreadDefaultBus(oldBus);
        }
    }

    private void assertNoopCleaner(final CachedOutputStreamCleaner cleaner) {
        final AtomicBoolean latch = new AtomicBoolean(false);
        final Closeable closeable = () -> latch.compareAndSet(false, true);
        cleaner.register(closeable);
        
        final DelayedCachedOutputStreamCleaner delayedCleaner = (DelayedCachedOutputStreamCleaner) cleaner;
        delayedCleaner.forceClean();

        // Noop, Closeable::close should not be called
        assertThat(latch.get(), is(false));
    }
}
