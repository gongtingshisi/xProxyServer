package com.danikula.videocache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static com.danikula.videocache.Preconditions.checkNotNull;

/**
 * Proxy for {@link Source} with caching support ({@link Cache}).
 * <p/>
 * Can be used only for sources with persistent data (that doesn't change with time).
 * Method {@link #read(byte[], long, int)} will be blocked while fetching data from source.
 * Useful for streaming something with caching e.g. streaming video/audio etc.
 *
 * @author Alexey Danilov (danikula@gmail.com).
 * @author zhangfeng
 * @license: Apache License 2.0
 */
class ProxyCache {

    private static final Logger LOG = LoggerFactory.getLogger("ProxyCache");
    private static final int MAX_READ_SOURCE_ATTEMPTS = 1;

    private final Source source;
    private final Cache cache;
    private final Object wc = new Object();
    private final Object stopLock = new Object();
    private final AtomicInteger readSourceErrorsCount;
    private volatile Thread sourceReaderThread;
    private volatile boolean stopped;
    private volatile int percentsAvailable = -1;
    /**
     * download speed,Bytes/s.keep in mind:single task speed,total speed,instant speed,average speed
     */
    public static long speed;

    public ProxyCache(Source source, Cache cache) {
        this.source = checkNotNull(source);
        this.cache = checkNotNull(cache);
        this.readSourceErrorsCount = new AtomicInteger();
    }

    public int read(byte[] buffer, long offset, int length) throws ProxyCacheException {
        ProxyCacheUtils.assertBuffer(buffer, offset, length);

        while (!cache.isCompleted() && cache.available() < (offset + length) && !stopped && (cache.available() < source.length())) {
            readSourceAsync();
            waitForSourceData();
            checkReadSourceErrorsCount();
        }
        int read = cache.read(buffer, offset, length);
        if (cache.isCompleted() && percentsAvailable != 100) {
            percentsAvailable = 100;
            onCachePercentsAvailableChanged(100);
        }
        return read;
    }

    public int read(byte[] buffer, long offset, int length, long requestSize) throws ProxyCacheException {
        ProxyCacheUtils.assertBuffer(buffer, offset, length);

        while (!cache.isCompleted() && cache.available() < (offset + length) && !stopped && (cache.available() < requestSize)) {
            readSourceAsync(requestSize);
            waitForSourceData();
            checkReadSourceErrorsCount();
        }
        int read = cache.read(buffer, offset, length);
        if (cache.isCompleted() && percentsAvailable != 100) {
            percentsAvailable = 100;
            onCachePercentsAvailableChanged(100);
        }
        return read;
    }

    private void checkReadSourceErrorsCount() throws ProxyCacheException {
        int errorsCount = readSourceErrorsCount.get();
        if (errorsCount >= MAX_READ_SOURCE_ATTEMPTS) {
            readSourceErrorsCount.set(0);
            throw new ProxyCacheException("Error reading source " + errorsCount + " times");
        }
    }

    public void shutdown() {
        //这个地方如果早于complete的话，一直持有锁，会导致FileCache的流关闭
        synchronized (stopLock) {
            LOG.warn("Shutdown proxy for " + source);
            try {
                stopped = true;
                if (sourceReaderThread != null) {
                    sourceReaderThread.interrupt();
                }
                cache.close();
            } catch (ProxyCacheException e) {
                onError(e);
            }
        }
    }

    private synchronized void readSourceAsync() throws ProxyCacheException {
        boolean readingInProgress = sourceReaderThread != null && sourceReaderThread.getState() != Thread.State.TERMINATED;
        if (!stopped && !cache.isCompleted() && !readingInProgress) {
            sourceReaderThread = new Thread(new SourceReaderRunnable(Integer.MIN_VALUE), "Source reader for " + source);
            sourceReaderThread.start();
        }
    }

    private synchronized void readSourceAsync(long requestSize) throws ProxyCacheException {
        boolean readingInProgress = sourceReaderThread != null && sourceReaderThread.getState() != Thread.State.TERMINATED;
        if (!stopped && !cache.isCompleted() && !readingInProgress) {
            sourceReaderThread = new Thread(new SourceReaderRunnable(requestSize), "Source reader for " + source);
            sourceReaderThread.start();
        }
    }

    private void waitForSourceData() throws ProxyCacheException {
        synchronized (wc) {
            try {
                wc.wait(1000);
            } catch (InterruptedException e) {
                throw new ProxyCacheException("Waiting source data is interrupted!", e);
            }
        }
    }

    private void notifyNewCacheDataAvailable(long cacheAvailable, long sourceAvailable) {
        onCacheAvailable(cacheAvailable, sourceAvailable);

        synchronized (wc) {
            wc.notifyAll();
        }
    }

    protected void onCacheAvailable(long cacheAvailable, long sourceLength) {
        boolean zeroLengthSource = sourceLength == 0;
        int percents = zeroLengthSource ? 100 : (int) ((float) cacheAvailable / sourceLength * 100);
        boolean percentsChanged = percents != percentsAvailable;
        boolean sourceLengthKnown = sourceLength >= 0;
        if (sourceLengthKnown && percentsChanged) {
            onCachePercentsAvailableChanged(percents);
        }
//        LOG.warn("已经下载" + cacheAvailable + "/" + sourceLength);
        percentsAvailable = percents;
    }

    protected void onCachePercentsAvailableChanged(int percentsAvailable) {
    }

    private void readSource(long requestSize) {
        long sourceAvailable = Integer.MIN_VALUE;
        long offset = 0;
        try {
            offset = cache.available();
            long init = offset;
            if (requestSize == Integer.MIN_VALUE) {
                source.open(offset);
            } else {
                source.openPartial(offset, requestSize);
            }
            sourceAvailable = requestSize;
            byte[] buffer = new byte[ProxyCacheUtils.DEFAULT_BUFFER_SIZE];
            int readBytes;
            long start = System.currentTimeMillis();
            long last = offset;

            while ((readBytes = source.read(buffer)) != -1) {
                synchronized (stopLock) {
                    if (isStopped()) {
                        LOG.warn("readSource thread is stopped.");
                        return;
                    }
                    cache.append(buffer, readBytes);
                }
                offset += readBytes;
                notifyNewCacheDataAvailable(offset, sourceAvailable);
                if (System.currentTimeMillis() - start > 1 * 1000) {
                    speed = (offset - last);//instant single task speed
                    last = offset;
                    start = System.currentTimeMillis();
                }
            }
            if (tryComplete(requestSize)) {
                onSourceRead();
                LOG.warn("\n\n##### Load success,size:" + (offset - init) + ",time:" + (System.currentTimeMillis() - start) + " #####  " + source);
            } else {
                LOG.warn("\n\n##### Load fail,size:" + (offset - init) + ",time:" + (System.currentTimeMillis() - start) + " ##### " + source);
            }
        } catch (Throwable e) {
            readSourceErrorsCount.incrementAndGet();
            onError(e);
        } finally {
            closeSource();
            notifyNewCacheDataAvailable(offset, sourceAvailable);
        }
    }

    private void onSourceRead() {
        // guaranteed notify listeners after source read and cache completed
        percentsAvailable = 100;
        onCachePercentsAvailableChanged(percentsAvailable);
    }

    private boolean tryComplete(long requestSize) throws ProxyCacheException {
        //如果notifyNewCacheDataAvailable通知了另一个线程优先于此处执行，它持有这个锁，导致把FileCache流关闭，那么此处 cache.available就会抛出读取文件句柄异常
//        synchronized (stopLock) {
        long available = cache.available();
        LOG.warn("tryComplete " + isStopped() + " " + available + " " + requestSize + " " + source.length());
//            if (!isStopped() && available == (source instanceof HttpUrlSource ? requestSize : source.length())) {
        return cache.complete();
//            }
//        }
//        return false;
    }

    private boolean isStopped() {
        return Thread.currentThread().isInterrupted() || stopped;
    }

    private void closeSource() {
        try {
            source.close();
        } catch (ProxyCacheException e) {
            onError(new ProxyCacheException("Error closing source " + source, e));
        }
    }

    protected final void onError(final Throwable e) {
        boolean interruption = e instanceof InterruptedProxyCacheException;
        if (interruption) {
            LOG.debug("ProxyCache is interrupted");
        } else {
            LOG.error("ProxyCache error", e);
        }
    }

    private class SourceReaderRunnable implements Runnable {
        private long requestSize;

        public SourceReaderRunnable(long requestSize) {
            this.requestSize = requestSize;
        }

        @Override
        public void run() {
            readSource(requestSize);
        }
    }
}
