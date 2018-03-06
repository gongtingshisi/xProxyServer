package com.danikula.videocache;

import android.os.Handler;
import android.os.Looper;
import android.os.Message;

import com.danikula.videocache.file.FileCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static com.danikula.videocache.Preconditions.checkNotNull;

/**
 * Client for {@link HttpProxyCacheServer}
 *
 * @author Alexey Danilov (danikula@gmail.com).
 * @author zhangfeng
 * @license: Apache License 2.0
 */
final class HttpProxyCacheServerClients {
    private static final Logger LOG = LoggerFactory.getLogger("HttpProxyCacheServerClients");

    private final AtomicInteger clientsCount = new AtomicInteger(0);
    private final String url;
    private String title;
    private volatile HttpProxyCache proxyCache;
    private final List<CacheListener> listeners = new CopyOnWriteArrayList<>();
    private final CacheListener uiCacheListener;
    private final Config config;

    public HttpProxyCacheServerClients(String title, String url, Config config) {
        this.title = title;
        this.url = checkNotNull(url);
        this.config = checkNotNull(config);
        this.uiCacheListener = new UiListenerHandler(title, url, listeners);
    }

    public HttpProxyCacheServerClients(String title, String url, Config config, CacheListener cacheListener) {
        listeners.add(cacheListener);
        this.title = title;
        this.url = checkNotNull(url);
        this.config = checkNotNull(config);
        this.uiCacheListener = new UiListenerHandler(title, url, listeners);
    }

    public boolean processRequest(GetRequest request, Socket socket) {
        boolean process = false;
        try {
            startProcessRequest();
            clientsCount.incrementAndGet();
            process = proxyCache.processRequest(request, socket);
        } catch (ProxyCacheException e) {
            e.printStackTrace();
            HandyUtil.handle("processRequest ", e);
        } finally {
            finishProcessRequest();
        }
        return process;
    }

    private synchronized void startProcessRequest() throws ProxyCacheException {
        proxyCache = proxyCache == null ? newHttpProxyCache() : proxyCache;
    }

    public long getCurrentSpeed() throws ProxyCacheException {
        proxyCache = proxyCache == null ? newHttpProxyCache() : proxyCache;
        return proxyCache.getSpeed();
    }

    private synchronized void finishProcessRequest() {
        if (clientsCount.decrementAndGet() <= 0) {
            proxyCache.shutdown();
            proxyCache = null;
        }
    }

    public void registerCacheListener(CacheListener cacheListener) {
        listeners.add(cacheListener);
    }

    public void unregisterCacheListener(CacheListener cacheListener) {
        listeners.remove(cacheListener);
    }

    public void shutdown() {
        listeners.clear();
        if (proxyCache != null) {
            proxyCache.registerCacheListener(null);
            proxyCache.shutdown();
            proxyCache = null;
        }
        clientsCount.set(0);
    }

    public int getClientsCount() {
        return clientsCount.get();
    }

    private HttpProxyCache newHttpProxyCache() throws ProxyCacheException {
        HttpUrlSource source = new HttpUrlSource(title, url, config.sourceInfoStorage, config.headerInjector, config.mime);
        File file = config.generateCacheFile(url);
        FileCache cache = new FileCache(file, config.diskUsage);
        HttpProxyCache httpProxyCache = new HttpProxyCache(source, cache);
        httpProxyCache.registerCacheListener(uiCacheListener);
        return httpProxyCache;
    }

    private static final class UiListenerHandler extends Handler implements CacheListener {
        private String title;
        private final String url;
        private final List<CacheListener> listeners;
        private boolean allFromCache;

        public UiListenerHandler(String title, String url, List<CacheListener> listeners) {
            super(Looper.getMainLooper());
            this.title = title;
            this.url = url;
            this.listeners = listeners;
        }

        @Override
        public void onCacheAvailable(String title, File file, String url, int percentsAvailable, boolean allFromCache) {
            this.allFromCache = allFromCache;
            Message message = obtainMessage();
            message.arg1 = percentsAvailable;
            message.obj = file;
            sendMessage(message);
        }

        @Override
        public void handleMessage(Message msg) {
            File file = (File) msg.obj;
            for (CacheListener cacheListener : listeners) {
                if (file.exists() && file.length() > 0)
                    cacheListener.onCacheAvailable(title, file, url, (int) msg.arg1, allFromCache);
            }
        }
    }
}
