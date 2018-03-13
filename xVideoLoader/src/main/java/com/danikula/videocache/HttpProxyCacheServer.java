package com.danikula.videocache;

import android.content.Context;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.net.Uri;
import android.net.wifi.WifiInfo;
import android.net.wifi.WifiManager;
import android.util.Log;
import android.webkit.URLUtil;

import com.danikula.videocache.file.DiskUsage;
import com.danikula.videocache.file.FileNameGenerator;
import com.danikula.videocache.file.Md5FileNameGenerator;
import com.danikula.videocache.file.TotalCountLruDiskUsage;
import com.danikula.videocache.file.TotalSizeLruDiskUsage;
import com.danikula.videocache.headers.EmptyHeadersInjector;
import com.danikula.videocache.headers.HeaderInjector;
import com.danikula.videocache.sourcestorage.SourceInfoStorage;
import com.danikula.videocache.sourcestorage.SourceInfoStorageFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.URL;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

import static com.danikula.videocache.Preconditions.checkAllNotNull;
import static com.danikula.videocache.Preconditions.checkNotNull;
import static com.danikula.videocache.file.FileCache.TEMP_POSTFIX;
import static java.net.HttpURLConnection.HTTP_MOVED_PERM;
import static java.net.HttpURLConnection.HTTP_MOVED_TEMP;
import static java.net.HttpURLConnection.HTTP_SEE_OTHER;

/**
 * Server to mainly preload partial file supported HTTP requests(typically such as mp4 stream),which accelerates the load speed when use.
 * And more,we will readjust the preload size in parallel according to current network and running tasks to avoid block current playing video.
 *
 * @author Alexey Danilov (danikula@gmail.com).
 * @author zhangfeng
 * @license: Apache License 2.0
 */
public class HttpProxyCacheServer {
    private static final Logger LOG = LoggerFactory.getLogger("HttpProxyCacheServer");
    private static final String PROXY_HOST = "127.0.0.1";
    private final Object clientsLock = new Object();
    private final ExecutorService socketProcessor = Executors.newCachedThreadPool();
    private final Map<String, HttpProxyCacheServerClients> clientsMap = new ConcurrentHashMap<>();
    private final Map<String, Socket> socketMap = new ConcurrentHashMap<>();
    private final Object socketLock = new Object();
    private final ServerSocket serverSocket;
    private final int port;
    private final Thread waitConnectionThread;
    private final Config config;
    private final Pinger pinger;
    private final ExecutorService preloadProcessor = Executors.newCachedThreadPool();
    private final Object redirectLock = new Object();
    private ArrayBlockingQueue<PreloadInfo> blockingQueue = new ArrayBlockingQueue<>(50);
    private final Object preloadLock = new Object();
    private boolean interrupt = false;
    public static final long NONE = -1;
    private boolean DEBUG = Log.isLoggable(getClass().getSimpleName(), Log.DEBUG);
    public static Context context;
    public static final String CONTENT_TYPE_VIDEO_MP4 = "video/mp4";
    private CountDownLatch work = new CountDownLatch(1);
    private int pending = 0;
    private Object pendingLock = new Object();
    public static final int BITRATE = 300;
    private static final int BYTES_RATE = (BITRATE << 10) / 8;
    private long speed;
    private SpeedMonitor speedMonitor = new SpeedMonitor();
    private PreloadConductor conductPreloadRunnable = new PreloadConductor();

    private void increasePendingCount() {
        synchronized (pendingLock) {
            pending++;
        }
    }

    private int getPendingCount() {
        synchronized (pendingLock) {
            return pending;
        }
    }


    private void decreasePendingCount() {
        synchronized (pendingLock) {
            if (pending > 0)
                pending--;
        }
    }

    class PreloadConductor implements Runnable {
        @Override
        public void run() {
            while (!interrupt) {
                synchronized (preloadLock) {
                    try {
                        preloadLock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                int put = 0;
                int allow = getMaxAllowedAccept();
                LOG.warn("Max accepted:" + allow + " ，queue :" + blockingQueue.size() + " ,pending:" + getPendingCount() + " ,running:" + getClientsCount());
                do {
                    if (allow > 0 && !blockingQueue.isEmpty()) {
                        PreloadInfo info = null;
                        try {
                            info = blockingQueue.take();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        if (info != null) {
                            LOG.warn("### Start preload " + info);
                            try {
                                preloadProcessor.submit(new PreloadRunnable(info.title, info.url, 0, getOfferSize(info), info.cacheListener));
                                put++;
                                increasePendingCount();
                            } catch (RejectedExecutionException e) {
                                LOG.error("Reject task !");
                            }
                        } else {
                            LOG.warn("Warning: Null object in queue...");
                        }
                        if (work.getCount() > 0) {
                            LOG.warn("Waiting server start working...");
                            try {
                                work.await();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    } else {
                        break;
                    }
                } while (put < allow);
            }
        }
    }

    private int getRunningCount() {
        return getClientsCount() + getPendingCount();
    }

    /**
     * whether we should give an expire timeout?
     */
    private final Map<String, String> redirectMap = new ConcurrentHashMap<>();

    public HttpProxyCacheServer(Context context) {
        this(new Builder(context).buildConfig());
        this.context = context;
    }

    private HttpProxyCacheServer(Config config) {
        this.config = checkNotNull(config);
        try {
            InetAddress inetAddress = InetAddress.getByName(PROXY_HOST);
            this.serverSocket = new ServerSocket(0, 0, inetAddress);
            this.port = serverSocket.getLocalPort();
            IgnoreHostProxySelector.install(PROXY_HOST, port);
            CountDownLatch startSignal = new CountDownLatch(1);
            this.waitConnectionThread = new Thread(new WaitRequestsRunnable(startSignal, work));
            this.waitConnectionThread.start();
            startSignal.await(); // freeze thread, wait for server starts
            this.pinger = new Pinger(PROXY_HOST, port);
            LOG.debug("Proxy cache server started. Is it alive? " + isAlive());
            preloadProcessor.submit(conductPreloadRunnable);
            preloadProcessor.submit(speedMonitor);
        } catch (IOException | InterruptedException e) {
            socketProcessor.shutdown();
            throw new IllegalStateException("Error starting local proxy server", e);
        }
    }

    /**
     * Returns url that wrap original url and should be used for client (MediaPlayer, ExoPlayer, etc).
     * <p>
     * If file for this url is fully cached (it means method {@link #isCacheCompleted(String)} returns {@code true})
     * then file:// uri to cached file will be returned.
     * <p>
     * Calling this method has same effect as calling {@link #getProxyUrl(String, boolean)} with 2nd parameter set to {@code true}.
     *
     * @param url a url to file that should be cached.
     * @return a wrapped by proxy url if file is not fully cached or url pointed to cache file otherwise.
     */
    private String getProxyUrl(String url) {
        return getProxyUrl(url, true);
    }

    class PreloadInfo {
        private String title;
        private String url;
        private long duration;
        private int offer;
        private CacheListener cacheListener;

        public PreloadInfo(String title, String url, long duration, int offer, CacheListener cacheListener) {
            this.title = title;
            this.url = url;
            this.duration = duration;
            this.offer = offer;
            this.cacheListener = cacheListener;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PreloadInfo info = (PreloadInfo) o;
            return url.equals(info.url);
        }

        @Override
        public String toString() {
            return "PreloadInfo{" +
                    "title='" + title + '\'' +
                    (DEBUG ? ", url='" + url + '\'' : "") +
                    ", duration=" + duration +
                    ", offer=" + offer +
                    '}';
        }
    }

    enum SPEED {
        NONE,
        SLOW,
        NORMAL,
        HIGH
    }

    class SpeedMonitor implements Runnable {
        @Override
        public void run() {
            while (!interrupt) {
                synchronized (clientsLock) {
                    if (clientsMap.size() > 0)
                        speed = 0;
                    for (HttpProxyCacheServerClients clients : clientsMap.values()) {
                        try {
                            speed += clients.getCurrentSpeed();
                        } catch (ProxyCacheException e) {
                            continue;
                        }
                    }
                }
                try {
                    Thread.sleep(1 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void dumpNetwork() {
        if (DEBUG)
            LOG.warn("Current connection:" + getNetworkTypeName(context));
    }

    public String getNetworkTypeName(Context context) {
        ConnectivityManager connectMgr = (ConnectivityManager) context.getSystemService(Context.CONNECTIVITY_SERVICE);
        if (connectMgr != null) {
            NetworkInfo info = connectMgr.getActiveNetworkInfo();

            if (info != null && info.isConnected()) {
                if (info.getType() == ConnectivityManager.TYPE_WIFI) {
                    WifiManager wifiManager = (WifiManager) context.getApplicationContext().getSystemService(Context.WIFI_SERVICE);
                    if (wifiManager != null) {
                        WifiInfo wifiInfo = wifiManager.getConnectionInfo();
                        if (wifiInfo != null)
                            return "Wifi:" + wifiInfo.getSSID();
                    }
                }
                return info.getSubtypeName();
            }
        }
        return "";
    }

    public static final int SPEED_NONE_MAX_NUM = 1;
    public static final int SPEED_SLOW_MAX_NUM = 2;
    public static final int SPEED_NORMAL_MAX_NUM = 3;
    public static final int SPEED_HIGH_MAX_NUM = 4;

    private SPEED getSpeed() {
        LOG.warn("Instant  speed:" + speed);
        if (speed < BYTES_RATE * SPEED_SLOW_MAX_NUM)
            return SPEED.NONE;
        else if (speed < BYTES_RATE * SPEED_NORMAL_MAX_NUM)
            return SPEED.SLOW;
        else if (speed < BYTES_RATE * SPEED_HIGH_MAX_NUM)
            return SPEED.NORMAL;
        else
            return SPEED.HIGH;
    }

    private int getMaxAllowedAccept() {
        int allow = 0;
        switch (getSpeed()) {
            case NONE:
                allow = SPEED_NONE_MAX_NUM - getRunningCount();
                break;
            case SLOW:
                allow = SPEED_SLOW_MAX_NUM - getRunningCount();
                break;
            case NORMAL:
                allow = SPEED_NORMAL_MAX_NUM - getRunningCount();
                break;
            case HIGH:
                allow = SPEED_HIGH_MAX_NUM - getRunningCount();
                break;
        }
        return (allow < 0) ? 0 : allow;
    }

    /**
     * preload partial data in advance
     *
     * @param title         :unique title
     * @param url           :the url to load
     * @param duration      :video duration,unit:second
     * @param second        :time to preload,unit:second,recommend 5 second.
     * @param cacheListener :the notifier on completion
     */
    public void preload(String title, String url, long duration, int second, CacheListener cacheListener) {
        if (url == null || duration <= 0 || second <= 0 || cacheListener == null)
            throw new IllegalArgumentException("Hi guy,your argument has problem, plz check it !");
        synchronized (redirectLock) {
            if (redirectMap.containsKey(url)) {
                String redirected = redirectMap.get(url);
                synchronized (clientsLock) {
                    if (clientsMap.containsKey(redirected)) {
                        LOG.warn(url + " is running,ignore.....");
                        return;
                    }
                }
            }
        }

        PreloadInfo info = new PreloadInfo(title, url, duration, second, cacheListener);
//        if (isAlive()) {
        if (!blockingQueue.contains(info) && !isCacheCompleted(url) && !isCacheUncompleted(url)) {
            LOG.warn("New a preload task:" + title + " ，queue :" + (blockingQueue.size() + 1)
                    + " ,pending:" + getPendingCount() + " ,running:" + getClientsCount());

            boolean added = blockingQueue.offer(info);
            if (!added) {
                LOG.warn("Fail to add : " + title);
            }
            if (added) {
                synchronized (preloadLock) {
                    preloadLock.notifyAll();
                }
            }
        }
//        }
    }

    /**
     * cancel a preload task in queue if it hasn't been running.
     *
     * @param title :unique title
     * @param url   :the url to cancel
     */
    private void cancelQueueing(String title, String url) {
        if (url == null)
            throw new IllegalArgumentException("Plz give a legal input!");
        for (Iterator<PreloadInfo> iterable = blockingQueue.iterator(); iterable.hasNext(); ) {
            PreloadInfo info = iterable.next();
            if (info.url.equals(url)) {
                iterable.remove();
                LOG.warn("Cancel preload queueing:" + title + " , queue:" + blockingQueue.size());
            }
        }
    }

    /**
     * cancel a preload task in pending if it hasn't been running.
     */
    private void cancelPending(String title, String url) {
        if (url == null)
            throw new IllegalArgumentException("Plz give a legal input!");
        //...
        if (false) {
            decreasePendingCount();
        }
    }

    /**
     * cancel a preload task ,include these ones are in running state
     */
    public void cancel(String title, String url) {
        if (url == null)
            throw new IllegalArgumentException("Plz give a legal input!");

        cancelQueueing(title, url);
        cancelPending(title, url);
        cleanRunningClient(null, title, url, true);
    }

    private void cleanRunningClient(GetRequest request, String title, String url, boolean artificial) {
        HttpProxyCacheServerClients clients;
        Socket socket;

        if ((request != null && request.keyUA) || artificial) {
            synchronized (clientsLock) {
                clients = clientsMap.get(url);
                clientsMap.remove(url);

                if (clients != null) {
                    synchronized (socketLock) {
                        socket = socketMap.get(url);
                        if (socket != null)
                            releaseSocket(socket);
                        socketMap.remove(url);
                    }
                    clients.shutdown();
                    if (artificial) {
                        LOG.warn("Cancel preload running :" + title + " ，queue :" + blockingQueue.size()
                                + " ,pending:" + getPendingCount() + " ,running:" + getClientsCount());
                    }
                }
            }
        }
    }

    /**
     * CARE:cancel all tasks including playing state !
     */
    public void cancelAll() {
        LOG.warn("Cancel all ...");
        blockingQueue.clear();
        synchronized (socketLock) {
            for (Iterator iterator = socketMap.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry entry = (Map.Entry) iterator.next();
                releaseSocket((Socket) entry.getValue());
                socketMap.remove(entry.getKey());
            }
        }
        shutdownClients();

        synchronized (pendingLock) {
            pending = 0;
        }
    }

    private long getOfferSize(PreloadInfo info) {
        return (512 * info.duration + BYTES_RATE * info.offer);
    }

    /**
     * mock a whole request instead partial range request,we reserve it for debug
     */
    private void request(String url) {
        if (url == null)
            return;
        preloadProcessor.submit(new MockSocket(url));
    }

    class MockSocket implements Runnable {
        private String url;

        public MockSocket(String url) {
            this.url = url;
        }

        @Override
        public void run() {
            try {
                LOG.warn("**************MOCK****************" + url);
                URL url = new URL(this.url);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.connect();
                int code = connection.getResponseCode();

                LOG.warn("respond " + code);
                if (code == HttpURLConnection.HTTP_OK || code == HttpURLConnection.HTTP_PARTIAL) {
                    InputStream inputStream = connection.getInputStream();

                    File cacheDir = config.cacheRoot;
                    URL uRL = new URL(this.url);

                    String actual = ProxyCacheUtils.decode(uRL.getPath().substring(1));
                    LOG.warn("decode:" + actual);

                    String fileName = config.fileNameGenerator.generate(actual);

                    File file = new File(cacheDir, fileName + "_mock.mp4");
                    FileOutputStream fos = new FileOutputStream(file);

                    byte[] buff = new byte[8 * 1024];
                    int read = 0;
                    long size = 0;
                    while ((read = inputStream.read(buff, 0, buff.length)) > 0) {
                        fos.write(buff, 0, read);
                        size += read;
                    }
                    fos.flush();
                    fos.close();
                    inputStream.close();
                    LOG.warn("write mock: " + file.getAbsolutePath() + " success，" + size);
                }
            } catch (MalformedURLException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    class PreloadRunnable implements Runnable {
        String title;
        String url;
        long offset;
        long size;
        HttpUrlSource urlSource;
        CacheListener cacheListener;

        public PreloadRunnable(String title, String url, long offset, long size, CacheListener cacheListener) {
            this.title = title;
            this.url = url;
            this.offset = offset;
            this.size = size;
            this.cacheListener = cacheListener;
        }

        @Override
        public void run() {
            try {
                long offset = 0;
                url = redirect(url);
                if (url == null) {
                    decreasePendingCount();
                    return;
                }
                LOG.warn("# Preload key:" + config.fileNameGenerator.generate(url) + " : " + title);
                String proxy = getProxyUrl(url);
                if (URLUtil.isFileUrl(proxy)) {
                    LOG.warn("Local complete cache found,preload success # " + title);
                    decreasePendingCount();
                    return;
                }

                if (isPreloadedOrCached(url)) {
                    long size = getPreloadedOrCachedSize(url);
                    if (this.size > size) {
                        offset = size;
                        LOG.warn("Continue to preload from " + offset + " : " + title);
                    } else {
                        LOG.warn("Local partial cache found,preload success # " + title);
                        decreasePendingCount();
                        return;
                    }
                }

                synchronized (clientsLock) {
                    HttpProxyCacheServerClients clients = clientsMap.get(url);
                    if (clients == null) {
                        clients = new HttpProxyCacheServerClients(title, url, config, cacheListener);
                        clientsMap.put(url, clients);
                    }
                }

                urlSource = new HttpUrlSource(context, title, proxy, config.mime);
                urlSource.openPartial(offset, size);
            } catch (Exception e) {
                HandyUtil.handle("preload " + this.urlSource, e);
                decreasePendingCount();
            }
        }
    }

    /**
     * Returns url that wrap original url and should be used for client (MediaPlayer, ExoPlayer, etc).
     * <p>
     * If parameter {@code allowCachedFileUri} is {@code true} and file for this url is fully cached
     * (it means method {@link #isCacheCompleted(String)} returns {@code true}) then file:// uri to cached file will be returned.
     *
     * @param url                a url to file that should be cached.
     * @param allowCachedFileUri {@code true} if allow to return file:// uri if url is fully cached
     * @return a wrapped by proxy url if file is not fully cached or url pointed to cache file otherwise (if {@code allowCachedFileUri} is {@code true}).
     */
    private String getProxyUrl(String url, boolean allowCachedFileUri) {
        if (allowCachedFileUri && isCacheCompleted(url)) {
            File cacheFile = getCacheCompletedFile(url);
            touchFileSafely(cacheFile);
            return Uri.fromFile(cacheFile).toString();
        }
        return appendToProxyUrl(url);
    }

    public String getPureProxyUrl(String url) {
        if (url == null)
            return null;
        return isAlive() ? appendToProxyUrl(url) : url;
    }

    public void registerCacheListener(CacheListener cacheListener, String url) {
        checkAllNotNull(cacheListener, url);
        synchronized (clientsLock) {
            try {
                getClients(url).registerCacheListener(cacheListener);
            } catch (ProxyCacheException e) {
                LOG.warn("Error registering cache listener", e);
            }
        }
    }

    public void unregisterCacheListener(CacheListener cacheListener, String url) {
        checkAllNotNull(cacheListener, url);
        synchronized (clientsLock) {
            try {
                getClients(url).unregisterCacheListener(cacheListener);
            } catch (ProxyCacheException e) {
                LOG.warn("Error registering cache listener", e);
            }
        }
    }

    public void unregisterCacheListener(CacheListener cacheListener) {
        checkNotNull(cacheListener);
        synchronized (clientsLock) {
            for (HttpProxyCacheServerClients clients : clientsMap.values()) {
                clients.unregisterCacheListener(cacheListener);
            }
        }
    }

    /**
     * Checks is cache contains fully cached file for particular url.
     *
     * @param url an url cache file will be checked for.
     * @return {@code true} if cache contains fully cached file for passed in parameters url.
     */
    private boolean isCacheCompleted(String url) {
        checkNotNull(url, "Url can't be null!");
        return getCacheCompletedFile(url).exists();
    }

    private boolean isCacheUncompleted(String url) {
        return getCacheUncompletedFile(url).exists();
    }

    public void shutdown() {
        LOG.warn("Shutdown proxy server");
        cancelAll();

        config.sourceInfoStorage.release();

        //i think these below should be guarded,except while app was killed..
        waitConnectionThread.interrupt();
        interrupt = true;
        try {
            if (!serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            onError(new ProxyCacheException("Error shutting down proxy server", e));
        }
        socketProcessor.shutdown();
        preloadProcessor.shutdown();
    }

    private boolean isAlive() {
        return pinger.ping(3, 70);   // 70+140+280=max~500ms
    }

    private String appendToProxyUrl(String url) {
        return String.format(Locale.US, "http://%s:%d/%s", PROXY_HOST, port, ProxyCacheUtils.encode(url));
    }

    private File getCacheCompletedFile(String url) {
        File cacheDir = config.cacheRoot;
        String fileName = config.fileNameGenerator.generate(url);
        return new File(cacheDir, fileName);
    }

    private long getCacheCompletedFileSize(String url) {
        File file = getCacheCompletedFile(url);
        return file.exists() ? file.length() : 0;
    }

    private File getCacheUncompletedFile(String url) {
        File file = getCacheCompletedFile(url);
        return new File(file.getParentFile(), file.getName() + TEMP_POSTFIX);
    }

    private long getCacheUncompletedFileSize(String url) {
        File file = getCacheUncompletedFile(url);
        return file.exists() ? file.length() : 0;
    }

    private boolean isPreloadedOrCached(String url) {
        if (isCacheCompleted(url))
            return true;
        if (isCacheUncompleted(url) && getCacheUncompletedFileSize(url) > 0)
            return true;
        return false;
    }

    private long getPreloadedOrCachedSize(String url) {
        if (isCacheCompleted(url))
            return getCacheCompletedFileSize(url);
        if (isCacheUncompleted(url))
            return getCacheUncompletedFileSize(url);
        return 0;
    }

    private void touchFileSafely(File cacheFile) {
        try {
            config.diskUsage.touch(cacheFile);
        } catch (IOException e) {
            LOG.error("Error touching file " + cacheFile, e);
        }
    }

    private void shutdownClients() {
        synchronized (clientsLock) {
            for (HttpProxyCacheServerClients clients : clientsMap.values()) {
                clients.shutdown();
            }
            clientsMap.clear();
        }
    }

    private void waitForRequest(CountDownLatch work) {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Socket socket = serverSocket.accept();
                if (work.getCount() > 0)
                    work.countDown();
                LOG.warn("\n\nrequest " + socket + "...");
                socketProcessor.submit(new SocketProcessorRunnable(socket));
            }
        } catch (IOException e) {
            onError(new ProxyCacheException("Error during waiting connection", e));
        }
    }

    private String redirect(String url) {
        synchronized (redirectLock) {
            if (redirectMap.containsKey(url)) {
                LOG.warn("hit redirect cache " + getTaskId(url));
                return redirectMap.get(url);
            }
        }
        try {
            final int MAX_REDIRECTS = 3;
            final int MAX_ALLOW = 1;
            HttpURLConnection connection;
            boolean redirected;
            int redirectCount = 0;
            String ourl = url;
            do {
                connection = (HttpURLConnection) new URL(url).openConnection();
                connection.setInstanceFollowRedirects(false);
                long start = System.currentTimeMillis();
                int code = connection.getResponseCode();
                if (System.currentTimeMillis() - start >= 200) {
                    LOG.warn("Redirect url too long time:" + (System.currentTimeMillis() - start) + " " + url);
                }
                redirected = code == HTTP_MOVED_PERM || code == HTTP_MOVED_TEMP || code == HTTP_SEE_OTHER;
                if (redirected) {
                    url = connection.getHeaderField("Location");
                    redirectCount++;
                    connection.disconnect();
                }
                if (redirectCount > MAX_REDIRECTS) {
                    throw new ProxyCacheException("Too many redirects: " + redirectCount);
                }
            } while (redirected && redirectCount < MAX_ALLOW);

            if (!ourl.equals(url)) {
                synchronized (redirectLock) {
                    if (!redirectMap.containsKey(ourl)) {
                        redirectMap.put(ourl, url);
                    }
                }
            }
            return url;
        } catch (Exception e) {
            HandyUtil.handle("redirect error.", e);
            return null;
        }
    }

    /**
     * NOTE:distinguish initial url,encoded/decoded url and redirected url
     */
    private String getTaskId(String url) {
        return config.fileNameGenerator.generate(url);
    }

    private void processSocket(Socket socket) {
        String url = null;
        GetRequest request;
        dumpNetwork();

        try {
            request = GetRequest.read(socket.getInputStream());
            url = ProxyCacheUtils.decode(request.uri);

            if (pinger.isPingRequest(url)) {
                pinger.responseToPing(socket);
            } else {
                url = redirect(url);
                if (!request.keyUA) {
                    decreasePendingCount();
                }

                if (url == null)
                    return;

                cleanRunningClient(request, null, url, false);

                boolean isCompleteCached = isCacheCompleted(url);
                long cached = isCompleteCached ? getCacheCompletedFileSize(url) : getCacheUncompletedFileSize(url);

                if (DEBUG)
                    LOG.warn("Request to cache proxy:" + url + " , " + request + " ,isCompleteCached:" + isCompleteCached + ",cached size:" + cached + "  # " + getTaskId(url));

                request.setUri(url);
                HttpProxyCacheServerClients clients = getClients(url);

                synchronized (socketLock) {
                    socketMap.remove(url);
                    socketMap.put(url, socket);
                }

                clients.processRequest(request, socket);

                synchronized (clientsLock) {
                    clientsMap.remove(url);
                }

                synchronized (socketLock) {
                    socketMap.remove(url);
                }
            }
        } catch (SocketException e) {
            // There is no way to determine that client closed connection http://stackoverflow.com/a/10241044/999458
            // So just to prevent log flooding don't log stacktrace
            LOG.warn("Closing socket… Socket is closed by client.");
        } catch (Exception e) {
            LOG.warn("Error processing request:" + e.getMessage());
            e.printStackTrace();
            onError(new ProxyCacheException("Error processing request", e));
        } finally {
            releaseSocket(socket);
            LOG.warn("Opened connections: " + getClientsCount() + " ,pending:" + getPendingCount());
        }
        if (!pinger.isPingRequest(url)) {
            synchronized (preloadLock) {
                preloadLock.notifyAll();
            }
        }
    }

    private HttpProxyCacheServerClients getClients(String url) throws ProxyCacheException {
        synchronized (clientsLock) {
            HttpProxyCacheServerClients clients = clientsMap.get(url);
            if (clients == null) {
                clients = new HttpProxyCacheServerClients(null, url, config);
                clientsMap.put(url, clients);
            }
            return clients;
        }
    }

    private int getClientsCount() {
        synchronized (clientsLock) {
            int count = 0;
            for (HttpProxyCacheServerClients clients : clientsMap.values()) {
                count += clients.getClientsCount();
            }
            return count;
        }
    }

    private void releaseSocket(Socket socket) {
        closeSocketInput(socket);
        closeSocketOutput(socket);
        closeSocket(socket);
    }

    private void closeSocketInput(Socket socket) {
        try {
            if (!socket.isInputShutdown()) {
                socket.shutdownInput();
            }
        } catch (SocketException e) {
            // There is no way to determine that client closed connection http://stackoverflow.com/a/10241044/999458
            // So just to prevent log flooding don't log stacktrace
            LOG.debug("Releasing input stream… Socket is closed by client.");
        } catch (IOException e) {
            onError(new ProxyCacheException("Error closing socket input stream", e));
        }
    }

    private void closeSocketOutput(Socket socket) {
        try {
            if (!socket.isOutputShutdown()) {
                socket.shutdownOutput();
            }
        } catch (IOException e) {
            LOG.warn("Failed to close socket on proxy side: {}. It seems client have already closed connection.", e.getMessage());
        }
    }

    private void closeSocket(Socket socket) {
        try {
            if (!socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            onError(new ProxyCacheException("Error closing socket", e));
        }
    }

    private void onError(Throwable e) {
        LOG.error("HttpProxyCacheServer error", e);
    }

    private final class WaitRequestsRunnable implements Runnable {

        private final CountDownLatch startSignal;
        private final CountDownLatch work;

        public WaitRequestsRunnable(CountDownLatch startSignal, CountDownLatch work) {
            this.startSignal = startSignal;
            this.work = work;
        }

        @Override
        public void run() {
            startSignal.countDown();
            waitForRequest(work);
        }
    }

    private final class SocketProcessorRunnable implements Runnable {

        private final Socket socket;

        public SocketProcessorRunnable(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            processSocket(socket);
        }
    }

    public static class ContentTypeProviderFactory {
        public static IContentTypeProvider createEmptyContentTypeProvider() {
            return new IContentTypeProvider() {
                @Override
                public String getMIME() {
                    return "";
                }
            };
        }
    }

    /**
     * Builder for {@link HttpProxyCacheServer}.
     */
    public static final class Builder {

        public static final long DEFAULT_MAX_SIZE = 512 * 1024 * 1024;

        private File cacheRoot;
        private FileNameGenerator fileNameGenerator;
        private DiskUsage diskUsage;
        private SourceInfoStorage sourceInfoStorage;
        private HeaderInjector headerInjector;
        private IContentTypeProvider mime;

        public Builder(Context context) {
            HttpProxyCacheServer.context = context;
            this.sourceInfoStorage = SourceInfoStorageFactory.newSourceInfoStorage(context);
            this.cacheRoot = context.getFilesDir();
            this.diskUsage = new TotalSizeLruDiskUsage(DEFAULT_MAX_SIZE);
            this.fileNameGenerator = new Md5FileNameGenerator();
            this.headerInjector = new EmptyHeadersInjector();
        }

        /**
         * Overrides default cache folder to be used for caching files.
         * <p>
         * By default AndroidVideoCache uses
         * '/Android/data/[app_package_name]/cache/video-cache/' if card is mounted and app has appropriate permission
         * or 'video-cache' subdirectory in default application's cache directory otherwise.
         * </p>
         * <b>Note</b> directory must be used <b>only</b> for AndroidVideoCache files.
         *
         * @param file a cache directory, can't be null.
         * @return a builder.
         */
        private Builder cacheDirectory(File file) {
            this.cacheRoot = checkNotNull(file);
            return this;
        }

        /**
         * Overrides default cache file name generator {@link Md5FileNameGenerator} .
         *
         * @param fileNameGenerator a new file name generator.
         * @return a builder.
         */
        public Builder fileNameGenerator(FileNameGenerator fileNameGenerator) {
            this.fileNameGenerator = checkNotNull(fileNameGenerator);
            return this;
        }

        /**
         * Sets max cache size in bytes.
         * <p>
         * All files that exceeds limit will be deleted using LRU strategy.
         * Default value is 512 Mb.
         * </p>
         * Note this method overrides result of calling {@link #maxCacheFilesCount(int)}
         *
         * @param maxSize max cache size in bytes.
         * @return a builder.
         */
        public Builder maxCacheSize(long maxSize) {
            this.diskUsage = new TotalSizeLruDiskUsage(maxSize);
            return this;
        }

        /**
         * Sets max cache files count.
         * All files that exceeds limit will be deleted using LRU strategy.
         * Note this method overrides result of calling {@link #maxCacheSize(long)}
         *
         * @param count max cache files count.
         * @return a builder.
         */
        public Builder maxCacheFilesCount(int count) {
            this.diskUsage = new TotalCountLruDiskUsage(count);
            return this;
        }

        /**
         * Set custom DiskUsage logic for handling when to keep or clean cache.
         *
         * @param diskUsage a disk usage strategy, cant be {@code null}.
         * @return a builder.
         */
        public Builder diskUsage(DiskUsage diskUsage) {
            this.diskUsage = checkNotNull(diskUsage);
            return this;
        }

        /**
         * set HTTP response Content-Type if possible
         */
        public Builder mimeProvider(IContentTypeProvider mime) {
            this.mime = checkNotNull(mime);
            return this;
        }

        /**
         * Add headers along the request to the server
         *
         * @param headerInjector to inject header base on url
         * @return a builder
         */
        public Builder headerInjector(HeaderInjector headerInjector) {
            this.headerInjector = checkNotNull(headerInjector);
            return this;
        }

        /**
         * Builds new instance of {@link HttpProxyCacheServer}.
         *
         * @return proxy cache. Only single instance should be used across whole app.
         */
        public HttpProxyCacheServer build() {
            Config config = buildConfig();
            return new HttpProxyCacheServer(config);
        }

        private Config buildConfig() {
            return new Config(cacheRoot, fileNameGenerator, diskUsage, sourceInfoStorage, headerInjector, mime);
        }

    }
}
