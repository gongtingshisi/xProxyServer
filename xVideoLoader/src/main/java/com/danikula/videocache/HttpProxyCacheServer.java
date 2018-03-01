package com.danikula.videocache;

import android.content.Context;
import android.net.Uri;
import android.util.Log;
import android.webkit.URLUtil;

import com.danikula.videocache.file.DiskUsage;
import com.danikula.videocache.file.FileCache;
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
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.danikula.videocache.Preconditions.checkAllNotNull;
import static com.danikula.videocache.Preconditions.checkNotNull;
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
    private final ServerSocket serverSocket;
    private final int port;
    private final Thread waitConnectionThread;
    private final Config config;
    private final Pinger pinger;
    private final ExecutorService preloadProcessor = Executors.newCachedThreadPool();
    private final Object redirectLock = new Object();
    private long speed;
    private ArrayBlockingQueue<PreloadInfo> blockingQueue = new ArrayBlockingQueue<>(50);
    private final Object preloadLock = new Object();
    private boolean interrupt = false;
    private Thread conductPreloadThread = new Thread() {
        @Override
        public void run() {
            while (!interrupt) {
                synchronized (preloadLock) {
                    int put = 0;
                    int allow = getMaxAllowedAccept();
                    LOG.warn("max accepted:" + allow + " ，queue :" + blockingQueue.size() + " ,running:" + getClientsCount());
                    do {
                        if (allow > 0 && !blockingQueue.isEmpty()) {
                            PreloadInfo info = blockingQueue.poll();
                            LOG.warn("### Start preload " + info);
                            preloadProcessor.submit(new PreloadRunnable(info.title, info.url, 0, getOfferSize(info), info.cacheListener));
                            put++;
                        } else
                            break;
                    } while (put < allow - getClientsCount());

                    try {
                        preloadLock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    };

    /**
     * whether we should give an expire timeout?
     */
    private final Map<String, String> redirectMap = new ConcurrentHashMap<>();

    public HttpProxyCacheServer(Context context) {
        this(new Builder(context).buildConfig());
    }

    private HttpProxyCacheServer(Config config) {
        this.config = checkNotNull(config);
        try {
            InetAddress inetAddress = InetAddress.getByName(PROXY_HOST);
            this.serverSocket = new ServerSocket(0, 0, inetAddress);
            this.port = serverSocket.getLocalPort();
            IgnoreHostProxySelector.install(PROXY_HOST, port);
            CountDownLatch startSignal = new CountDownLatch(1);
            this.waitConnectionThread = new Thread(new WaitRequestsRunnable(startSignal));
            this.waitConnectionThread.start();
            startSignal.await(); // freeze thread, wait for server starts
            this.pinger = new Pinger(PROXY_HOST, port);
            LOG.debug("Proxy cache server started. Is it alive? " + isAlive());
            conductPreloadThread.start();
        } catch (IOException | InterruptedException e) {
            socketProcessor.shutdown();
            throw new IllegalStateException("Error starting local proxy server", e);
        }
    }

    /**
     * Returns url that wrap original url and should be used for client (MediaPlayer, ExoPlayer, etc).
     * <p>
     * If file for this url is fully cached (it means method {@link #isCached(String)} returns {@code true})
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
        String title;
        String url;
        long duration;
        int offer;
        CacheListener cacheListener;

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
                    ", url='" + url + '\'' +
                    ", duration=" + duration +
                    ", offer=" + offer +
                    ", cacheListener=" + cacheListener +
                    '}';
        }
    }

    enum SPEED {
        NONE,
        SLOW,
        NORMAL,
        HIGH
    }

    public static final int SPEED_NONE_MAX_NUM = 0;
    public static final int SPEED_SLOW_MAX_NUM = 1;
    public static final int SPEED_NORMAL_MAX_NUM = 2;
    public static final int SPEED_HIGH_MAX_NUM = 3;

    private SPEED getSpeed() {
        speed = HttpProxyCache.speed;
        LOG.warn("instant single speed:" + speed);
        if (speed < 0) {
            return getClientsCount() > 0 ? SPEED.NORMAL : SPEED.SLOW;
        } else if (speed < 1 * 1024)
            return SPEED.NONE;
        else if (speed < 30 * 1024)
            return SPEED.SLOW;
        else if (speed < 100 * 1024)
            return SPEED.NORMAL;
        else
            return SPEED.HIGH;
    }

    private int getMaxAllowedAccept() {
        int allow = 0;
        switch (getSpeed()) {
            case NONE:
                allow = SPEED_NONE_MAX_NUM - getClientsCount();
                break;
            case SLOW:
                allow = SPEED_SLOW_MAX_NUM - getClientsCount();
                break;
            case NORMAL:
                allow = SPEED_NORMAL_MAX_NUM - getClientsCount();
                break;
            case HIGH:
                allow = SPEED_HIGH_MAX_NUM - getClientsCount();
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
     * @param second        :time to preload,unit:second
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
        synchronized (preloadLock) {
            if (!blockingQueue.contains(info) && !isCacheCompleted(url) && !isCached(url)) {
                LOG.warn("New a preload task:" + title);
                try {
                    int size = blockingQueue.size();
                    blockingQueue.add(info);
                    if (size == 0)
                        preloadLock.notifyAll();
                } catch (IllegalStateException e) {
                    LOG.error("Preload pool full exception ~~~ throw " + title);
                }
            }
        }
//        }
    }

    /**
     * cancel a preload task if it hasn't been running.return true on success,otherwise false
     *
     * @param title :unique title
     * @param url   :the url to cancel
     */
    public boolean cancel(String title, String url) {
        if (url == null)
            throw new IllegalArgumentException("Plz give a legal input!");
        synchronized (preloadLock) {
            if (blockingQueue.contains(url)) {
                LOG.warn("Cancel preload " + title + " :" + url);
                return blockingQueue.remove(url);
            }
        }
        return false;
    }

    private long getOfferSize(PreloadInfo info) {
        int bitrate = 400 * 1000;
        return (512 * info.duration + bitrate * info.offer / 8);
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

    /**
     * return offset size of preloaded partial file.0 on failure.
     */
    private long getCacheFileSize(String url) {
        File file = getCacheFile(url);
        return file.exists() ? file.length() : 0;
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
                url = redirect(url);
                if (url == null)
                    return;
                LOG.warn(title + " # Preload key:" + config.fileNameGenerator.generate(url));
                String proxy = getProxyUrl(url);
                if (URLUtil.isFileUrl(proxy)) {
                    LOG.warn("Local cache found,preload success # " + title);
                    return;
                }

                synchronized (clientsLock) {
                    HttpProxyCacheServerClients clients = clientsMap.get(url);
                    if (clients == null) {
                        clients = new HttpProxyCacheServerClients(title, url, config, size, cacheListener);
                        clientsMap.put(url, clients);
                    }
                }
                urlSource = new HttpUrlSource(title, proxy);
                urlSource.openPartial(0, size);
            } catch (Exception e) {
                HandyUtil.handle("preload " + this.urlSource, e);
            }
        }
    }

    /**
     * Returns url that wrap original url and should be used for client (MediaPlayer, ExoPlayer, etc).
     * <p>
     * If parameter {@code allowCachedFileUri} is {@code true} and file for this url is fully cached
     * (it means method {@link #isCached(String)} returns {@code true}) then file:// uri to cached file will be returned.
     *
     * @param url                a url to file that should be cached.
     * @param allowCachedFileUri {@code true} if allow to return file:// uri if url is fully cached
     * @return a wrapped by proxy url if file is not fully cached or url pointed to cache file otherwise (if {@code allowCachedFileUri} is {@code true}).
     */
    private String getProxyUrl(String url, boolean allowCachedFileUri) {
        if (allowCachedFileUri && isCached(url)) {
            File cacheFile = getCacheFile(url);
            touchFileSafely(cacheFile);
            return Uri.fromFile(cacheFile).toString();
        }
        return isAlive() ? appendToProxyUrl(url) : url;
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
    private boolean isCached(String url) {
        checkNotNull(url, "Url can't be null!");
        return getCacheFile(url).exists();
    }

    public void shutdown() {
        LOG.warn("Shutdown proxy server");

        shutdownClients();

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
    }

    private boolean isAlive() {
        return pinger.ping(3, 70);   // 70+140+280=max~500ms
    }

    private String appendToProxyUrl(String url) {
        return String.format(Locale.US, "http://%s:%d/%s", PROXY_HOST, port, ProxyCacheUtils.encode(url));
    }

    private File getCacheFile(String url) {
        File cacheDir = config.cacheRoot;
        String fileName = config.fileNameGenerator.generate(url);

        return new File(cacheDir, fileName);
    }

    private boolean isCacheCompleted(String url) {
        try {
            return new FileCache(getCacheFile(url), config.diskUsage).isCompleted();
        } catch (ProxyCacheException e) {
            e.printStackTrace();
        }
        return false;
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

    private void waitForRequest() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Socket socket = serverSocket.accept();
                LOG.warn("Accept new socket " + socket);
                socketProcessor.submit(new SocketProcessorRunnable(socket));
            }
        } catch (IOException e) {
            onError(new ProxyCacheException("Error during waiting connection", e));
        }
    }

    private String redirect(String url) {
        synchronized (redirectLock) {
            if (redirectMap.containsKey(url)) {
                LOG.warn("hit redirect cache");
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
                connection.setConnectTimeout(3 * 1000);
                int code = connection.getResponseCode();
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

    /**
     * return false on ping,otherwise true
     */
    private void processSocket(Socket socket) {
        String url = null;
        try {
            //If this request is from player which ranges from a keyframe position,not from last cache position.
            //  1.we'd cancel previous network request.
            //  2.whether the previous cached file fragment is thrown or reused.
            GetRequest request = GetRequest.read(socket.getInputStream());

            url = ProxyCacheUtils.decode(request.uri);

            if (pinger.isPingRequest(url)) {
                pinger.responseToPing(socket);
            } else {
                url = redirect(url);
                if (url == null)
                    return;

                /**
                 * if has preload running
                 * */
                long cached = getCacheFileSize(url);
                if (Log.isLoggable("HttpProxyCacheServer", Log.DEBUG))
                    LOG.warn("Request to cache proxy:" + url + " , " + request + ",cached size:" + cached + "  # " + getTaskId(url));

                synchronized (clientsLock) {
                    if (clientsMap.containsKey(url) && request.keyUA && !isCacheCompleted(url)) {
                        LOG.warn("####### (Cancel) Previous running preload,ready to pick up uncompleted cache," + " # " + getTaskId(url) + " if possible !!!");
                        //same url will access twice in a sequence.
//                        clientsMap.get(url).shutdown();
                    }
                }
                request.setUri(url);
                if (cached != 0 && request.keyFrameRequest && request.rangeOffset > cached) {
                    LOG.warn("searching key frame info out of range.. this will lead to load slow.\n" +
                            "              1.we ought to cancel previous network request.\n" +
                            "              2.whether the previous cached file fragment is thrown or reused..");
                }
                long offset = request.keyFrameRequest ? request.rangeOffset : cached;

                HttpProxyCacheServerClients clients = getClients(url);
                boolean continuePartial = false;
                if (offset != 0) {
                    continuePartial = !request.keyFrameRequest;
                    LOG.warn("Continue "/* + (uncompleted ? "uncompleted" : "complete")*/ + "partial cache,load from:" + offset + " # " + getTaskId(url));
                    request.setRangeOffset(offset);
                    clients.clearRequestSize();
                }
                clients.processRequest(request, socket, continuePartial);
                speed = clients.getCurrentSpeed();

                synchronized (clientsLock) {
                    if (clientsMap.containsKey(url)) {
                        clientsMap.remove(url);
                    }
                }
            }
        } catch (SocketException e) {
            // There is no way to determine that client closed connection http://stackoverflow.com/a/10241044/999458
            // So just to prevent log flooding don't log stacktrace
            LOG.warn("Closing socket… Socket is closed by client.");
        } catch (ProxyCacheException | IOException e) {
            LOG.warn("Error processing request:" + e.getMessage());
            onError(new ProxyCacheException("Error processing request", e));
        } finally {
            releaseSocket(socket);
            LOG.warn("Opened connections: " + getClientsCount());
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
                clients = new HttpProxyCacheServerClients(null, url, config, Integer.MIN_VALUE);
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

        public WaitRequestsRunnable(CountDownLatch startSignal) {
            this.startSignal = startSignal;
        }

        @Override
        public void run() {
            startSignal.countDown();
            waitForRequest();
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

    /**
     * Builder for {@link HttpProxyCacheServer}.
     */
    public static final class Builder {

        private static final long DEFAULT_MAX_SIZE = 512 * 1024 * 1024;

        private File cacheRoot;
        private FileNameGenerator fileNameGenerator;
        private DiskUsage diskUsage;
        private SourceInfoStorage sourceInfoStorage;
        private HeaderInjector headerInjector;

        public Builder(Context context) {
            this.sourceInfoStorage = SourceInfoStorageFactory.newSourceInfoStorage(context);
            this.cacheRoot = context.getFilesDir();//StorageUtils.getIndividualCacheDirectory(context);
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
            return new Config(cacheRoot, fileNameGenerator, diskUsage, sourceInfoStorage, headerInjector);
        }

    }
}
