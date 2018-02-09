package com.danikula.videocache;

import android.content.Context;
import android.net.Uri;
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
import java.util.Locale;
import java.util.Map;
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
 * Simple lightweight proxy server with file caching support that handles HTTP requests.
 * Typical usage:
 * <pre><code>
 * public onCreate(Bundle state) {
 *      super.onCreate(state);
 *
 *      HttpProxyCacheServer proxy = getProxy();
 *      String proxyUrl = proxy.getProxyUrl(VIDEO_URL);
 *      videoView.setVideoPath(proxyUrl);
 * }
 *
 * private HttpProxyCacheServer getProxy() {
 * // should return single instance of HttpProxyCacheServer shared for whole app.
 * }
 * </code></pre>
 *
 * @author Alexey Danilov (danikula@gmail.com).
 */
public class HttpProxyCacheServer {
    private static final Logger LOG = LoggerFactory.getLogger("HttpProxyCacheServer");
    private static final String PROXY_HOST = "127.0.0.1";

    private final Object clientsLock = new Object();
    private final ExecutorService socketProcessor = Executors.newFixedThreadPool(8);
    private final Map<String, HttpProxyCacheServerClients> clientsMap = new ConcurrentHashMap<>();
    private final ServerSocket serverSocket;
    private final int port;
    private final Thread waitConnectionThread;
    private final Config config;
    private final Pinger pinger;
    private final ExecutorService preloadProcessor = Executors.newFixedThreadPool(6);
    private final Object redirectLock = new Object();
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

    /**
     * preload partial data in advance
     *
     * @param url      :the url to load
     * @param duration :video duration,unit:second
     * @param second   :time to preload,unit:second
     */
    public void preload(String url, long duration, int second) {
        int bitrate = 200 * 1000;
        long size = (512 * duration + bitrate * second / 8);
        synchronized (redirectLock) {
            if (redirectMap.containsKey(url)) {
                String redirected = redirectMap.get(url);
                synchronized (clientsLock) {
                    if (clientsMap.containsKey(redirected)) {
                        LOG.warn(url + " is running,ignore.");
                        return;
                    }
                }
            }
        }
        LOG.warn("preload " + url + ", size:" + size);
        preloadProcessor.submit(new PreloadRunnable(url, 0, size));
    }

    public void request(String url) {
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
     * return offset size of preloaded partial file.-1 on failure.
     */
    private long getCacheFieSize(String url) {
        File file = getCacheFile(url);
        return file.exists() ? file.length() : -1;
    }

    class PreloadRunnable implements Runnable {
        String url;
        long offset;
        long size;
        HttpUrlSource urlSource;

        public PreloadRunnable(String url, long offset, long size) {
            this.url = url;
            this.offset = offset;
            this.size = size;
        }

        @Override
        public void run() {
            String before = url;
            try {
                url = redirect(url);
                if (url == null)
                    return;
                LOG.warn(before + " # Preload key:" + config.fileNameGenerator.generate(url));
                String proxy = getProxyUrl(url);
                if (URLUtil.isFileUrl(proxy)) {
                    LOG.warn("Local cache found,preload success # " + getTaskId(url));
                    return;
                }

                synchronized (clientsLock) {
                    HttpProxyCacheServerClients clients = clientsMap.get(url);
                    if (clients == null) {
                        clients = new HttpProxyCacheServerClients(url, config, size);
                        clientsMap.put(url, clients);
                    }
                }
                urlSource = new HttpUrlSource(proxy);
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
            long time = System.currentTimeMillis();
            String ourl = url;
            do {
                connection = (HttpURLConnection) new URL(url).openConnection();
                connection.setInstanceFollowRedirects(false);

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
            LOG.warn("redirect " + ourl + " time: " + (System.currentTimeMillis() - time) + " ##### " + getTaskId(url));

            time = System.currentTimeMillis();
            if (!ourl.equals(url)) {
                synchronized (redirectLock) {
                    if (!redirectMap.containsKey(ourl)) {
                        redirectMap.put(ourl, url);
                    }
                }
            }
            LOG.warn("redirect finish " + ourl + " TTTOOO " + url + " time: " + (System.currentTimeMillis() - time) + " ##### " + getTaskId(url));
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
        try {
            GetRequest request = GetRequest.read(socket.getInputStream());

            String url = ProxyCacheUtils.decode(request.uri);

            if (pinger.isPingRequest(url)) {
                pinger.responseToPing(socket);
            } else {
//                boolean uncompleted = false;
                url = redirect(url);
                if (url == null)
                    return;
                LOG.warn("Request to cache proxy:" + url + " , " + request + " # " + getTaskId(url));
                //todo:if we play the in-preloading state one,we ought to cancel the task,pick up downloaded part.
//                synchronized (clientsLock) {
//                    if (clientsMap.containsKey(url) && clientsMap.get(url).getRequestSize() != Integer.MIN_VALUE) {
//                        LOG.warn("Cancel running preload,ready to pick up uncompleted cache," + " # " + getTaskId(url));
//                        clientsMap.get(url).shutdown();
//                        uncompleted = true;
//                    }
//                }
                request.setUri(url);
                long offset = request.keyFrameRequest ? request.rangeOffset : getCacheFieSize(url);

                HttpProxyCacheServerClients clients = getClients(url);
                boolean continuePartial = false;
                if (offset != -1) {
                    continuePartial = !request.keyFrameRequest;
                    LOG.warn("Continue "/* + (uncompleted ? "uncompleted" : "complete")*/ + " partial cache,load from:" + offset + " # " + getTaskId(url));
                    request.setRangeOffset(offset);
                    clients.clearRequestSize();
                }
                clients.processRequest(request, socket, continuePartial);

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
    }

    private HttpProxyCacheServerClients getClients(String url) throws ProxyCacheException {
        synchronized (clientsLock) {
            HttpProxyCacheServerClients clients = clientsMap.get(url);
            if (clients == null) {
                clients = new HttpProxyCacheServerClients(url, config, Integer.MIN_VALUE);
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
            this.cacheRoot = StorageUtils.getIndividualCacheDirectory(context);
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
        public Builder cacheDirectory(File file) {
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
