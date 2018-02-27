package com.danikula.videocache;

import com.danikula.videocache.file.FileNameGenerator;
import com.danikula.videocache.file.Md5FileNameGenerator;
import com.danikula.videocache.headers.EmptyHeadersInjector;
import com.danikula.videocache.headers.HeaderInjector;
import com.danikula.videocache.sourcestorage.SourceInfoStorage;
import com.danikula.videocache.sourcestorage.SourceInfoStorageFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

import static com.danikula.videocache.Preconditions.checkNotNull;
import static com.danikula.videocache.ProxyCacheUtils.DEFAULT_BUFFER_SIZE;
import static java.net.HttpURLConnection.HTTP_MOVED_PERM;
import static java.net.HttpURLConnection.HTTP_MOVED_TEMP;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_PARTIAL;
import static java.net.HttpURLConnection.HTTP_SEE_OTHER;

/**
 * {@link Source} that uses http resource as source for {@link ProxyCache}.
 *
 * @author Alexey Danilov (danikula@gmail.com).
 */
public class HttpUrlSource implements Source {
    private static final Logger LOG = LoggerFactory.getLogger("HttpUrlSource");

    private static final int MAX_REDIRECTS = 5;
    private final SourceInfoStorage sourceInfoStorage;
    private final HeaderInjector headerInjector;
    private SourceInfo sourceInfo;
    private HttpURLConnection connection;
    private InputStream inputStream;
    private String id;

    public long getRequestSize() {
        return sourceInfo.requestSize;
    }

    public HttpUrlSource(String title, String url) {
        this(title, url, SourceInfoStorageFactory.newEmptySourceInfoStorage());
    }

    public HttpUrlSource(String title, String url, SourceInfoStorage sourceInfoStorage) {
        this(title, url, sourceInfoStorage, new EmptyHeadersInjector(), new Md5FileNameGenerator());
    }

    public HttpUrlSource(String title, String url, SourceInfoStorage sourceInfoStorage, HeaderInjector headerInjector) {
        this(title, url, sourceInfoStorage, headerInjector, new Md5FileNameGenerator());
    }

    public HttpUrlSource(String title, String url, SourceInfoStorage sourceInfoStorage, HeaderInjector headerInjector, FileNameGenerator nameGenerator) {
        this.sourceInfoStorage = checkNotNull(sourceInfoStorage);
        this.headerInjector = checkNotNull(headerInjector);
        SourceInfo sourceInfo = sourceInfoStorage.get(url);
        this.sourceInfo = sourceInfo != null ? sourceInfo :
                new SourceInfo(title, url, Integer.MIN_VALUE, ProxyCacheUtils.getSupposablyMime(url), Integer.MIN_VALUE);
        this.id = nameGenerator.generate(url);
    }

    public HttpUrlSource(String title, String url, SourceInfoStorage sourceInfoStorage, HeaderInjector headerInjector, boolean continuePartial) {
        this(title, url, sourceInfoStorage, headerInjector, new Md5FileNameGenerator(), continuePartial);
    }

    public HttpUrlSource(String title, String url, SourceInfoStorage sourceInfoStorage, HeaderInjector headerInjector, FileNameGenerator nameGenerator, boolean continuePartial) {
        this.sourceInfoStorage = checkNotNull(sourceInfoStorage);
        this.headerInjector = checkNotNull(headerInjector);
        SourceInfo sourceInfo = continuePartial ? null : sourceInfoStorage.get(url);
        this.sourceInfo = sourceInfo != null ? sourceInfo :
                new SourceInfo(title, url, Integer.MIN_VALUE, ProxyCacheUtils.getSupposablyMime(url), Integer.MIN_VALUE);
        this.id = nameGenerator.generate(url);
    }

    public HttpUrlSource(HttpUrlSource source) {
        this.sourceInfo = source.sourceInfo;
        this.sourceInfoStorage = source.sourceInfoStorage;
        this.headerInjector = source.headerInjector;
    }

    @Override
    public synchronized long length() throws ProxyCacheException {
        if (sourceInfo.length == Integer.MIN_VALUE) {
            fetchContentInfo();
        }
        return sourceInfo.length;
    }

    @Override
    public void open(long offset) throws ProxyCacheException {
        try {
            connection = openConnection(offset, -1);
            String mime = connection.getContentType();
            inputStream = new BufferedInputStream(connection.getInputStream(), DEFAULT_BUFFER_SIZE);
            long length = readSourceAvailableBytes(connection, offset, connection.getResponseCode());

            this.sourceInfo = new SourceInfo(sourceInfo.title, sourceInfo.url, length, mime, getContentLength(connection));
            this.sourceInfoStorage.put(sourceInfo.url, sourceInfo);
        } catch (IOException e) {
            LOG.warn("Error opening connection for " + sourceInfo.url + " with offset " + offset + ", " + e.getMessage() + " # " + id);
            e.printStackTrace();
        }
    }

    @Override
    public void openPartial(long offset, long size) throws ProxyCacheException {
        try {
            LOG.warn("openPartial " + getTitle() + " offset:" + offset + ",size:" + size);
            connection = openConnection(offset, size, -1);
            String mime = connection.getContentType();
            inputStream = new BufferedInputStream(connection.getInputStream(), DEFAULT_BUFFER_SIZE);
            long length = readSourceAvailableBytes(connection, offset, connection.getResponseCode());
            this.sourceInfo = new SourceInfo(sourceInfo.title, sourceInfo.url, length, mime, size);
            this.sourceInfoStorage.put(sourceInfo.url, sourceInfo);
        } catch (IOException e) {
            LOG.warn("Error openPartial connection for " + sourceInfo.url + " with offset " + offset + ", " + e.getMessage() + " # " + id);
            e.printStackTrace();
        }
    }

    private long readSourceAvailableBytes(HttpURLConnection connection, long offset, int responseCode) throws IOException {
        long contentLength = getContentLength(connection);
        return responseCode == HTTP_OK ? contentLength
                : responseCode == HTTP_PARTIAL ? contentLength + offset : sourceInfo.length;
    }

    private long getContentLength(HttpURLConnection connection) {
        String contentLengthValue = connection.getHeaderField("Content-Length");
        return contentLengthValue == null ? -1 : Long.parseLong(contentLengthValue);
    }

    @Override
    public void close() throws ProxyCacheException {
        if (connection != null) {
            try {
                connection.disconnect();
            } catch (NullPointerException | IllegalArgumentException e) {
                String message = "Wait... but why? WTF!? " +
                        "Really shouldn't happen any more after fixing https://github.com/danikula/AndroidVideoCache/issues/43. " +
                        "If you read it on your device log, please, notify me danikula@gmail.com or create issue here " +
                        "https://github.com/danikula/AndroidVideoCache/issues.";
                throw new RuntimeException(message, e);
            } catch (ArrayIndexOutOfBoundsException e) {
                LOG.error("Error closing connection correctly. Should happen only on Android L. " +
                        "If anybody know how to fix it, please visit https://github.com/danikula/AndroidVideoCache/issues/88. " +
                        "Until good solution is not know, just ignore this issue :(", e);
            }
        }
    }

    @Override
    public int read(byte[] buffer) throws ProxyCacheException {
        if (inputStream == null) {
            throw new ProxyCacheException("Error reading data from " + sourceInfo.url + ": connection is absent!");
        }
        try {
            return inputStream.read(buffer, 0, buffer.length);
        } catch (InterruptedIOException e) {
            throw new InterruptedProxyCacheException("Reading source " + sourceInfo.url + " is interrupted", e);
        } catch (IOException e) {
            throw new ProxyCacheException("Error reading data from " + sourceInfo.url, e);
        }
    }

    private void fetchContentInfo() throws ProxyCacheException {
        HttpURLConnection urlConnection = null;
        try {
            urlConnection = openConnectionViaHeader(15000);
            long length = getContentLength(urlConnection);
            String mime = urlConnection.getContentType();
            this.sourceInfo = new SourceInfo(sourceInfo.title, sourceInfo.url, length, mime, Integer.MIN_VALUE);
            this.sourceInfoStorage.put(sourceInfo.url, sourceInfo);
            LOG.debug("Source info fetched: " + sourceInfo + " # " + id);
        } catch (IOException e) {
            LOG.error("Error fetching info from " + sourceInfo.url + " # " + id, e);
        } finally {
//            ProxyCacheUtils.close(inputStream);
            if (urlConnection != null) {
                LOG.debug(id + " fetchContentInfo disconnect");
                urlConnection.disconnect();
            }
        }
    }

    private void fetchContentInfo(long requestSize) throws ProxyCacheException {
        LOG.warn("Read content info from " + sourceInfo.url + " # " + id);
        HttpURLConnection urlConnection = null;
        try {
            urlConnection = openConnectionViaHeader(10000);
            long length = getContentLength(urlConnection);
            String mime = urlConnection.getContentType();
            this.sourceInfo = new SourceInfo(sourceInfo.title, sourceInfo.url, length, mime, requestSize);
            this.sourceInfoStorage.put(sourceInfo.url, sourceInfo);
            LOG.debug("Source info fetched: " + sourceInfo + " # " + id);
        } catch (IOException e) {
            LOG.error("Error fetching info from " + sourceInfo.url + " # " + id, e);
        } finally {
//            ProxyCacheUtils.close(inputStream);
            if (urlConnection != null) {
                LOG.warn("disconnect fetchContentInfo " + sourceInfo.url + " # " + id);
                urlConnection.disconnect();
            }
        }
    }

    private HttpURLConnection openConnectionViaHeader(int timeout) throws IOException, ProxyCacheException {
        HttpURLConnection connection;
        boolean redirected;
        int redirectCount = 0;
        String url = this.sourceInfo.url;

        do {
            connection = (HttpURLConnection) new URL(url).openConnection();
            connection.setInstanceFollowRedirects(false);
            if (timeout > 0) {
                connection.setConnectTimeout(timeout);
                connection.setReadTimeout(timeout);
            }
            connection.setRequestMethod("HEAD");
            int code = connection.getResponseCode();
            redirected = code == HTTP_MOVED_PERM || code == HTTP_MOVED_TEMP || code == HTTP_SEE_OTHER;
            if (redirected) {
                url = connection.getHeaderField("Location");
                redirectCount++;
                connection.disconnect();
                LOG.warn("Redirect closed:" + url + " # " + id);
            }
            if (redirectCount > MAX_REDIRECTS) {
                throw new ProxyCacheException("Too many redirects: " + redirectCount);
            }
        } while (redirected);
        return connection;
    }

    private HttpURLConnection openConnection(long offset, int timeout) throws IOException, ProxyCacheException {
        HttpURLConnection connection;
        boolean redirected;
        int redirectCount = 0;
        String url = this.sourceInfo.url;
        do {
            if (!Pinger.isPing(url))
                LOG.debug("Open connection " + (offset > 0 ? "with offset " + offset : "") + " to " + url + " # " + id);
            URL Url = new URL(url);
            connection = (HttpURLConnection) Url.openConnection();

            injectCustomHeaders(connection, url);
            connection.setInstanceFollowRedirects(false);

            if (offset > 0) {
                connection.setRequestProperty("Range", "bytes=" + offset + "-");
            }
            if (timeout > 0) {
                connection.setConnectTimeout(timeout);
                connection.setReadTimeout(timeout);
            }
            int code = connection.getResponseCode();
            redirected = code == HTTP_MOVED_PERM || code == HTTP_MOVED_TEMP || code == HTTP_SEE_OTHER;
            if (redirected) {
                url = connection.getHeaderField("Location");
                redirectCount++;
                LOG.warn("redirected,close connection " + sourceInfo.url + " # " + id);
                connection.disconnect();
            }
            if (redirectCount > MAX_REDIRECTS) {
                throw new ProxyCacheException("Too many redirects: " + redirectCount);
            }
        } while (redirected);
        return connection;
    }

    /**
     * 从offset处请求size大小
     */
    private HttpURLConnection openConnection(long offset, long size, int timeout) throws IOException, ProxyCacheException {
        HttpURLConnection connection;
        boolean redirected;
        int redirectCount = 0;
        String url = this.sourceInfo.url;
        do {
            if (!Pinger.isPing(url))
                LOG.warn("Open connection " + getTitle() + " offset " + offset + " ,size: " + size + ", " + url + " # " + id);
            connection = (HttpURLConnection) new URL(url).openConnection();
            injectCustomHeaders(connection, url);
            connection.setInstanceFollowRedirects(false);
            connection.setRequestProperty("Range", "bytes=" + offset + "-" + (offset + size - 1));
            if (timeout > 0) {
                connection.setConnectTimeout(timeout);
                connection.setReadTimeout(timeout);
            }
            int code = connection.getResponseCode();
            redirected = code == HTTP_MOVED_PERM || code == HTTP_MOVED_TEMP || code == HTTP_SEE_OTHER;
            if (redirected) {
                url = connection.getHeaderField("Location");
                redirectCount++;
                LOG.warn(getTitle() + " redirected ,disconnect socket " + " # " + id);
                connection.disconnect();
            }
            if (redirectCount > MAX_REDIRECTS) {
                throw new ProxyCacheException("Too many redirects: " + redirectCount);
            }
        } while (redirected);
        return connection;
    }

    private void injectCustomHeaders(HttpURLConnection connection, String url) {
        Map<String, String> extraHeaders = headerInjector.addHeaders(url);
        for (Map.Entry<String, String> header : extraHeaders.entrySet()) {
            connection.setRequestProperty(header.getKey(), header.getValue());
        }
    }

    public synchronized String getMime() throws ProxyCacheException {
        return "video/mp4";

//        if (TextUtils.isEmpty(sourceInfo.mime)) {
//            fetchContentInfo();
//        }
//        return sourceInfo.mime;
    }

    public String getUrl() {
        return sourceInfo.url;
    }

    public String getTitle() {
        return sourceInfo.title;
    }

    @Override
    public String toString() {
        return "id=" + this.id + ",HttpUrlSource{sourceInfo='" + sourceInfo + "}";
    }
}
