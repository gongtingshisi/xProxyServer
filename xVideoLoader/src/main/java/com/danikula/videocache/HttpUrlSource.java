package com.danikula.videocache;

import android.content.Context;
import android.util.Log;

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

import static com.danikula.videocache.HttpProxyCacheServer.NONE;
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
 * @author zhangfeng
 * @license: Apache License 2.0
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
    public static final int TIMEOUT = 15 * 1000;
    private static final String LOCAL_HOST = "http://127.0.0.1";
    private boolean DEBUG = Log.isLoggable(getClass().getSimpleName(), Log.DEBUG);
    private IContentTypeProvider mime;

    public HttpUrlSource(Context context, String title, String url, IContentTypeProvider mime) {
        this(title, url, SourceInfoStorageFactory.newSourceInfoStorage(context), mime);
    }

    public HttpUrlSource(String title, String url, IContentTypeProvider mime) {
        this(title, url, SourceInfoStorageFactory.newEmptySourceInfoStorage(), mime);
    }

    public HttpUrlSource(String title, String url, SourceInfoStorage sourceInfoStorage, IContentTypeProvider mime) {
        this(title, url, sourceInfoStorage, new EmptyHeadersInjector(), new Md5FileNameGenerator(), mime);
    }

    public HttpUrlSource(String title, String url, SourceInfoStorage sourceInfoStorage, HeaderInjector headerInjector, IContentTypeProvider mime) {
        this(title, url, sourceInfoStorage, headerInjector, new Md5FileNameGenerator(), mime);
    }

    public HttpUrlSource(String title, String url, SourceInfoStorage sourceInfoStorage, HeaderInjector headerInjector, FileNameGenerator nameGenerator, IContentTypeProvider mime) {
        this.sourceInfoStorage = checkNotNull(sourceInfoStorage);
        this.headerInjector = checkNotNull(headerInjector);
        SourceInfo sourceInfo = sourceInfoStorage.get(url);
        this.sourceInfo = sourceInfo != null ? sourceInfo :
                new SourceInfo(title, url, NONE, mime.getMIME());
        this.id = nameGenerator.generate(url);
        this.mime = mime;
    }

    public HttpUrlSource(HttpUrlSource source) {
        this.sourceInfo = source.sourceInfo;
        this.sourceInfoStorage = source.sourceInfoStorage;
        this.headerInjector = source.headerInjector;
        this.mime = source.mime;
    }

    @Override
    public synchronized long length() throws ProxyCacheException {
        if (sourceInfo.length == NONE) {
            fetchContentInfo();
        }
        return sourceInfo.length;
    }

    @Override
    public void open(long offset) throws ProxyCacheException {
        try {
            LOG.warn("open " + getTitle() + " offset:" + offset + ", " + id);
            connection = openConnection(offset, -1);
            inputStream = new BufferedInputStream(connection.getInputStream(), DEFAULT_BUFFER_SIZE);
            this.sourceInfo = new SourceInfo(sourceInfo.title, sourceInfo.url, sourceInfo.length, getMime());
            if (!sourceInfo.url.startsWith(LOCAL_HOST)) {
                this.sourceInfoStorage.put(sourceInfo.url, sourceInfo);
            }
        } catch (IOException e) {
            LOG.warn("Error opening connection for " + sourceInfo.url + " with offset " + offset + ", " + e.getMessage() + " # " + id);
            e.printStackTrace();
        }
    }

    @Override
    public void openPartial(long offset, long size) throws ProxyCacheException {
        try {
            LOG.warn("openPartial " + getTitle() + " offset:" + offset + ",size:" + size + ", " + id);
            connection = openConnection(offset, size, -1);
            inputStream = new BufferedInputStream(connection.getInputStream(), DEFAULT_BUFFER_SIZE);
            this.sourceInfo = new SourceInfo(sourceInfo.title, sourceInfo.url, sourceInfo.length, getMime());
            if (!sourceInfo.url.startsWith(LOCAL_HOST)) {
                this.sourceInfoStorage.put(sourceInfo.url, sourceInfo);
            }
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
                throw new ProxyCacheException(message, e);
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
            urlConnection = openConnectionViaHeader(TIMEOUT);
            long length = getContentLength(urlConnection);
            this.sourceInfo = new SourceInfo(sourceInfo.title, sourceInfo.url, length, urlConnection.getContentType());
            this.sourceInfoStorage.put(sourceInfo.url, sourceInfo);
            if (DEBUG)
                LOG.warn("Source info fetched: " + sourceInfo + " # " + id);
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

    private HttpURLConnection openConnectionViaHeader(int timeout) throws IOException, ProxyCacheException {
        HttpURLConnection connection;
        boolean redirected;
        int redirectCount = 0;
        String url = this.sourceInfo.url;

        do {
            if (DEBUG)
                LOG.warn("HEAD :" + url);
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

    private HttpURLConnection openConnection(long offset, long size, int timeout) throws IOException, ProxyCacheException {
        HttpURLConnection connection;
        boolean redirected;
        int redirectCount = 0;
        String url = this.sourceInfo.url;
        do {
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

    public String getMime() {
        if (this.mime != null)
            return mime.getMIME();
        else {
            try {
                fetchContentInfo();
            } catch (ProxyCacheException e) {
                e.printStackTrace();
            }
        }
        return sourceInfo.mime;
    }

    public String getUrl() {
        return sourceInfo.url;
    }

    public String getTitle() {
        return "<" + sourceInfo.title + ">";
    }

    @Override
    public String toString() {
        return "id=" + this.id + ",HttpUrlSource{sourceInfo='" + sourceInfo + "}";
    }
}
