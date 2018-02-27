package com.danikula.videocache;

/**
 * Stores source's info.
 *
 * @author Alexey Danilov (danikula@gmail.com).
 */
public class SourceInfo {
    public final String title;
    public final String url;
    public final long length;
    public final String mime;
    public final long requestSize;

    public SourceInfo(String title, String url, long length, String mime, long requestSize) {
        this.title = title;
        this.url = url;
        this.length = length;
        this.mime = mime;
        this.requestSize = requestSize;
    }

    @Override
    public String toString() {
        return "SourceInfo{" +
                "title='" + title + '\'' +
                ", length=" + length +
                ", mime='" + mime + '\'' +
                ", requestSize=" + requestSize +
                '}';
    }
}
