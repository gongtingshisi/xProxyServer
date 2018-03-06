package com.danikula.videocache;

/**
 * Stores source's info.
 *
 * @author Alexey Danilov (danikula@gmail.com).
 * @author zhangfeng
 * @license: Apache License 2.0
 */
public class SourceInfo {
    public final String title;
    public final String url;
    public final long length;
    public final String mime;

    public SourceInfo(String title, String url, long length, String mime) {
        this.title = title;
        this.url = url;
        this.length = length;
        this.mime = mime;
    }

    @Override
    public String toString() {
        return "SourceInfo{" +
                "title='" + title + '\'' +
                ", length=" + length +
                ", mime='" + mime + '\'' +
                '}';
    }
}
