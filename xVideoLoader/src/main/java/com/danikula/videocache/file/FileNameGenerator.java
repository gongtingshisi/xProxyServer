package com.danikula.videocache.file;

/**
 * Generator for files to be used for caching.
 *
 * @author Alexey Danilov (danikula@gmail.com).
 * @author zhangfeng
 * @license: Apache License 2.0
 */
public interface FileNameGenerator {

    String generate(String url);

}
