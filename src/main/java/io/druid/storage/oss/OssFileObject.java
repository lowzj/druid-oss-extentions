package io.druid.storage.oss;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import javax.tools.FileObject;

import com.aliyun.oss.model.OSSObject;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import com.metamx.common.UOE;

/**
 * Copyright 2015, Easemob.
 * All rights reserved.
 * Author: zhangjin@easemob.com
 */
public class OssFileObject implements FileObject {
    volatile boolean streamAcquired = false;

    private final URI uri;
    private final OSSObject ossObject;

    public OssFileObject(URI uri, OSSObject ossObject) {
        this.uri = uri;
        this.ossObject = ossObject;
    }

    @Override
    public URI toUri() {
        return uri;
    }

    @Override
    public String getName() {
        final String ext = Files.getFileExtension(uri.getPath());
        return Files.getNameWithoutExtension(
                uri.getPath()) + (Strings.isNullOrEmpty(ext) ? "" : ("." + ext));
    }

    @Override
    public InputStream openInputStream() throws IOException {
        try {
            streamAcquired = true;
            return ossObject.getObjectContent();
        } catch (Exception e) {
            throw new IOException(String.format("Could not load OSS URI [%s]", uri), e);
        }
    }

    @Override
    public OutputStream openOutputStream() throws IOException {
        throw new UOE("Cannot stream OSS output");
    }

    @Override
    public Reader openReader(boolean ignoreEncodingErrors) throws IOException {
        throw new UOE("Cannot open reader");
    }

    @Override
    public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
        throw new UOE("Cannot open character sequence");
    }

    @Override
    public Writer openWriter() throws IOException {
        throw new UOE("Cannot open writer");
    }

    @Override
    public long getLastModified() {
        return ossObject.getObjectMetadata().getLastModified().getTime();
    }

    @Override
    public boolean delete() {
        throw new UOE("Cannot delete OSS items anonymously.");
    }

    @Override
    public void finalize() throws Throwable {
        try {
            if (!streamAcquired) {
                ossObject.getObjectContent().close();
            }
        } finally {
            super.finalize();
        }
    }
}
