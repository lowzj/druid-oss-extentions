package io.druid.firehose.oss;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.aliyun.oss.OSSClient;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.metamx.common.CompressionUtils;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.impl.FileIteratingFirehose;
import io.druid.data.input.impl.StringInputRowParser;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

/**
 * Copyright 2015, Easemob.
 * All rights reserved.
 * Author: zhangjin@easemob.com
 */
public class StaticOssFirehoseFactory implements FirehoseFactory<StringInputRowParser> {
    private static final Logger log = new Logger(StaticOssFirehoseFactory.class);

    private final OSSClient ossClient;
    private final List<URI> uris;

    @JsonCreator
    public StaticOssFirehoseFactory(
            @JacksonInject("ossClient") OSSClient ossClient,
            @JsonProperty("uris") List<URI> uris) {
        this.ossClient = ossClient;
        this.uris = uris;
        for (final URI inputURI : uris) {
            Preconditions.checkArgument(inputURI.getScheme().equals("oss"),
                    "input uri scheme == oss (%s)", inputURI);
        }
    }

    @JsonProperty
    public List<URI> getUris() {
        return uris;
    }

    @Override
    public Firehose connect(final StringInputRowParser firehoseParser) throws IOException, ParseException {
        Preconditions.checkNotNull(ossClient, "null ossClient");

        final LinkedList<URI> objectQueue = Lists.newLinkedList(uris);

        return new FileIteratingFirehose(
                new Iterator<LineIterator>() {
                    @Override
                    public boolean hasNext() {
                        return !objectQueue.isEmpty();
                    }

                    @Override
                    public LineIterator next() {
                        final URI nextURI = objectQueue.poll();

                        final String ossBucket = nextURI.getAuthority();
                        final String ossKey = nextURI.getPath().startsWith("/")
                                ? nextURI.getPath().substring(1)
                                : nextURI.getPath();

                        log.info("Reading from bucket[%s] object[%s] (%s)",
                                ossBucket, ossKey, nextURI);

                        try {
                            final InputStream innerInputStream = ossClient.getObject(
                                    ossBucket, ossKey)
                                    .getObjectContent();

                            final InputStream outerInputStream = ossKey.endsWith(".gz")
                                    ? CompressionUtils.gzipInputStream(innerInputStream)
                                    : innerInputStream;

                            return IOUtils.lineIterator(new BufferedReader(
                                    new InputStreamReader(outerInputStream, Charsets.UTF_8)));
                        } catch (Exception e) {
                            log.error(e,
                                    "Exception reading from bucket[%s] object[%s]",
                                    ossBucket,
                                    ossKey);

                            throw Throwables.propagate(e);
                        }
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                },
                firehoseParser
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StaticOssFirehoseFactory factory = (StaticOssFirehoseFactory) o;

        return !(uris != null ? !uris.equals(factory.uris) : factory.uris != null);
    }

    @Override
    public int hashCode() {
        return uris != null ? uris.hashCode() : 0;
    }
}
