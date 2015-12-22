package io.druid.storage.oss;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.CannedAccessControlList;
import com.aliyun.oss.model.ObjectMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.metamx.common.CompressionUtils;
import com.metamx.common.logger.Logger;
import io.druid.segment.SegmentUtils;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.timeline.DataSegment;

/**
 * Copyright 2015, Easemob.
 * All rights reserved.
 * Author: zhangjin@easemob.com
 */
public class OssDataSegmentPusher implements DataSegmentPusher {
    private static final Logger log = new Logger(OssDataSegmentPusher.class);

    private final OSSClient ossClient;
    private final OssDataSegmentPusherConfig config;
    private final ObjectMapper jsonMapper;

    @Inject
    public OssDataSegmentPusher(OSSClient ossClient,
            OssDataSegmentPusherConfig config,
            ObjectMapper jsonMapper) {
        this.ossClient = ossClient;
        this.config = config;
        this.jsonMapper = jsonMapper;
        log.info("configured oss as deep storage");
    }


    @Override
    public String getPathForHadoop(String dataSource) {
        return String.format("%s://%s/%s/%s", OssStorageDruidModule.SCHEME,
                config.getBucket(), config.getBaseKey(), dataSource);
    }

    @Override
    public DataSegment push(File indexFilesDir, DataSegment inSegment) throws IOException {
        final String ossPath = OssUtils.genSegmentPath(config.getBaseKey(), inSegment);

        log.info("Copying segment[%s] to OSS at location[%s]", inSegment.getIdentifier(), ossPath);

        final File zipOutFile = File.createTempFile("druid", "index.zip");
        final long indexSize = CompressionUtils.zip(indexFilesDir, zipOutFile);

        try {
            InputStream content = new FileInputStream(zipOutFile);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(zipOutFile.length());

            final String outputBucket = config.getBucket();
            final String ossDescriptorPath = OssUtils.descriptorPathForSegmentPath(ossPath);

            metadata.setObjectAcl(CannedAccessControlList.Default);
            if (!config.getDisableAcl()) {
                metadata.setObjectAcl(CannedAccessControlList.PublicReadWrite);
            }

            log.info("Pushing %s.", metadata);
            ossClient.putObject(outputBucket, ossPath, content, metadata);

            final DataSegment outSegment = inSegment.withSize(indexSize)
                    .withLoadSpec(
                            ImmutableMap.<String, Object>of(
                                    "type",
                                    "oss_zip",
                                    "bucket",
                                    outputBucket,
                                    "key",
                                    ossPath
                            )
                    )
                    .withBinaryVersion(SegmentUtils.getVersionFromDir(indexFilesDir));

            File descriptorFile = File.createTempFile("druid", "descriptor.json");
            InputStream descriptorContent = new FileInputStream(descriptorFile);
            Files.copy(ByteStreams.newInputStreamSupplier(jsonMapper.writeValueAsBytes(inSegment)), descriptorFile);
            ObjectMetadata descriptorObject = new ObjectMetadata();
            if (!config.getDisableAcl()) {
                descriptorObject.setObjectAcl(CannedAccessControlList.PublicReadWrite);
            }

            log.info("Pushing %s", descriptorObject);
            ossClient.putObject(outputBucket, ossDescriptorPath, descriptorContent, descriptorObject);

            log.info("Deleting zipped index File[%s]", zipOutFile);
            zipOutFile.delete();

            log.info("Deleting descriptor file[%s]", descriptorFile);
            descriptorFile.delete();

            return outSegment;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
