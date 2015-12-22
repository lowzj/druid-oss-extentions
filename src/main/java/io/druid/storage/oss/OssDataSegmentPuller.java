package io.druid.storage.oss;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import javax.tools.FileObject;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.OSSObject;
import com.google.common.base.Predicate;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.metamx.common.CompressionUtils;
import com.metamx.common.FileUtils;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.MapUtils;
import com.metamx.common.logger.Logger;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.segment.loading.URIDataPuller;
import io.druid.timeline.DataSegment;

/**
 * Copyright 2015, Easemob.
 * All rights reserved.
 * Author: zhangjin@easemob.com
 */
public class OssDataSegmentPuller implements DataSegmentPuller, URIDataPuller {
    private static final Logger log = new Logger(OssDataSegmentPuller.class);

    public static final String BUCKET = "bucket";
    public static final String KEY = "key";

    public static FileObject buildFileObject(final URI uri, final OSSClient ossClient) throws IOException {
        final OssCoords coords = new OssCoords(OssUtils.checkURI(uri));
        final OSSObject ossObject = ossClient.getObject(coords.bucket, coords.path);
        return new OssFileObject(uri, ossObject);
    }

    private final OSSClient ossClient;

    @Inject
    public OssDataSegmentPuller(OSSClient ossClient) {
        this.ossClient = ossClient;
    }

    @Override
    public void getSegmentFiles(DataSegment dataSegment, File file) throws SegmentLoadingException {
        getSegmentFiles(new OssCoords(dataSegment), file);
    }

    public FileUtils.FileCopyResult getSegmentFiles(OssCoords ossCoords, File outDir) throws SegmentLoadingException {
        log.info("Pulling index from OSS at path[%s] to outDir[%s]",
                OssUtils.genURI(ossCoords).toString(), outDir);
        prepareOutDir(outDir);

        try {
            final URI uri = OssUtils.genURI(ossCoords);
            final ByteSource byteSource = new ByteSource() {
                @Override
                public InputStream openStream() throws IOException {
                    return buildFileObject(uri, ossClient).openInputStream();
                }
            };
            if (CompressionUtils.isZip(ossCoords.path)) {
                final FileUtils.FileCopyResult result = CompressionUtils.unzip(
                        byteSource, outDir, true);
                log.info("Loaded %d bytes from [%s] to [%s]",
                        result.size(), ossCoords.toString(), outDir.getAbsolutePath());
                return result;
            }
            if (CompressionUtils.isGz(ossCoords.path)) {
                final String fName = Files.getNameWithoutExtension(uri.getPath());
                final File outFile = new File(outDir, fName);

                final FileUtils.FileCopyResult result = CompressionUtils.gunzip(byteSource, outFile);
                log.info("Loaded %d bytes from [%s] to [%s]",
                        result.size(), ossCoords.toString(), outFile.getAbsolutePath());
                return result;
            }
            throw new IAE("Do not know how to load file type at [%s]", uri.toString());
        } catch (Exception e) {
            try {
                org.apache.commons.io.FileUtils.deleteDirectory(outDir);
            } catch (IOException ioe) {
                log.warn(ioe,
                        "Failed to remove output directory [%s] for segment pulled from [%s]",
                        outDir.getAbsolutePath(),
                        ossCoords.toString()
                );
            }
            throw new SegmentLoadingException(e, e.getMessage());
        }
    }

    @Override
    public InputStream getInputStream(URI uri) throws IOException {
        return buildFileObject(uri, ossClient).openInputStream();
    }

    @Override
    public String getVersion(URI uri) throws IOException {
        final FileObject object = buildFileObject(uri, ossClient);
        return String.format("%d", object.getLastModified());
    }

    @Override
    public Predicate<Throwable> shouldRetryPredicate() {
        return null;
    }

    public void prepareOutDir(final File outDir) throws ISE {
        if (!outDir.exists()) {
            outDir.mkdirs();
        }

        if (!outDir.isDirectory()) {
            throw new ISE("[%s] must be a directory.", outDir);
        }
    }

    protected static class OssCoords {
        String bucket;
        String path;

        public OssCoords(URI uri) {
            if (!OssStorageDruidModule.SCHEME.equalsIgnoreCase(uri.getScheme())) {
                throw new IAE("Unsupported oss scheme: [%s]", uri.getScheme());
            }
            bucket = uri.getHost();
            String path = uri.getPath();
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
            this.path = path;
        }

        public OssCoords(DataSegment segment) {
            Map<String, Object> loadSpec = segment.getLoadSpec();
            bucket = MapUtils.getString(loadSpec, BUCKET);
            path = MapUtils.getString(loadSpec, KEY);
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
        }

        public OssCoords(String bucket, String path) {
            this.bucket = bucket;
            this.path = path;
        }

        public String toString() {
            return String.format("%s://%s/%s", OssStorageDruidModule.SCHEME, bucket, path);
        }
    }
}
