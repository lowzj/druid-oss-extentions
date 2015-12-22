package io.druid.storage.oss;

import java.util.List;

import com.aliyun.oss.OSSClient;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import io.druid.guice.Binders;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.initialization.DruidModule;

/**
 * Copyright 2015, Easemob.
 * All rights reserved.
 * Author: zhangjin@easemob.com
 */
public class OssStorageDruidModule implements DruidModule {
    public static final String SCHEME = "oss";

    @Override
    public List<? extends Module> getJacksonModules() {
        return ImmutableList.of(
                new Module() {
                    @Override
                    public String getModuleName() {
                        return "DruidOssStorage-" + System.identityHashCode(this);
                    }

                    @Override
                    public Version version() {
                        return Version.unknownVersion();
                    }

                    @Override
                    public void setupModule(SetupContext context) {
                        context.registerSubtypes(OssLoadSpec.class);
                    }
                }
        );
    }

    @Override
    public void configure(Binder binder) {
        JsonConfigProvider.bind(binder, "druid.oss", OssCredentialConfig.class);
        JsonConfigProvider.bind(binder, "druid.storage", OssDataSegmentPusherConfig.class);

        Binders.dataSegmentPullerBinder(binder)
                .addBinding(SCHEME)
                .to(OssDataSegmentPuller.class)
                .in(LazySingleton.class);
        Binders.dataSegmentPusherBinder(binder)
                .addBinding(SCHEME)
                .to(OssDataSegmentPusher.class)
                .in(LazySingleton.class);
        Binders.dataSegmentKillerBinder(binder)
                .addBinding(SCHEME)
                .to(OssDataSegmentKiller.class)
                .in(LazySingleton.class);
    }

    @Provides
    @LazySingleton
    public OSSClient getOssClient(final OssCredentialConfig credential) {
        return new OSSClient(credential.getEndpoint(),
                credential.getAccessKeyId(), credential.getAccessKeySecret());
    }
}
