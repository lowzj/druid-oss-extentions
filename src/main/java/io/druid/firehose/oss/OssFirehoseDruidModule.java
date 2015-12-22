package io.druid.firehose.oss;

import java.util.List;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;

/**
 * Copyright 2015, Easemob.
 * All rights reserved.
 * Author: zhangjin@easemob.com
 */
public class OssFirehoseDruidModule implements DruidModule {
    @Override
    public List<? extends Module> getJacksonModules() {
        return ImmutableList.of(
                new SimpleModule().registerSubtypes(
                        new NamedType(StaticOssFirehoseFactory.class, "static-oss")));

    }

    @Override
    public void configure(Binder binder) {

    }
}
