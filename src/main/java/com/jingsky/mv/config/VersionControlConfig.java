package com.jingsky.mv.config;

import com.netflix.appinfo.ApplicationInfoManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.Map;

@Configuration
public class VersionControlConfig {
    @Autowired
    private ApplicationInfoManager aim;
    @Value("${versionControl:0}")
    private String versionControl;

    @PostConstruct
    public void init() {
        Map<String, String> map = aim.getInfo().getMetadata();
        map.put("versionControl", versionControl);
    }
}
