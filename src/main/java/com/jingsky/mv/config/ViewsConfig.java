package com.jingsky.mv.config;

import com.jingsky.mv.vo.View;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConfigurationProperties(prefix = "spring")
@Data
public class ViewsConfig {
    /**
     * 物化视图列表
     */
    private List<View> views;
}
