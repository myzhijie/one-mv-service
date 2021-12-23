package com.jingsky.mv.controller;

import com.jingsky.mv.config.DatasourceConfig;
import com.jingsky.mv.config.ViewsConfig;
import com.jingsky.mv.util.DatabaseService;
import com.jingsky.mv.util.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController()
public class HiController {
    @Value("${spring.application.name}")
    private String applicationName;
    @Autowired
    DatabaseService fromDatabaseService;
    @Autowired
    private ViewsConfig viewsConfig;
    @Value("${spring.hikari.from.username}")
    private String username;

    @RequestMapping("/hi")
    public Response hi() {
        String hi="Hi! I am "+applicationName+".";
        return new Response(hi);
    }
}

