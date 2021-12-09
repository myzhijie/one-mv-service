package com.jingsky.mv.controller;

import com.jingsky.mv.util.Response;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController()
public class HiController {
    @Value("${spring.application.name}")
    private String applicationName;
    @Autowired
    private HikariDataSource fromDataSource;
    @Autowired
    private HikariDataSource toDataSource;

    @RequestMapping("/hi")
    public Response hi() {
        String hi="Hi! I am "+applicationName+".";
        return new Response(hi);
    }
}

