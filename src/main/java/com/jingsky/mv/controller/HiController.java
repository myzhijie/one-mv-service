package com.jingsky.mv.controller;

import com.jingsky.mv.util.Response;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController()
public class HiController {
    @Value("${spring.application.name}")
    private String applicationName;

    @RequestMapping("/hi")
    public Response hi() {
        String hi="Hi! I am "+applicationName+".";
        return new Response(hi);
    }
}

