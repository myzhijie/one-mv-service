package com.jingsky.mv;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.jingsky.mv.*")
public class MvApplication {
    public static void main(String[] args) {
        SpringApplication.run(MvApplication.class,args);
    }
}
