package com.atguigu.dwpublisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.dwpublisher.mapper")
public class DwPublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(DwPublisherApplication.class, args);
    }

}
