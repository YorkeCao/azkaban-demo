package io.yorkecao.azkabandemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@EnableAspectJAutoProxy(exposeProxy = true, proxyTargetClass = true)
@SpringBootApplication
public class AzkabanDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(AzkabanDemoApplication.class, args);
    }
}
