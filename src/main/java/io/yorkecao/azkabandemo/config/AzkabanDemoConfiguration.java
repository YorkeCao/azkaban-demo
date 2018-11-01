package io.yorkecao.azkabandemo.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import javax.validation.constraints.NotBlank;

/**
 * @author Yorke
 */
@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "azkaban-demo")
public class AzkabanDemoConfiguration {
    @NotBlank private String url;
    @NotBlank private String username;
    @NotBlank private String password;

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public RestTemplate restTemplate() {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(2000);
        requestFactory.setReadTimeout(2000);
        return new RestTemplate(requestFactory);
    }
}
