package dev.vality.schedulator.config;

import dev.vality.woody.api.flow.error.WRuntimeException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Collections;

@Configuration
public class RetryConfig {

    @Value("${retry-policy.dominant.maxAttempts}")
    int maxAttempts;

    @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(
                new SimpleRetryPolicy(maxAttempts, Collections.singletonMap(WRuntimeException.class, true))
        );
        retryTemplate.setBackOffPolicy(new ExponentialBackOffPolicy());

        return retryTemplate;
    }

}
