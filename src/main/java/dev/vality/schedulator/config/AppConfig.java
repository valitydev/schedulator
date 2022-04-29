package dev.vality.schedulator.config;

import dev.vality.schedulator.handler.machinegun.MgProcessorHandler;
import dev.vality.schedulator.handler.machinegun.MgProcessorMdcDecorator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class AppConfig {

    @Bean
    @Primary
    @Autowired
    public MgProcessorMdcDecorator mgProcessorHandlerDecorator(MgProcessorHandler mgProcessorHandler) {
        return new MgProcessorMdcDecorator(mgProcessorHandler);
    }

}
