package com.goldlapel.spring;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@ConditionalOnClass(name = "org.postgresql.Driver")
@ConditionalOnProperty(name = "goldlapel.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(GoldLapelProperties.class)
public class GoldLapelAutoConfiguration {

    @Bean
    static GoldLapelDataSourcePostProcessor goldLapelDataSourcePostProcessor(GoldLapelProperties properties) {
        return new GoldLapelDataSourcePostProcessor(properties);
    }
}
