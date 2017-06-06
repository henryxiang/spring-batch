package com.example.demo.config;

import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

@Configuration
public class AppConfiguration {
    @Primary
    @Bean(name ="prodDataSource")
    @ConfigurationProperties(prefix="spring.datasource")
    public DataSource dataSource() {
        return DataSourceBuilder.create().build();
    }
}
