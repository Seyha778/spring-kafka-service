package com.apextech.springkafka.config;

import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableTransactionManagement
@EntityScan({"com.apextech.springkafka.entity"})
@EnableJpaRepositories({"com.apextech.springkafka.repository"})
public class PersistenceConfiguration {}
