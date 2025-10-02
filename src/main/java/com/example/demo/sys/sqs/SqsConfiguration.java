package com.example.demo.sys.sqs;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.net.URI;

@Slf4j
@Configuration
public class SqsConfiguration {

    @Bean
    @Profile("!local")
    public SqsClient sqsClient(
            @Value("${aws.sqs.region}") String region,
            @Value("${aws.sqs.endpoint:}") String endpoint // optional, used for local ElasticMQ
    ) {
        var builder = SqsClient.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.builder().build());

        if (endpoint != null && !endpoint.isBlank()) {
            log.info("SqsConfiguration::sqsClient - Using custom endpoint {}", endpoint);
            builder = builder.endpointOverride(URI.create(endpoint));
        } else {
            log.info("SqsConfiguration::sqsClient - Using AWS endpoint for region {}", region);
        }

        return builder.build();
    }

    // Optional: in 'local' profile, force static dummy credentials to avoid relying on machine/global config.
    @Bean
    @Profile("local")
    public StaticCredentialsProvider localStaticCredentialsProvider(
            @Value("${aws.sqs.accessKey:dummy}") String accessKey,
            @Value("${aws.sqs.secretKey:dummy}") String secretKey
    ) {
        log.info("SqsConfiguration::localStaticCredentialsProvider - Using static credentials in 'local' profile");
        return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
    }

    // If 'local' profile is active and a StaticCredentialsProvider bean exists, prefer it.
    @Bean
    @Profile("local")
    public SqsClient localSqsClient(
            @Value("${aws.sqs.region:us-east-1}") String region,
            @Value("${aws.sqs.endpoint:http://localhost:9324}") String endpoint,
            StaticCredentialsProvider staticCredentialsProvider
    ) {
        log.info("SqsConfiguration::localSqsClient - Creating SqsClient for ElasticMQ at {}", endpoint);
        return SqsClient.builder()
                .region(Region.of(region))
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(staticCredentialsProvider)
                .build();
    }
}
