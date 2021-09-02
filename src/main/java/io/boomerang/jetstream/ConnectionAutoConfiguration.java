package io.boomerang.jetstream;

import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

@Configuration
class ConnectionAutoConfiguration {

  @Autowired
  Properties properties;

  @Bean
  @Lazy
  public NatsDurableConnection natsConnection() throws InterruptedException {
    // @formatter:off
    return new NatsDurableConnection(
        List.of(properties.getJetstreamUrl()),
        properties.getServerReconnectWaitTime(),
        properties.getServerMaxReconnectAttempts());
    // @formatter:on
  }
}
