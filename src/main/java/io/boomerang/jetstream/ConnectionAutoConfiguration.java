package io.boomerang.jetstream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Consumer;
import io.nats.client.ErrorListener;
import io.nats.client.Nats;
import io.nats.client.Options;

@Configuration
class ConnectionAutoConfiguration {

  private static final Logger logger = LogManager.getLogger(ConnectionAutoConfiguration.class);

  @Autowired
  Properties properties;

  @Bean(destroyMethod = "close")
  public Connection natsConnection() throws Exception {

    logger.debug(
        "Initializing a new NATS connection context with URL: " + properties.getJetstreamUrl());

    // Define NATS connection and error listeners
    ConnectionListener connectionListener = new ConnectionListener() {

      @Override
      public void connectionEvent(Connection conn, Events type) {
        logger.debug("NATS connection event of type \"" + type + "\" on connection: " + conn);
      }
    };
    ErrorListener errorListener = new ErrorListener() {

      @Override
      public void errorOccurred(Connection conn, String error) {
        logger.error("A NATS error occurred \"" + error + "\" on connection: " + conn);
      }

      @Override
      public void exceptionOccurred(Connection conn, Exception exp) {
        logger.error("A NATS exception occurred on connection: " + conn, exp);
      }

      @Override
      public void slowConsumerDetected(Connection conn, Consumer consumer) {
        logger.debug("NATS detected a slow consumer [" + consumer + "] on connection: " + conn);
      }
    };

    // @formatter:off
    Options options = new Options.Builder()
        .server(properties.getJetstreamUrl())
        .connectionListener(connectionListener)
        .errorListener(errorListener)
        .reconnectWait(properties.getServerReconnectWaitTime())
        .maxReconnects(properties.getServerMaxReconnectAttempts())
        .build();
    // @formatter:on

    return Nats.connect(options);
  }
}
