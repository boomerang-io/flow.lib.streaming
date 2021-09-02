package io.boomerang.jetstream;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Consumer;
import io.nats.client.ErrorListener;
import io.nats.client.Nats;
import io.nats.client.Options;

class NatsDurableConnection implements DisposableBean, ConnectionListener, ErrorListener {

  private static final Logger logger = LogManager.getLogger(NatsDurableConnection.class);

  private List<String> serverUrls;

  private Duration reconnectWaitTime;

  private Integer maxReconnectAttempts;

  private Connection connection;

  NatsDurableConnection(List<String> serverUrls, Duration reconnectWaitTime,
      Integer maxReconnectAttempts) throws InterruptedException {

    // Assign the properties
    this.serverUrls = serverUrls;
    this.reconnectWaitTime = reconnectWaitTime;
    this.maxReconnectAttempts = maxReconnectAttempts;

    // Connect to NATS server
    connectAsynchronously();
  }

  void connectAsynchronously() throws InterruptedException {

    // @formatter:off
    Options options = new Options.Builder()
        .servers(this.serverUrls.toArray(String[]::new))
        .connectionListener(this)
        .errorListener(this)
        .reconnectWait(this.reconnectWaitTime)
        .maxReconnects(this.maxReconnectAttempts)
        .build();
    // @formatter:on

    // Try to connect in another thread
    Nats.connectAsynchronously(options, true);
  }

  public Optional<Connection> getOptional() {
    return Optional.ofNullable(connection);
  }

  @Override
  public void connectionEvent(Connection conn, Events type) {
    logger.debug("NATS connection event of type \"" + type + "\" on connection: " + conn);
    this.connection = conn;
  }

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

  @Override
  public void destroy() throws Exception {
    connection.close();
  }
}
