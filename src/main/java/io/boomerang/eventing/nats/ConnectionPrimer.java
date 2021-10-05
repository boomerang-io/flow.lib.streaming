package io.boomerang.eventing.nats;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Consumer;
import io.nats.client.ErrorListener;
import io.nats.client.Nats;
import io.nats.client.Options;

/**
 * The {@link ConnectionPrimer ConnectionPrimer} class is the main class for
 * managing the connection to the NATS server.
 * 
 * <br />
 * <br />
 * 
 * Connections, by default, are configured to try to reconnect to the server if
 * there is a network failure up to
 * {@link NatsProperties#DEFAULT_MAX_RECONNECT_ATTEMPTS times}. You can
 * configure this behavior in the {@link NatsProperties NatsProperties}.
 * 
 * <br />
 * <br />
 * 
 * The list of servers used for connecting is provided by the
 * {@link NatsProperties NatsProperties}.
 * 
 * <br />
 * <br />
 * 
 * When a connection is {@link #close() closed} the thread and socket resources
 * are cleaned up.
 * 
 * @since 0.1.0
 */
public class ConnectionPrimer implements ConnectionListener, ErrorListener, AutoCloseable {

  private static final Logger logger = LogManager.getLogger(ConnectionPrimer.class);

  private Connection connection;

  private List<Reference<ConnectionPrimerListener>> listeners = new ArrayList<>();

  /**
   * Create a new {@link ConnectionPrimer ConnectionPrimer} object with the
   * default connection options and a NATS server URL.
   * 
   * @param serverURL The URL string for the NATS server.
   * 
   * @see {@link io.nats.client.Options Options} for default connection options
   *      for the NATS server.
   */
  public ConnectionPrimer(String serverURL) {
    this(List.of(serverURL));
  }

  /**
   * Create a new {@link ConnectionPrimer ConnectionPrimer} object with the
   * default connection options and a list of known NATS server URLs.
   * 
   * @param serverURLs A list of server URL strings for NATS servers.
   * 
   * @see {@link io.nats.client.Options Options} for default connection options
   *      for the NATS server.
   */
  public ConnectionPrimer(List<String> serverURLs) {
    this(new Options.Builder().servers(serverURLs.toArray(new String[0])));
  }

  /**
   * Create a new {@link ConnectionPrimer ConnectionPrimer} object with the
   * provided connection configuration from {@link io.nats.client.Options.Builder
   * Builder}.
   * 
   * @param optionsBuilder NATS server connection options builder.
   * 
   * @note {@link io.nats.client.Options.Builder#connectionListener
   *       connectionListener} and
   *       {@link io.nats.client.Options.Builder#errorListener errorListener}
   *       handlers will be <b>overwritten</b> as the connection to the NATS
   *       server is managed automatically by {@link ConnectionPrimer
   *       ConnectionPrimer}.
   */
  public ConnectionPrimer(Options.Builder optionsBuilder) {

    // @formatter:off
    Options options = optionsBuilder
        .connectionListener(this)
        .errorListener(this)
        .build();
    // @formatter:on

    // Try to connect in another thread
    try {
      Nats.connectAsynchronously(options, true);
    } catch (InterruptedException e) {
      logger.error("An error occurred while connecting to known NATS servers!", e);
    }
  }

  @Override
  public void connectionEvent(Connection conn, Events type) {
    logger.debug("NATS connection event of type \"" + type + "\" on connection: " + conn);
    this.connection = conn;
    notifyAllListeners();
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
  public void close() throws Exception {
    if (connection != null) {
      connection.close();
    }
  }

  public Connection getConnection() {
    return this.connection;
  }

  public void addListener(ConnectionPrimerListener listener) {

    if (getListenerIdx(listener) >= 0) {
      logger.warn("Listener (" + listener + ") has been already added!");
      return;
    }

    listeners.add(new WeakReference<ConnectionPrimerListener>(listener));
    logger.debug("Successfully added listener (" + listener + ")");
  }

  public void removeListener(ConnectionPrimerListener listener) {
    int idx = getListenerIdx(listener);

    if (idx < 0) {
      logger.warn("Listener (" + listener + ") not found!");
      return;
    }

    listeners.remove(idx);
    logger.debug("Successfully removed listener (" + listener + ")");
  }

  private int getListenerIdx(ConnectionPrimerListener listener) {

    // First, clean up null references
    listeners.removeIf(reference -> reference.get() == null);

    // Find the index of the provided listener
    // @formatter:off
    return IntStream.range(0, listeners.size())
        .filter(i -> listeners.get(i).get().equals(listener))
        .findFirst()
        .orElse(-1);
    // @formatter:on
  }

  private void notifyAllListeners() {

    // @formatter:off
    listeners.stream()
        .map(Reference::get)
        .filter(Objects::nonNull)
        .forEach(listener -> listener.connectionUpdated(this));
    // @formatter:on
  }
}
