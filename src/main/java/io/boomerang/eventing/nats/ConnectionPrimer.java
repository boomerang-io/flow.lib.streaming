package io.boomerang.eventing.nats;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.nats.client.Connection;
import io.nats.client.Connection.Status;
import io.nats.client.ConnectionListener;
import io.nats.client.Consumer;
import io.nats.client.ErrorListener;
import io.nats.client.Nats;
import io.nats.client.Options;

/**
 * The {@link ConnectionPrimer ConnectionPrimer} class is the main class for managing the connection
 * to the NATS server.
 * 
 * <br />
 * <br />
 * 
 * {@link ConnectionPrimer ConnectionPrimer} is build on top of {@code jnats}
 * {@link io.nats.client.Connection Connection}, thus it shares a lot of properties and options with
 * {@code jnats} {@link io.nats.client.Connection Connection}.
 * 
 * When a connection is {@link #close() closed} the thread and socket resources are cleaned up.
 * 
 * <br />
 * <br />
 * 
 * @since 0.1.0
 */
public class ConnectionPrimer implements ConnectionListener, ErrorListener, AutoCloseable {

  private static final Logger logger = LogManager.getLogger(ConnectionPrimer.class);

  private Connection connection;

  private final Options options;

  private List<Reference<ConnectionPrimerListener>> listeners = new ArrayList<>();

  /**
   * Create a new {@link ConnectionPrimer ConnectionPrimer} object with the default connection
   * options and a NATS server URL.
   * 
   * @param serverURL The URL string for the NATS server.
   * @since 0.1.0
   * @see {@link io.nats.client.Options Options} for default connection options for the NATS server.
   */
  public ConnectionPrimer(String serverURL) {
    this(List.of(serverURL));
  }

  /**
   * Create a new {@link ConnectionPrimer ConnectionPrimer} object with the default connection
   * options and a list of known NATS server URLs.
   * 
   * @param serverURLs A list of server URL strings for NATS servers.
   * @since 0.1.0
   * @see {@link io.nats.client.Options Options} for default connection options for the NATS server.
   */
  public ConnectionPrimer(List<String> serverURLs) {
    this(new Options.Builder().servers(serverURLs.toArray(new String[0])));
  }

  /**
   * Create a new {@link ConnectionPrimer ConnectionPrimer} object with the provided connection
   * configuration from {@link io.nats.client.Options.Builder Builder}.
   * 
   * @param optionsBuilder NATS server connection options builder.
   * @since 0.1.0
   * @note It is <b>important</b> to know that
   *       {@link io.nats.client.Options.Builder#connectionListener connectionListener} and
   *       {@link io.nats.client.Options.Builder#errorListener errorListener} handlers will be
   *       <b>overwritten</b> as the connection to the NATS server is managed automatically by
   *       {@link ConnectionPrimer ConnectionPrimer}.
   */
  public ConnectionPrimer(Options.Builder optionsBuilder) {

    // @formatter:off
    options = optionsBuilder
        .connectionListener(this)
        .errorListener(this)
        .build();
    // @formatter:on

    connect();
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

  public void connect() {

    // Try to connect synchronously
    try {
      this.connection = Nats.connect(options);
    } catch (Exception e1) {

      // Synchronously connection failed for some reason, try to connect to the server
      // asynchronously
      try {
        Nats.connectAsynchronously(options, true);
      } catch (InterruptedException e2) {

        logger.error("An error occurred while connecting to known NATS servers!", e2);
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (connection != null) {
      connection.close();
    }
  }

  public Connection getActiveConnection() {

    if (this.connection != null && this.connection.getStatus() == Status.CONNECTED) {
      return this.connection;
    }
    return null;
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

    // Then find the index of the provided listener
    // @formatter:off
    return IntStream.range(0, listeners.size())
        .filter(i -> listeners.get(i).get().equals(listener))
        .findFirst()
        .orElse(-1);
    // @formatter:on
  }

  private void notifyAllListeners() {

    // First, clean up null references
    listeners.removeIf(reference -> reference.get() == null);

    // Then notify existing listeners
    // @formatter:off
    listeners.stream()
        .map(Reference::get)
        .forEach(listener -> listener.connectionUpdated(this));
    // @formatter:on
  }
}
