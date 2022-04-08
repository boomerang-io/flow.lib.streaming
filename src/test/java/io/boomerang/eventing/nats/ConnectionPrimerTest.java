package io.boomerang.eventing.nats;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.Comparator;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import berlin.yuna.natsserver.config.NatsConfig;
import berlin.yuna.natsserver.logic.Nats;
import io.nats.client.Options;

/**
 * Unit test for Connection Primer.
 */
public class ConnectionPrimerTest {

  private final Duration WAIT_DURATION = Duration.ofSeconds(8);

  private final Duration POLL_DURATION = Duration.ofMillis(500);

  private final Integer SERVER_PORT = ThreadLocalRandom.current().nextInt(29170, 29998 + 1);

  private final String serverUrl =
      MessageFormat.format("nats://localhost:{0,number,#}", SERVER_PORT);

  private final String jetstreamStoreDir = System.getProperty("java.io.tmpdir") + UUID.randomUUID();

  private Nats natsServer;

  @BeforeEach
  @SuppressWarnings("resource")
  void setupNatsServer() {
    // @formatter:off
    natsServer = new Nats(SERVER_PORT)
        .config(NatsConfig.JETSTREAM, "true")
        .config(NatsConfig.STORE_DIR, jetstreamStoreDir);
    // @formatter:on
  }

  @AfterEach
  void cleanUpServer() {
    natsServer.stop();

    try (Stream<Path> walk = Files.walk(Paths.get(jetstreamStoreDir))) {
      walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    } catch (IOException e) {
      System.err
          .println("Could not delete NATS Jetstream temporary directory: " + jetstreamStoreDir);
    }
  }

  @Test
  public void testConnectToServer() throws Exception {
    natsServer.start();

    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(() -> Objects.nonNull(connectionPrimer.getActiveConnection()));
    Boolean connected = Objects.nonNull(connectionPrimer.getActiveConnection());

    assertDoesNotThrow(() -> connectionPrimer.close());
    natsServer.stop();
    assertTrue(connected);
  }

  @Test
  public void testConnectBeforeServerStarted() throws Exception {
    final ConnectionPrimer connectionPrimer =
        new ConnectionPrimer(new Options.Builder().server(serverUrl).reconnectWait(POLL_DURATION));

    TimeUnit.SECONDS.sleep(WAIT_DURATION.toSeconds());

    assertTrue(Objects.isNull(connectionPrimer.getActiveConnection()));

    natsServer.start();

    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(() -> Objects.nonNull(connectionPrimer.getActiveConnection()));
    Boolean connected = Objects.nonNull(connectionPrimer.getActiveConnection());

    assertDoesNotThrow(() -> connectionPrimer.close());
    natsServer.stop();
    assertTrue(connected);
  }

  @Test
  public void testNeverConnected() throws InterruptedException {
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(
        new Options.Builder().server(serverUrl).reconnectWait(POLL_DURATION));

    TimeUnit.SECONDS.sleep(WAIT_DURATION.toSeconds());

    assertTrue(Objects.isNull(connectionPrimer.getActiveConnection()));
    assertDoesNotThrow(() -> connectionPrimer.close());
  }

  @Test
  public void testListenerUpdates() throws Exception {
    final AtomicBoolean serverIsOnline = new AtomicBoolean(false);
    final AtomicInteger fails = new AtomicInteger();

    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(
        new Options.Builder().server(serverUrl).reconnectWait(POLL_DURATION));
    final ConnectionPrimerListener listener = new ConnectionPrimerListener() {
      @Override
      public void connectionUpdated(ConnectionPrimer connectionPrimer) {
        if (serverIsOnline.get() != Objects.nonNull(connectionPrimer.getActiveConnection())) {
          fails.incrementAndGet();
        }
      }
    };
    connectionPrimer.addListener(listener);
    TimeUnit.SECONDS.sleep(WAIT_DURATION.toSeconds());
    connectionPrimer.removeListener(listener);

    natsServer.start();

    TimeUnit.SECONDS.sleep(WAIT_DURATION.toSeconds());
    serverIsOnline.set(true);
    connectionPrimer.addListener(listener);

    TimeUnit.SECONDS.sleep(WAIT_DURATION.toSeconds());

    assertEquals(0, fails.get());
    assertDoesNotThrow(() -> connectionPrimer.close());

    natsServer.stop();
  }
}
