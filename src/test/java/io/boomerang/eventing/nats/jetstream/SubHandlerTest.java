package io.boomerang.eventing.nats.jetstream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import berlin.yuna.natsserver.config.NatsConfig;
import berlin.yuna.natsserver.logic.Nats;
import io.boomerang.eventing.nats.ConnectionPrimer;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StreamConfiguration;

/**
 * Unit test for Subscriber handler.
 */
@Execution(ExecutionMode.CONCURRENT)
public class SubHandlerTest {

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
  void testPushSubHandlerSubscriptionSucceeded() throws Exception {
    String testSubject = "test69420";

    natsServer.start();

    // Create connection and the transceiver object
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubject).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-push")
            .deliverSubject("delivery.test").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(true)
            .automaticallyCreateConsumer(true).build());

    AtomicBoolean testMatch = new AtomicBoolean(false);

    // Subscribe to a push-based consumer
    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        // Do not care about this!
      }

      @Override
      public void subscriptionSucceeded(SubOnlyTunnel tunnel) {
        testMatch.set(true);
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));

    // Wait until the subscription has received subscription succeeded notification
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(testMatch::get);

    assertTrue(testMatch.get());

    assertDoesNotThrow(() -> connectionPrimer.close());
    natsServer.stop();
  }

  @Test
  void testPushSubHandlerDelayedSubscriptionSucceeded() throws Exception {
    String testSubject = "test69420";

    // Create connection and the transceiver object
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubject).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-push")
            .deliverSubject("delivery.test").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(true)
            .automaticallyCreateConsumer(true).build());

    AtomicBoolean testMatch = new AtomicBoolean(false);

    // Subscribe to a push-based consumer
    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        // Do not care about this!
      }

      @Override
      public void subscriptionSucceeded(SubOnlyTunnel tunnel) {
        testMatch.set(true);
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));

    // Wait a few seconds and start the server
    TimeUnit.SECONDS.sleep(WAIT_DURATION.toSeconds());
    natsServer.start();

    // Wait until the subscription has received subscription succeeded notification
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(testMatch::get);

    assertTrue(testMatch.get());

    assertDoesNotThrow(() -> connectionPrimer.close());
    natsServer.stop();
  }

  @Test
  void testPushSubHandlerSubscriptionFailed() throws Exception {
    String testSubject = "test69420";

    natsServer.start();

    // Create connection and the transceiver object
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubject).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-push")
            .deliverSubject("delivery.test").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(false)
            .automaticallyCreateConsumer(false).build());

    AtomicBoolean testMatch = new AtomicBoolean(false);

    // Subscribe to a push-based consumer
    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        // Do not care about this!
      }

      @Override
      public void subscriptionFailed(SubOnlyTunnel tunnel, Exception exception) {
        testMatch.set(true);
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));

    // Wait until the subscription has received subscription succeeded notification
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(testMatch::get);

    assertTrue(testMatch.get());

    assertDoesNotThrow(() -> connectionPrimer.close());
    natsServer.stop();
  }

  @Test
  void testPushSubHandlerDelayedSubscriptionFailed() throws Exception {
    String testSubject = "test69420";

    // Create connection and the transceiver object
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubject).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-push")
            .deliverSubject("delivery.test").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(false)
            .automaticallyCreateConsumer(false).build());

    AtomicBoolean testMatch = new AtomicBoolean(false);

    // Subscribe to a push-based consumer
    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        // Do not care about this!
      }

      @Override
      public void subscriptionFailed(SubOnlyTunnel tunnel, Exception exception) {
        testMatch.set(true);
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));

    // Wait a few seconds and start the server
    TimeUnit.SECONDS.sleep(WAIT_DURATION.toSeconds());
    natsServer.start();

    // Wait until the subscription has received subscription succeeded notification
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(testMatch::get);

    assertTrue(testMatch.get());

    assertDoesNotThrow(() -> connectionPrimer.close());
    natsServer.stop();
  }

  @Test
  void testPullSubHandlerSubscriptionSucceeded() throws Exception {
    String testSubject = "test69420";

    natsServer.start();

    // Create connection and the transceiver object
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubject).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-pull").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(true)
            .automaticallyCreateConsumer(true).build());

    AtomicBoolean testMatch = new AtomicBoolean(false);

    // Subscribe to a pull-based consumer
    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        // Do not care about this!
      }

      @Override
      public void subscriptionSucceeded(SubOnlyTunnel tunnel) {
        testMatch.set(true);
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));

    // Wait until the subscription has received subscription succeeded notification
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(testMatch::get);

    assertTrue(testMatch.get());

    assertDoesNotThrow(() -> connectionPrimer.close());
    natsServer.stop();
  }

  @Test
  void testPullSubHandlerDelayedSubscriptionSucceeded() throws Exception {
    String testSubject = "test69420";

    // Create connection and the transceiver object
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubject).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-pull").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(true)
            .automaticallyCreateConsumer(true).build());

    AtomicBoolean testMatch = new AtomicBoolean(false);

    // Subscribe to a pull-based consumer
    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        // Do not care about this!
      }

      @Override
      public void subscriptionSucceeded(SubOnlyTunnel tunnel) {
        testMatch.set(true);
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));

    // Wait a few seconds and start the server
    TimeUnit.SECONDS.sleep(WAIT_DURATION.toSeconds());
    natsServer.start();

    // Wait until the subscription has received subscription succeeded notification
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(testMatch::get);

    assertTrue(testMatch.get());

    assertDoesNotThrow(() -> connectionPrimer.close());
    natsServer.stop();
  }

  @Test
  void testPullSubHandlerSubscriptionFailed() throws Exception {
    String testSubject = "test69420";

    natsServer.start();

    // Create connection and the transceiver object
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubject).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-pull").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(false)
            .automaticallyCreateConsumer(false).build());

    AtomicBoolean testMatch = new AtomicBoolean(false);

    // Subscribe to a pull-based consumer
    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        // Do not care about this!
      }

      @Override
      public void subscriptionFailed(SubOnlyTunnel tunnel, Exception exception) {
        testMatch.set(true);
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));

    // Wait until the subscription has received subscription succeeded notification
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(testMatch::get);

    assertTrue(testMatch.get());

    assertDoesNotThrow(() -> connectionPrimer.close());
    natsServer.stop();
  }

  @Test
  void testPullSubHandlerDelayedSubscriptionFailed() throws Exception {
    String testSubject = "test69420";

    // Create connection and the transceiver object
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubject).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-pull").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(false)
            .automaticallyCreateConsumer(false).build());

    AtomicBoolean testMatch = new AtomicBoolean(false);

    // Subscribe to a pull-based consumer
    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        // Do not care about this!
      }

      @Override
      public void subscriptionFailed(SubOnlyTunnel tunnel, Exception exception) {
        testMatch.set(true);
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));

    // Wait a few seconds and start the server
    TimeUnit.SECONDS.sleep(WAIT_DURATION.toSeconds());
    natsServer.start();

    // Wait until the subscription has received subscription succeeded notification
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(testMatch::get);

    assertTrue(testMatch.get());

    assertDoesNotThrow(() -> connectionPrimer.close());
    natsServer.stop();
  }
}
