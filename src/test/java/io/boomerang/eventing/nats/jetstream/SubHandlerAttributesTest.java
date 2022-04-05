package io.boomerang.eventing.nats.jetstream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
public class SubHandlerAttributesTest {

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
  void testPushSubHandlerIsSubscribed() throws Exception {
    String testSubject = "test69420";

    // Create connection and the transceiver object
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubject).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-push")
            .deliverSubject("delivery.test").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(true)
            .automaticallyCreateConsumer(true).build());

    // Test not subscribed
    assertFalse(pubSubTransceiver.isSubscribed());

    // Subscribe to a push-based consumer
    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        // Do not care about this!
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));

    // Test subscribed
    assertTrue(pubSubTransceiver.isSubscribed());

    assertDoesNotThrow(() -> connectionPrimer.close());
  }

  @Test
  void testPushSubHandlerActiveSubscription() throws Exception {
    String testSubject = "test69420";

    // Create connection and the transceiver object
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubject).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-push")
            .deliverSubject("delivery.test").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(true)
            .automaticallyCreateConsumer(true).build());

    // Test not subscribed and no active subscription
    assertFalse(pubSubTransceiver.isSubscribed());
    assertFalse(pubSubTransceiver.isSubscriptionActive());

    // Subscribe to a push-based consumer
    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        // Do not care about this!
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));

    // Test subscribed but no active subscription
    assertTrue(pubSubTransceiver.isSubscribed());
    assertFalse(pubSubTransceiver.isSubscriptionActive());

    // Start the server
    natsServer.start();

    // Wait until the subscription became active
    Awaitility.await().atMost(Duration.ofSeconds(30)).with().pollInterval(Duration.ofMillis(500))
        .until(pubSubTransceiver::isSubscriptionActive);

    assertDoesNotThrow(() -> connectionPrimer.close());
    natsServer.stop();
  }

  @Test
  void testPushSubHandlerSubscriptionAfterStop() throws Exception {
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

    // Subscribe to a push-based consumer
    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        // Do not care about this!
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));

    // Wait until the subscription is set and active
    Awaitility.await().atMost(Duration.ofSeconds(4)).with().pollInterval(Duration.ofMillis(500))
        .until(() -> pubSubTransceiver.isSubscribed() && pubSubTransceiver.isSubscriptionActive());

    // Stop the server and wait a few seconds
    natsServer.stop();
    TimeUnit.SECONDS.sleep(2);

    // Test that it is subscribed and has an active subscription for push-based consumers, which
    // have the subscription always active after subscribed to the consumer for at least once
    assertTrue(pubSubTransceiver.isSubscribed());
    assertTrue(pubSubTransceiver.isSubscriptionActive());

    assertDoesNotThrow(() -> connectionPrimer.close());
  }

  @Test
  void testPullSubHandlerIsSubscribed() throws Exception {
    String testSubject = "test69420";

    // Create connection and the transceiver object
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubject).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-pull").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(true)
            .automaticallyCreateConsumer(true).build());

    // Test not subscribed
    assertFalse(pubSubTransceiver.isSubscribed());

    // Subscribe to a pull-based consumer
    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        // Do not care about this!
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));

    // Test subscribed
    assertTrue(pubSubTransceiver.isSubscribed());

    assertDoesNotThrow(() -> connectionPrimer.close());
  }

  @Test
  void testPullSubHandlerActiveSubscription() throws Exception {
    String testSubject = "test69420";

    // Create connection and the transceiver object
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubject).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-pull").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(true)
            .automaticallyCreateConsumer(true).build());

    // Test not subscribed and no active subscription
    assertFalse(pubSubTransceiver.isSubscribed());
    assertFalse(pubSubTransceiver.isSubscriptionActive());

    // Subscribe to a pull-based consumer
    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        // Do not care about this!
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));

    // Test subscribed but no active subscription
    assertTrue(pubSubTransceiver.isSubscribed());
    assertFalse(pubSubTransceiver.isSubscriptionActive());

    // Start the server
    natsServer.start();

    // Wait until the subscription became active
    Awaitility.await().atMost(Duration.ofSeconds(30)).with().pollInterval(Duration.ofMillis(500))
        .until(pubSubTransceiver::isSubscriptionActive);

    assertDoesNotThrow(() -> connectionPrimer.close());
    natsServer.stop();
  }

  @Test
  void testPullSubHandlerSubscriptionAfterStop() throws Exception {
    String testSubject = "test69420";

    natsServer.start();

    // Create connection and the transceiver object
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubject).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-pull").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(true)
            .automaticallyCreateConsumer(true).build());

    // Subscribe to a pull-based consumer
    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        // Do not care about this!
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));

    // Wait until the subscription is set and active
    Awaitility.await().atMost(Duration.ofSeconds(4)).with().pollInterval(Duration.ofMillis(500))
        .until(() -> pubSubTransceiver.isSubscribed() && pubSubTransceiver.isSubscriptionActive());

    // Stop the server and wait a few seconds
    natsServer.stop();
    TimeUnit.SECONDS.sleep(2);

    // Test that it is subscribed and has an active subscription for pull-based consumers, which
    // have the subscription always active after subscribed to the consumer for at least once
    assertTrue(pubSubTransceiver.isSubscribed());
    assertTrue(pubSubTransceiver.isSubscriptionActive());

    assertDoesNotThrow(() -> connectionPrimer.close());
  }
}
