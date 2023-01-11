package io.boomerang.eventing.nats.jetstream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import io.boomerang.eventing.base.BaseEventingTest;
import io.boomerang.eventing.nats.ConnectionPrimer;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StreamConfiguration;

/**
 * Unit test for Subscriber handler.
 */
@Execution(ExecutionMode.CONCURRENT)
public class SubHandlerAttributesTest extends BaseEventingTest {

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
    startNATSServer();

    // Wait until the subscription became active
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(pubSubTransceiver::isSubscriptionActive);

    assertDoesNotThrow(() -> connectionPrimer.close());
  }

  @Test
  void testPushSubHandlerSubscriptionAfterStop() throws Exception {
    String testSubject = "test69420";

    startNATSServer();

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
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(() -> pubSubTransceiver.isSubscribed() && pubSubTransceiver.isSubscriptionActive());

    // Stop the server and wait a few seconds
    natsServer.stop();
    TimeUnit.SECONDS.sleep(WAIT_DURATION.toSeconds());

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
    startNATSServer();

    // Wait until the subscription became active
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(pubSubTransceiver::isSubscriptionActive);

    assertDoesNotThrow(() -> connectionPrimer.close());
  }

  @Test
  void testPullSubHandlerSubscriptionAfterStop() throws Exception {
    String testSubject = "test69420";

    startNATSServer();

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
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(() -> pubSubTransceiver.isSubscribed() && pubSubTransceiver.isSubscriptionActive());

    // Stop the server and wait a few seconds
    natsServer.stop();
    TimeUnit.SECONDS.sleep(WAIT_DURATION.toSeconds());

    // Test that it is subscribed and has an active subscription for pull-based consumers, which
    // have the subscription always active after subscribed to the consumer for at least once
    assertTrue(pubSubTransceiver.isSubscribed());
    assertTrue(pubSubTransceiver.isSubscriptionActive());

    assertDoesNotThrow(() -> connectionPrimer.close());
  }
}
