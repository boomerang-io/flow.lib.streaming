package io.boomerang.eventing.nats.jetstream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
public class SubHandlerTest extends BaseEventingTest {

  @Test
  void testPushSubHandlerSubscriptionSucceeded() throws Exception {
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
    startNATSServer();

    // Wait until the subscription has received subscription succeeded notification
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(testMatch::get);

    assertTrue(testMatch.get());

    assertDoesNotThrow(() -> connectionPrimer.close());
  }

  @Test
  void testPushSubHandlerSubscriptionFailed() throws Exception {
    String testSubject = "test69420";

    startNATSServer();

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
    startNATSServer();

    // Wait until the subscription has received subscription succeeded notification
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(testMatch::get);

    assertTrue(testMatch.get());

    assertDoesNotThrow(() -> connectionPrimer.close());
  }

  @Test
  void testPullSubHandlerSubscriptionSucceeded() throws Exception {
    String testSubject = "test69420";

    startNATSServer();

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
    startNATSServer();

    // Wait until the subscription has received subscription succeeded notification
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(testMatch::get);

    assertTrue(testMatch.get());

    assertDoesNotThrow(() -> connectionPrimer.close());
  }

  @Test
  void testPullSubHandlerSubscriptionFailed() throws Exception {
    String testSubject = "test69420";

    startNATSServer();

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
    startNATSServer();

    // Wait until the subscription has received subscription succeeded notification
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(testMatch::get);

    assertTrue(testMatch.get());

    assertDoesNotThrow(() -> connectionPrimer.close());
  }
}
