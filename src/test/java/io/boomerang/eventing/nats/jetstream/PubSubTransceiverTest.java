package io.boomerang.eventing.nats.jetstream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import io.boomerang.eventing.base.BaseEventingTest;
import io.boomerang.eventing.nats.ConnectionPrimer;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StreamConfiguration;

/**
 * Unit test for Publish and Subscriber Transceiver.
 */
@Execution(ExecutionMode.CONCURRENT)
public class PubSubTransceiverTest extends BaseEventingTest {

  @Test
  void testPubSubPushConsumerOneMessage() throws Exception {
    List<String> testSubjects = List.of("test1", "test2", "test3", "test4", "test4");
    String testMessage = "Test message!";
    startNATSServer();

    // Create connection and the transceiver object
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubjects).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-push")
            .deliverSubject("delivery.test").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(true)
            .automaticallyCreateConsumer(true).build());

    AtomicBoolean testMatch = new AtomicBoolean(false);

    // Publish one message
    assertDoesNotThrow(() -> pubSubTransceiver.publish(
        testSubjects.get(ThreadLocalRandom.current().nextInt(0, testSubjects.size())),
        testMessage));

    // Subscribe to a push-based consumer
    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        testMatch
            .set(testSubjects.stream().anyMatch(subject::equals) && testMessage.equals(message));
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));

    // Wait until the subscription has received the published message
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(testMatch::get);

    assertDoesNotThrow(() -> connectionPrimer.close());
  }

  @Test
  void testPubSubPullConsumerOneMessage() throws Exception {
    List<String> testSubjects = List.of("test1", "test2", "test3", "test4", "test4");
    String testMessage = "Test message!";

    startNATSServer();

    // Create connection and the transceiver object
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubjects).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-pull").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(true)
            .automaticallyCreateConsumer(true).build());

    AtomicBoolean testMatch = new AtomicBoolean(false);

    // Publish one message
    assertDoesNotThrow(() -> pubSubTransceiver.publish(
        testSubjects.get(ThreadLocalRandom.current().nextInt(0, testSubjects.size())),
        testMessage));

    // Subscribe to a pull-based consumer
    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        testMatch
            .set(testSubjects.stream().anyMatch(subject::equals) && testMessage.equals(message));
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));

    // Wait until the subscription has received the published message
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(testMatch::get);

    assertDoesNotThrow(() -> connectionPrimer.close());
  }

  @Test
  void testPubSubPushConsumerManyMessages() throws Exception {
    List<String> testSubjects = List.of("test1", "test2", "test3", "test4", "test4");
    List<String> testMessages =
        List.of("Test message!", "Hey", "there", "how", "are", "you? :)", "fu covid");

    // Generate random messages that will be published
    List<SimpleEntry<String, String>> messages = testMessages.stream()
        .map(message -> new SimpleEntry<>(
            testSubjects.get(ThreadLocalRandom.current().nextInt(0, testSubjects.size())), message))
        .collect(Collectors.toList());
    ArrayList<Boolean> matches = new ArrayList<>(Collections.nCopies(messages.size(), false));

    startNATSServer();

    // Create connection and the transceiver object
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubjects).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-push")
            .deliverSubject("delivery.test").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(true)
            .automaticallyCreateConsumer(true).build());

    // Publish many message
    for (SimpleEntry<String, String> message : messages) {
      assertDoesNotThrow(() -> pubSubTransceiver.publish(message.getKey(), message.getValue()));
    }

    // Subscribe to a push-based consumer
    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        IntStream.range(0, matches.size())
            .filter(idx -> messages.get(idx).getKey().equals(subject)
                && messages.get(idx).getValue().equals(message))
            .findFirst().ifPresent(idx -> matches.set(idx, true));
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));

    // Wait until the subscription has received all the published message
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(() -> matches.stream().allMatch(Boolean::valueOf));

    assertDoesNotThrow(() -> connectionPrimer.close());
  }

  @Test
  void testPubSubPullConsumerManyMessages() throws Exception {
    List<String> testSubjects = List.of("test1", "test2", "test3", "test4", "test4");
    List<String> testMessages =
        List.of("Test message!", "Hey", "there", "how", "are", "you? :)", "fu covid");

    // Generate random messages that will be published
    List<SimpleEntry<String, String>> messages = testMessages.stream()
        .map(message -> new SimpleEntry<>(
            testSubjects.get(ThreadLocalRandom.current().nextInt(0, testSubjects.size())), message))
        .collect(Collectors.toList());
    ArrayList<Boolean> matches = new ArrayList<>(Collections.nCopies(messages.size(), false));

    startNATSServer();

    // Create connection and the transceiver object
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubjects).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-pull").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(true)
            .automaticallyCreateConsumer(true).build());

    // Publish many message
    for (SimpleEntry<String, String> message : messages) {
      assertDoesNotThrow(() -> pubSubTransceiver.publish(message.getKey(), message.getValue()));
    }

    // Subscribe to a pull-based consumer
    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        IntStream.range(0, matches.size())
            .filter(idx -> messages.get(idx).getKey().equals(subject)
                && messages.get(idx).getValue().equals(message))
            .findFirst().ifPresent(idx -> matches.set(idx, true));
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));

    // Wait until the subscription has received all the published message
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(() -> matches.stream().allMatch(Boolean::valueOf));

    assertDoesNotThrow(() -> connectionPrimer.close());
  }

  @Test
  void testPubSubPushConsumerUnsubscribe() throws Exception {
    List<String> testSubjects = List.of("test1", "test2", "test3", "test4", "test4");
    String testMessageWhenSubscribed = "Test message while subscribed!";
    String testMessageNotSubscribed = "Test message not subscribed!";

    startNATSServer();

    // Create connection and the transceiver object
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubjects).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-push")
            .deliverSubject("delivery.test").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(true)
            .automaticallyCreateConsumer(true).build());

    AtomicInteger testMatches = new AtomicInteger(0);

    // Publish one message
    assertDoesNotThrow(() -> pubSubTransceiver.publish(
        testSubjects.get(ThreadLocalRandom.current().nextInt(0, testSubjects.size())),
        testMessageWhenSubscribed));

    // Subscribe to a push-based consumer
    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        Boolean subjectMatch = testSubjects.stream().anyMatch(subject::equals);
        Boolean messageMatch = List.of(testMessageWhenSubscribed, testMessageNotSubscribed).stream()
            .anyMatch(message::equals);

        if (subjectMatch && messageMatch) {
          testMatches.incrementAndGet();
        }
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));

    // Wait until the subscription has received the published message
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(() -> testMatches.get() == 1);

    // Unsubscribe, send another message and wait a few seconds
    pubSubTransceiver.unsubscribe();
    assertDoesNotThrow(() -> pubSubTransceiver.publish(
        testSubjects.get(ThreadLocalRandom.current().nextInt(0, testSubjects.size())),
        testMessageNotSubscribed));
    TimeUnit.SECONDS.sleep(WAIT_DURATION.toSeconds());

    // The matches count still must be one
    assertTrue(testMatches.get() == 1);

    assertDoesNotThrow(() -> connectionPrimer.close());
  }

  @Test
  void testPubSubPullConsumerUnsubscribe() throws Exception {
    List<String> testSubjects = List.of("test1", "test2", "test3", "test4", "test4");
    String testMessageWhenSubscribed = "Test message while subscribed!";
    String testMessageNotSubscribed = "Test message not subscribed!";

    startNATSServer();

    // Create connection and the transceiver object
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubjects).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-pull").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(true)
            .automaticallyCreateConsumer(true).build());

    AtomicInteger testMatches = new AtomicInteger(0);

    // Publish one message
    assertDoesNotThrow(() -> pubSubTransceiver.publish(
        testSubjects.get(ThreadLocalRandom.current().nextInt(0, testSubjects.size())),
        testMessageWhenSubscribed));

    // Subscribe to a pull-based consumer
    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        Boolean subjectMatch = testSubjects.stream().anyMatch(subject::equals);
        Boolean messageMatch = List.of(testMessageWhenSubscribed, testMessageNotSubscribed).stream()
            .anyMatch(message::equals);

        if (subjectMatch && messageMatch) {
          testMatches.incrementAndGet();
        }
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));

    // Wait until the subscription has received the published message
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(() -> testMatches.get() == 1);

    // Unsubscribe, send another message and wait a few seconds
    pubSubTransceiver.unsubscribe();
    assertDoesNotThrow(() -> pubSubTransceiver.publish(
        testSubjects.get(ThreadLocalRandom.current().nextInt(0, testSubjects.size())),
        testMessageNotSubscribed));
    TimeUnit.SECONDS.sleep(WAIT_DURATION.toSeconds());

    // The matches count still must be one
    assertTrue(testMatches.get() == 1);

    assertDoesNotThrow(() -> connectionPrimer.close());
  }
}
