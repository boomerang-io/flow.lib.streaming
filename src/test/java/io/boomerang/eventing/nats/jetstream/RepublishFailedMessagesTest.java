package io.boomerang.eventing.nats.jetstream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import io.boomerang.eventing.base.BaseEventingTest;
import io.boomerang.eventing.nats.ConnectionPrimer;
import io.boomerang.eventing.nats.jetstream.exception.FailedPublishMessageException;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StreamConfiguration;

/**
 * Unit test for Publish Transmitter with attempt to republish failed messages.
 */
public class RepublishFailedMessagesTest extends BaseEventingTest {

  @Test
  void testRepublishFailedMessage() throws Exception {
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects("test").build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-pull").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(true)
            .automaticallyCreateConsumer(true).build());

    AtomicBoolean testMatch = new AtomicBoolean(false);

    // Publish one message before server is started
    assertDoesNotThrow(() -> pubSubTransceiver.publish("test", "Test message!", true));

    SubHandler subHandler = new SubHandler() {
      @Override
      public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message) {
        testMatch.set("test".equals(subject) && "Test message!".equals(message));
      }
    };
    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(subHandler));
    
    TimeUnit.SECONDS.sleep(2);

    startNATSServer();

    // Check that the message has been received after starting the server
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(testMatch::get);

    assertDoesNotThrow(() -> connectionPrimer.close());
  }
  
  @Test
  void testRepublishMultipleFailedMessages() throws Exception {
    List<String> testSubjects = List.of("test1", "test2", "test3", "test4", "test4");
    List<String> testMessages =
        List.of("Test message!", "Hey", "there", "how", "are", "you? :)", "fu covid");

    // Generate random messages that will be published
    List<SimpleEntry<String, String>> messages = testMessages.stream()
        .map(message -> new SimpleEntry<>(
            testSubjects.get(ThreadLocalRandom.current().nextInt(0, testSubjects.size())), message))
        .collect(Collectors.toList());
    ArrayList<Boolean> matches = new ArrayList<>(Collections.nCopies(messages.size(), false));

    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubjects).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-pull").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(true)
            .automaticallyCreateConsumer(true).build());

    // Publish many messages before server is started
    for (SimpleEntry<String, String> message : messages) {
      assertDoesNotThrow(() -> pubSubTransceiver.publish(message.getKey(), message.getValue(), true));
    }

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
    
    TimeUnit.SECONDS.sleep(2);

    startNATSServer();

    // Check that the messages have been received after starting the server
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(() -> matches.stream().allMatch(Boolean::valueOf));

    assertDoesNotThrow(() -> connectionPrimer.close());
  }
  
  @Test
  void testRepublishFailedConnection() throws Exception {
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects("test").build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-pull").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(true)
            .automaticallyCreateConsumer(true).build());

    // Flag set to false and server offline, publish fails
    assertThrows(FailedPublishMessageException.class,
        () -> pubSubTransceiver.publish("test", "Test message!", false));

    startNATSServer();
    TimeUnit.SECONDS.sleep(2);

    assertDoesNotThrow(() -> pubSubTransceiver.publish("test", "Test message!", false));

    assertDoesNotThrow(() -> connectionPrimer.close());
  }
}
