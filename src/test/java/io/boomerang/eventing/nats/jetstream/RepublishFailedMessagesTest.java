package io.boomerang.eventing.nats.jetstream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import berlin.yuna.natsserver.config.NatsConfig;
import berlin.yuna.natsserver.logic.Nats;
import io.boomerang.eventing.nats.ConnectionPrimer;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StreamConfiguration;

/**
 * Unit test for Publish Transmitter with attempt to republish failed messages.
 */
public class RepublishFailedMessagesTest {

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

    natsServer.start();

    // Check that the message has been received after starting the server
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(testMatch::get);

    assertDoesNotThrow(() -> connectionPrimer.close());

    natsServer.stop();
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

    natsServer.start();

    // Check that the messages have been received after starting the server
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(() -> matches.stream().allMatch(Boolean::valueOf));

    assertDoesNotThrow(() -> connectionPrimer.close());

    natsServer.stop();
  }
}
