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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
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
 * Unit test for Publish and Subscriber Transceiver.
 */
@Execution(ExecutionMode.CONCURRENT)
public class PubSubTransceiverTest {

  private final Integer SERVER_PORT = ThreadLocalRandom.current().nextInt(29170, 29998 + 1);

  private final String serverUrl =
      MessageFormat.format("nats://localhost:{0,number,#}", SERVER_PORT);

  private final String jetstreamStoreDir = System.getProperty("java.io.tmpdir") + UUID.randomUUID();

  private Nats natsServer;

  @BeforeEach
  void setupNatsServer() {
    // @formatter:off
    natsServer = new Nats()
        .port(SERVER_PORT)
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
  void testPubSubPushConsumer() throws IOException, InterruptedException {
    List<String> testSubjects = List.of("test1", "test2", "test3", "test4", "test4");
    String testMessage = "Test message!";

    natsServer.start();

    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubjects).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-push")
            .deliverSubject("delivery.test").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(true)
            .automaticallyCreateConsumer(true).build());

    AtomicBoolean testMatch = new AtomicBoolean(false);

    assertDoesNotThrow(() -> pubSubTransceiver.publish(
        testSubjects.get(ThreadLocalRandom.current().nextInt(0, testSubjects.size())),
        testMessage));

    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(new SubHandler() {
      @Override
      public void newMessageReceived(PubSubTunnel tunnel, String subject, String message) {
        testMatch.set(testSubjects.stream().anyMatch((testSubject) -> subject.equals(testSubject))
            && testMessage.equals(message));
      }
    }));

    Awaitility.await().atMost(Duration.ofSeconds(2)).with().pollInterval(Duration.ofMillis(500))
        .until(() -> testMatch.get());

    assertTrue(testMatch.get());

    assertDoesNotThrow(() -> connectionPrimer.close());
    natsServer.stop();
  }

  @Test
  void testPubSubPullConsumer() throws IOException, InterruptedException {
    List<String> testSubjects = List.of("test1", "test2", "test3", "test4", "test4");
    String testMessage = "Test message!";

    natsServer.start();

    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubSubTransceiver pubSubTransceiver = new PubSubTransceiver(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects(testSubjects).build(),
        new ConsumerConfiguration.Builder().durable("test-consumer-pull").build(),
        new PubSubConfiguration.Builder().automaticallyCreateStream(true)
            .automaticallyCreateConsumer(true).build());

    AtomicBoolean testMatch = new AtomicBoolean(false);

    assertDoesNotThrow(() -> pubSubTransceiver.publish(
        testSubjects.get(ThreadLocalRandom.current().nextInt(0, testSubjects.size())),
        testMessage));

    assertDoesNotThrow(() -> pubSubTransceiver.subscribe(new SubHandler() {
      @Override
      public void newMessageReceived(PubSubTunnel tunnel, String subject, String message) {
        testMatch.set(testSubjects.stream().anyMatch((testSubject) -> subject.equals(testSubject))
            && testMessage.equals(message));
      }
    }));

    Awaitility.await().atMost(Duration.ofSeconds(2)).with().pollInterval(Duration.ofMillis(500))
        .until(() -> testMatch.get());

    assertTrue(testMatch.get());

    assertDoesNotThrow(() -> connectionPrimer.close());
    natsServer.stop();
  }
}
