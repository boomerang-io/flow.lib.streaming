package io.boomerang.eventing.nats.jetstream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import berlin.yuna.natsserver.config.NatsConfig;
import berlin.yuna.natsserver.logic.Nats;
import io.boomerang.eventing.nats.ConnectionPrimer;
import io.boomerang.eventing.nats.jetstream.exception.NoNatsConnectionException;
import io.boomerang.eventing.nats.jetstream.exception.StreamNotFoundException;
import io.boomerang.eventing.nats.jetstream.exception.SubjectMismatchException;
import io.nats.client.Options;
import io.nats.client.api.StreamConfiguration;

/**
 * Unit test for Publish Transmitter.
 */
public class PubTransmitterTest {

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
  void testSubjectMatch() throws Exception {
    natsServer.start();

    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubTransmitter pubTransmitter = new PubTransmitter(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects("test").build(),
        new PubOnlyConfiguration.Builder().automaticallyCreateStream(true).build());

    assertThrows(SubjectMismatchException.class,
        () -> pubTransmitter.publish("no-match-subject", "Test message!"));
    assertDoesNotThrow(() -> pubTransmitter.publish("test", "Test message!"));

    assertDoesNotThrow(() -> connectionPrimer.close());
    natsServer.stop();
  }

  @Test
  void testCreateStream() throws Exception {
    natsServer.start();

    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    StreamConfiguration streamConfiguration =
        new StreamConfiguration.Builder().name("test").subjects("test").build();
    PubOnlyConfiguration pubOnlyConfigurationNoAutoCreate =
        new PubOnlyConfiguration.Builder().automaticallyCreateStream(false).build();
    PubOnlyConfiguration pubOnlyConfigurationWithAutoCreate =
        new PubOnlyConfiguration.Builder().automaticallyCreateStream(true).build();
    PubTransmitter pubTransmitter1 =
        new PubTransmitter(connectionPrimer, streamConfiguration, pubOnlyConfigurationNoAutoCreate);
    PubTransmitter pubTransmitter2 = new PubTransmitter(connectionPrimer, streamConfiguration,
        pubOnlyConfigurationWithAutoCreate);

    assertThrows(StreamNotFoundException.class,
        () -> pubTransmitter1.publish("test", "Test message!"));
    assertDoesNotThrow(() -> pubTransmitter2.publish("test", "Test message!"));

    assertDoesNotThrow(() -> connectionPrimer.close());
    natsServer.stop();
  }

  @Test
  void testServerConnection() throws Exception {
    final ConnectionPrimer connectionPrimer =
        new ConnectionPrimer(new Options.Builder().server(serverUrl).reconnectWait(POLL_DURATION));

    PubTransmitter pubTransmitter = new PubTransmitter(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects("test").build(),
        new PubOnlyConfiguration.Builder().automaticallyCreateStream(true).build());

    assertThrows(NoNatsConnectionException.class,
        () -> pubTransmitter.publish("test", "Test message!"));

    natsServer.start();
    TimeUnit.SECONDS.sleep(WAIT_DURATION.toSeconds());

    assertDoesNotThrow(() -> pubTransmitter.publish("test", "Test message!"));

    assertDoesNotThrow(() -> connectionPrimer.close());
    natsServer.stop();
  }
}
