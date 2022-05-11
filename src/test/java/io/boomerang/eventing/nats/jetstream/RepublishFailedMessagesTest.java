package io.boomerang.eventing.nats.jetstream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import berlin.yuna.natsserver.config.NatsConfig;
import berlin.yuna.natsserver.logic.Nats;
import io.boomerang.eventing.nats.ConnectionPrimer;
import io.nats.client.Options;
import io.nats.client.api.StreamConfiguration;

/**
 * Unit test for Publish Transmitter with attempt to republish failed messages.
 */
public class RepublishFailedMessagesTest {

//  private final Duration WAIT_DURATION = Duration.ofSeconds(8);

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
  void testRepublishFailedMessages() throws Exception {
    final ConnectionPrimer connectionPrimer =
        new ConnectionPrimer(new Options.Builder().server(serverUrl).reconnectWait(POLL_DURATION));

    PubTransmitter pubTransmitter = new PubTransmitter(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects("test").build(),
        new PubOnlyConfiguration.Builder().automaticallyCreateStream(true).build());
    
    assertDoesNotThrow(() -> pubTransmitter.publish("test", "Test message!", true));
    
    natsServer.start();
    
    assertDoesNotThrow(() -> connectionPrimer.close());
    natsServer.stop();
  }
}
