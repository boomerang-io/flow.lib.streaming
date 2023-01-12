package io.boomerang.eventing.base;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
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
import berlin.yuna.natsserver.config.NatsConfig;
import berlin.yuna.natsserver.logic.Nats;

public class BaseEventingTest {
  protected final Duration WAIT_DURATION = Duration.ofSeconds(8);

  protected final Duration POLL_DURATION = Duration.ofMillis(500);
  private int SERVER_PORT = generateServerPort();
  private final int MAX_RETRY = 5;
  protected String serverUrl = MessageFormat.format("nats://localhost:{0,number,#}", SERVER_PORT);

  private final String jetstreamStoreDir = System.getProperty("java.io.tmpdir") + UUID.randomUUID();

  protected Nats natsServer;


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
    // Use timeout, so the port is freed
    natsServer.stop(1000);

    try (Stream<Path> walk = Files.walk(Paths.get(jetstreamStoreDir))) {
      walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    } catch (IOException e) {
      System.err
          .println("Could not delete NATS Jetstream temporary directory: " + jetstreamStoreDir);
    }
    natsServer = null;
  }

  private final int generateServerPort() {
    return ThreadLocalRandom.current().nextInt(29170, 29998 + 1);
  }

  protected final void startNATSServer() throws Exception {
    boolean started = false;
    int count = 0;
    while (!started && count < MAX_RETRY) {
      try {
        natsServer.start();
        started = true;
      } catch (BindException e) {
        System.out.println("NATS port is in use: " + SERVER_PORT);
        SERVER_PORT = generateServerPort();
        System.out.println("Retry with a different port: " + SERVER_PORT);
        serverUrl = MessageFormat.format("nats://localhost:{0,number,#}", SERVER_PORT);
        setupNatsServer();
      } catch (Exception ex) {
        System.out.println("NATS Server did not Start! " + ex);
        throw ex;
      }
      count++;
    }
  }
}
