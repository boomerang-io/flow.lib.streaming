package io.boomerang.eventing.nats.jetstream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import io.boomerang.eventing.base.BaseEventingTest;
import io.boomerang.eventing.nats.ConnectionPrimer;
import io.boomerang.eventing.nats.jetstream.exception.FailedPublishMessageException;
import io.boomerang.eventing.nats.jetstream.exception.StreamNotFoundException;
import io.boomerang.eventing.nats.jetstream.exception.SubjectMismatchException;
import io.nats.client.Options;
import io.nats.client.api.StreamConfiguration;

/**
 * Unit test for Publish Transmitter.
 */
public class PubTransmitterTest extends BaseEventingTest {

  @Test
  void testSubjectMatch() throws Exception {
    startNATSServer();

    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    PubTransmitter pubTransmitter = new PubTransmitter(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects("test").build(),
        new PubOnlyConfiguration.Builder().automaticallyCreateStream(true).build());

    assertThrows(SubjectMismatchException.class,
        () -> pubTransmitter.publish("no-match-subject", "Test message!"));
    assertDoesNotThrow(() -> pubTransmitter.publish("test", "Test message!"));

    assertDoesNotThrow(() -> connectionPrimer.close());
  }

  @Test
  void testCreateStream() throws Exception {
    startNATSServer();

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
  }

  @Test
  void testServerConnection() throws Exception {
    final ConnectionPrimer connectionPrimer =
        new ConnectionPrimer(new Options.Builder().server(serverUrl).reconnectWait(POLL_DURATION));

    PubTransmitter pubTransmitter = new PubTransmitter(connectionPrimer,
        new StreamConfiguration.Builder().name("test").subjects("test").build(),
        new PubOnlyConfiguration.Builder().automaticallyCreateStream(true).build());

    assertThrows(FailedPublishMessageException.class,
        () -> pubTransmitter.publish("test", "Test message!"));

    startNATSServer();
    TimeUnit.SECONDS.sleep(WAIT_DURATION.toSeconds());

    assertDoesNotThrow(() -> pubTransmitter.publish("test", "Test message!"));

    assertDoesNotThrow(() -> connectionPrimer.close());
  }
}
