package io.boomerang.eventing.nats;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import io.boomerang.eventing.base.BaseEventingTest;
import io.nats.client.Options;

/**
 * Unit test for Connection Primer.
 */
public class ConnectionPrimerTest extends BaseEventingTest {

  @Test
  public void testConnectToServer() throws Exception {
    startNATSServer();

    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(serverUrl);
    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(() -> Objects.nonNull(connectionPrimer.getActiveConnection()));
    Boolean connected = Objects.nonNull(connectionPrimer.getActiveConnection());

    assertDoesNotThrow(() -> connectionPrimer.close());
    assertTrue(connected);
  }

  @Test
  public void testConnectBeforeServerStarted() throws Exception {
    final ConnectionPrimer connectionPrimer =
        new ConnectionPrimer(new Options.Builder().server(serverUrl).reconnectWait(POLL_DURATION));

    TimeUnit.SECONDS.sleep(WAIT_DURATION.toSeconds());

    assertTrue(Objects.isNull(connectionPrimer.getActiveConnection()));

    startNATSServer();

    Awaitility.await().atMost(WAIT_DURATION).with().pollInterval(POLL_DURATION)
        .until(() -> Objects.nonNull(connectionPrimer.getActiveConnection()));
    Boolean connected = Objects.nonNull(connectionPrimer.getActiveConnection());

    assertDoesNotThrow(() -> connectionPrimer.close());
    assertTrue(connected);
  }

  @Test
  public void testNeverConnected() throws InterruptedException {
    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(
        new Options.Builder().server(serverUrl).reconnectWait(POLL_DURATION));

    TimeUnit.SECONDS.sleep(WAIT_DURATION.toSeconds());

    assertTrue(Objects.isNull(connectionPrimer.getActiveConnection()));
    assertDoesNotThrow(() -> connectionPrimer.close());
  }

  @Test
  public void testListenerUpdates() throws Exception {
    final AtomicBoolean serverIsOnline = new AtomicBoolean(false);
    final AtomicInteger fails = new AtomicInteger();

    final ConnectionPrimer connectionPrimer = new ConnectionPrimer(
        new Options.Builder().server(serverUrl).reconnectWait(POLL_DURATION));
    final ConnectionPrimerListener listener = new ConnectionPrimerListener() {
      @Override
      public void connectionUpdated(ConnectionPrimer connectionPrimer) {
        if (serverIsOnline.get() != Objects.nonNull(connectionPrimer.getActiveConnection())) {
          fails.incrementAndGet();
        }
      }
    };
    connectionPrimer.addListener(listener);
    TimeUnit.SECONDS.sleep(WAIT_DURATION.toSeconds());
    connectionPrimer.removeListener(listener);

    startNATSServer();

    TimeUnit.SECONDS.sleep(WAIT_DURATION.toSeconds());
    serverIsOnline.set(true);
    connectionPrimer.addListener(listener);

    TimeUnit.SECONDS.sleep(WAIT_DURATION.toSeconds());

    assertEquals(0, fails.get());
    assertDoesNotThrow(() -> connectionPrimer.close());
  }
}
