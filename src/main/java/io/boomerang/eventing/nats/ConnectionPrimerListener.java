package io.boomerang.eventing.nats;

/**
 * @since 0.1.0
 */
public interface ConnectionPrimerListener {

  /**
   * @since 0.1.0
   */
  public void connectionUpdated(ConnectionPrimer connectionPrimer);
}
