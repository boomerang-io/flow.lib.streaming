package io.boomerang.eventing.nats.jetstream.exception;

import io.boomerang.eventing.nats.jetstream.SubHandler;
import io.boomerang.eventing.nats.jetstream.SubOnlyTunnel;

/**
 * {@link SubHandlerReferenceClearedException} is used to indicate that the subscription handler
 * provided for the subscription has been cleared from memory.
 * 
 * @since 0.3.0
 * @note When subscribing with {@link SubOnlyTunnel#subscribe(SubHandler)}, the reference to the
 *       handler is stored as a weak reference, thus the persistence of the handler must be managed
 *       by the code that invoked the subscription.
 */
public class SubHandlerReferenceClearedException extends RuntimeException {

  public SubHandlerReferenceClearedException(String message) {
    super(message);
  }
}
