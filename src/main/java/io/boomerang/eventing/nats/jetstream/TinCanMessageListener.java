package io.boomerang.eventing.nats.jetstream;

public interface TinCanMessageListener {

  /**
   * Invoked when receiving a new message from NATS Jetstream server.
   * 
   * @param subject The subject of the message.
   * @param message The message itself.
   */
  public void newMessageReceived(String subject, String message);
}
