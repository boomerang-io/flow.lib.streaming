package io.boomerang.jetstream;

public interface JetstreamMessageListener {

  /**
   * Invoked when receiving a new message from NATS Jetstream server.
   * 
   * @param subject The subject of the new message.
   * @param message The message.
   */
  public void newMessageReceived(String subject, String message);
}
