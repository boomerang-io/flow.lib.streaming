package io.boomerang.jetstream;

public interface JetstreamMessageListener {

  public void newMessageReceived(String subject, String message);
}
