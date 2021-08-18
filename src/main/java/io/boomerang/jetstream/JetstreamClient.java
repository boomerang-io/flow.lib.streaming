package io.boomerang.jetstream;

public interface JetstreamClient {

  public void publish(String subject, String message);

  public Boolean subscribe(String subject, JetstreamMessageListener listener);
}
