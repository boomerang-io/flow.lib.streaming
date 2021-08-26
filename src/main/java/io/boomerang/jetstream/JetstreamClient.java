package io.boomerang.jetstream;

public interface JetstreamClient {

  public Boolean publish(String subject, String message);

  public Boolean subscribe(String subject, ConsumerType consumerType,
      JetstreamMessageListener listener);

  public Boolean unsubscribe(String subject);
}
