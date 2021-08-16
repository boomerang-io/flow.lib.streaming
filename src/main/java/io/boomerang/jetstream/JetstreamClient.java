package io.boomerang.jetstream;

public interface JetstreamClient {

  void publish(String subject, String message);

  void consume(String subject);
}
