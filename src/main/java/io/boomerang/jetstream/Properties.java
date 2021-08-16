package io.boomerang.jetstream;

import java.time.Duration;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import lombok.AccessLevel;
import lombok.Getter;

@Component
@Getter(AccessLevel.PACKAGE)
class Properties {

  @Value("${eventing.jetstream.server.url}")
  private String jetstreamUrl;

  @Value("${eventing.jetstream.server.reconnect-wait-time:10s}")
  private Duration serverReconnectWaitTime;

  @Value("${eventing.jetstream.server.reconnect-max-attempts:-1}")
  private Integer serverMaxReconnectAttempts;

  @Value("${eventing.jetstream.stream.name}")
  private String streamName;

  @Value("${eventing.jetstream.stream.storage-type:Memory}")
  private StorageType storageType;

  @Value("#{\"${eventing.jetstream.stream.subjects}\".split(\",\")}")
  private List<String> subjects;

  @Value("${eventing.jetstream.stream.replicas:1}")
  private Integer replicas;

  @Value("${eventing.jetstream.stream.message-max-age:30d}")
  private Duration messageMaxAge;

  @Value("${eventing.jetstream.stream.max-bytes:-1}")
  private Long maxBytes;

  @Value("${eventing.jetstream.stream.max-messages:-1}")
  private Long maxMessages;

  @Value("${eventing.jetstream.stream.max-message-size:-1}")
  private Long maxMessageSize;

  @Value("${eventing.jetstream.stream.max-consumers:-1}")
  private Long maxConsumers;

  @Value("${eventing.jetstream.stream.no-acknowledgment:false}")
  private Boolean noAcknowledgment;

  @Value("${eventing.jetstream.stream.retention-policy:Limits}")
  private RetentionPolicy retentionPolicy;

  @Value("${eventing.jetstream.stream.discard-policy:Old}")
  private DiscardPolicy discardPolicy;

  @Value("${eventing.jetstream.stream.duplicate-window:0ms}")
  private Duration duplicateWindow;
}
