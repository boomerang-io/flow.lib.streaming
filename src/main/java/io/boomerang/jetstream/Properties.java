package io.boomerang.jetstream;

import java.time.Duration;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.ReplayPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import lombok.AccessLevel;
import lombok.Getter;

@Component
@Getter(AccessLevel.PACKAGE)
class Properties {

  // NATS Jetstream server properties

  @Value("${eventing.jetstream.server.url}")
  private String jetstreamUrl;

  @Value("${eventing.jetstream.server.reconnect-wait-time:10s}")
  private Duration serverReconnectWaitTime;

  @Value("${eventing.jetstream.server.reconnect-max-attempts:-1}")
  private Integer serverMaxReconnectAttempts;

  // NATS Jetstream stream properties

  @Value("${eventing.jetstream.stream.name}")
  private String streamName;

  @Value("${eventing.jetstream.stream.storage-type:Memory}")
  private StorageType streamStorageType;

  @Value("#{\"${eventing.jetstream.stream.subjects}\".split(\",\")}")
  private List<String> streamSubjects;

  @Value("${eventing.jetstream.stream.replicas:1}")
  private Integer streamReplicas;

  @Value("${eventing.jetstream.stream.message-max-age:30d}")
  private Duration streamMessageMaxAge;

  @Value("${eventing.jetstream.stream.max-bytes:-1}")
  private Long streamMaxBytes;

  @Value("${eventing.jetstream.stream.max-messages:-1}")
  private Long streamMaxMessages;

  @Value("${eventing.jetstream.stream.max-message-size:-1}")
  private Long streamMaxMessageSize;

  @Value("${eventing.jetstream.stream.max-consumers:-1}")
  private Long streamMaxConsumers;

  @Value("${eventing.jetstream.stream.no-acknowledgment:false}")
  private Boolean streamNoAcknowledgment;

  @Value("${eventing.jetstream.stream.retention-policy:Limits}")
  private RetentionPolicy streamRetentionPolicy;

  @Value("${eventing.jetstream.stream.discard-policy:Old}")
  private DiscardPolicy streamDiscardPolicy;

  @Value("${eventing.jetstream.stream.duplicate-window:0ms}")
  private Duration streamDuplicateWindow;

  // NATS Jetstream consumer properties

  @Value("${eventing.jetstream.consumer.name}")
  private String consumerName;

  @Value("${eventing.jetstream.consumer.delivery-target}")
  private String consumerDeliveryTarget;

  @Value("${eventing.jetstream.consumer.deliver-policy:All}")
  private DeliverPolicy consumerDeliverPolicy;

  @Value("${eventing.jetstream.consumer.acknowledgment-policy:Explicit}")
  private AckPolicy consumerAcknowledgmentPolicy;

  @Value("${eventing.jetstream.consumer.acknowledgment-timeout:30s}")
  private Duration consumerAcknowledgmentTimeout;

  @Value("${eventing.jetstream.consumer.replay-policy:Instant}")
  private ReplayPolicy consumerReplayPolicy;

  @Value("${eventing.jetstream.consumer.filter-subject:#{null}}")
  private String consumerFilterSubject;

  @Value("${eventing.jetstream.consumer.max-acknowledgment-messages-pending:0}")
  private Long consumerMaxAcknowledgmentMessagesPending;

  @Value("${eventing.jetstream.consumer.max-deliver-retry:-1}")
  private Long consumerMaxDeliverRetry;
}
