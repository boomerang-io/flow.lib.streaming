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

@Component
class Properties {

  // NATS Jetstream server properties

  @Value("${eventing.jetstream.server.url}")
  private String jetstreamUrl;

  @Value("#{T(java.time.Duration).parse('${eventing.jetstream.server.reconnect-wait-time:PT10S}')}")
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

  @Value("#{T(java.time.Duration).parse('${eventing.jetstream.stream.message-max-age:P30D}')}")
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

  @Value("#{T(java.time.Duration).parse('${eventing.jetstream.stream.duplicate-window:PT0.0S}')}")
  private Duration streamDuplicateWindow;

  // NATS Jetstream push-based consumer properties

  @Value("${eventing.jetstream.consumer.push.name}")
  private String consumerPushName;

  @Value("${eventing.jetstream.consumer.push.acknowledgment-policy:None}")
  private AckPolicy consumerPushAcknowledgmentPolicy;

  @Value("#{T(java.time.Duration).parse('${eventing.jetstream.consumer.push.acknowledgment-wait:PT30S}')}")
  private Duration consumerPushAcknowledgmentWait;

  @Value("${eventing.jetstream.consumer.push.deliver-policy:All}")
  private DeliverPolicy consumerPushDeliverPolicy;

  @Value("${eventing.jetstream.consumer.push.delivery-subject}")
  private String consumerPushDeliverySubject;

  @Value("${eventing.jetstream.consumer.push.filter-subject:#{null}}")
  private String consumerPushFilterSubject;

  @Value("${eventing.jetstream.consumer.push.max-acknowledgments-pending:0}")
  private Long consumerPushMaxAcknowledgmentsPending;

  @Value("${eventing.jetstream.consumer.push.max-deliver-retry:-1}")
  private Long consumerPushMaxDeliverRetry;

  @Value("${eventing.jetstream.consumer.push.replay-policy:Instant}")
  private ReplayPolicy consumerPushReplayPolicy;

  // NATS Jetstream pull-based consumer properties

  @Value("${eventing.jetstream.consumer.pull.name}")
  private String consumerPullName;

  @Value("${eventing.jetstream.consumer.pull.acknowledgment-policy:Explicit}")
  private AckPolicy consumerPullAcknowledgmentPolicy;

  @Value("#{T(java.time.Duration).parse('${eventing.jetstream.consumer.pull.acknowledgment-wait:PT30S}')}")
  private Duration consumerPullAcknowledgmentWait;

  @Value("${eventing.jetstream.consumer.pull.deliver-policy:All}")
  private DeliverPolicy consumerPullDeliverPolicy;

  @Value("${eventing.jetstream.consumer.pull.filter-subject:#{null}}")
  private String consumerPullFilterSubject;

  @Value("${eventing.jetstream.consumer.pull.max-acknowledgments-pending:0}")
  private Long consumerPullMaxAcknowledgmentsPending;

  @Value("${eventing.jetstream.consumer.pull.max-deliver-retry:-1}")
  private Long consumerPullMaxDeliverRetry;

  @Value("${eventing.jetstream.consumer.pull.replay-policy:Instant}")
  private ReplayPolicy consumerPullReplayPolicy;

  @Value("${eventing.jetstream.consumer.pull.batch-size:10}")
  private Integer consumerPullBatchSize;

  @Value("#{T(java.time.Duration).parse('${eventing.jetstream.consumer.pull.batch-first-message-wait:PT30S}')}")
  private Duration consumerPullBatchFirstMessageWait;

  // Property getters (auto-generated)

  public String getJetstreamUrl() {
    return this.jetstreamUrl;
  }

  public Duration getServerReconnectWaitTime() {
    return this.serverReconnectWaitTime;
  }

  public Integer getServerMaxReconnectAttempts() {
    return this.serverMaxReconnectAttempts;
  }

  public String getStreamName() {
    return this.streamName;
  }

  public StorageType getStreamStorageType() {
    return this.streamStorageType;
  }

  public List<String> getStreamSubjects() {
    return this.streamSubjects;
  }

  public Integer getStreamReplicas() {
    return this.streamReplicas;
  }

  public Duration getStreamMessageMaxAge() {
    return this.streamMessageMaxAge;
  }

  public Long getStreamMaxBytes() {
    return this.streamMaxBytes;
  }

  public Long getStreamMaxMessages() {
    return this.streamMaxMessages;
  }

  public Long getStreamMaxMessageSize() {
    return this.streamMaxMessageSize;
  }

  public Long getStreamMaxConsumers() {
    return this.streamMaxConsumers;
  }

  public Boolean getStreamNoAcknowledgment() {
    return this.streamNoAcknowledgment;
  }

  public Boolean isStreamNoAcknowledgment() {
    return this.streamNoAcknowledgment;
  }

  public RetentionPolicy getStreamRetentionPolicy() {
    return this.streamRetentionPolicy;
  }

  public DiscardPolicy getStreamDiscardPolicy() {
    return this.streamDiscardPolicy;
  }

  public Duration getStreamDuplicateWindow() {
    return this.streamDuplicateWindow;
  }

  public String getConsumerPushName() {
    return this.consumerPushName;
  }

  public AckPolicy getConsumerPushAcknowledgmentPolicy() {
    return this.consumerPushAcknowledgmentPolicy;
  }

  public Duration getConsumerPushAcknowledgmentWait() {
    return this.consumerPushAcknowledgmentWait;
  }

  public DeliverPolicy getConsumerPushDeliverPolicy() {
    return this.consumerPushDeliverPolicy;
  }

  public String getConsumerPushDeliverySubject() {
    return this.consumerPushDeliverySubject;
  }

  public String getConsumerPushFilterSubject() {
    return this.consumerPushFilterSubject;
  }

  public Long getConsumerPushMaxAcknowledgmentsPending() {
    return this.consumerPushMaxAcknowledgmentsPending;
  }

  public Long getConsumerPushMaxDeliverRetry() {
    return this.consumerPushMaxDeliverRetry;
  }

  public ReplayPolicy getConsumerPushReplayPolicy() {
    return this.consumerPushReplayPolicy;
  }

  public String getConsumerPullName() {
    return this.consumerPullName;
  }

  public AckPolicy getConsumerPullAcknowledgmentPolicy() {
    return this.consumerPullAcknowledgmentPolicy;
  }

  public Duration getConsumerPullAcknowledgmentWait() {
    return this.consumerPullAcknowledgmentWait;
  }

  public DeliverPolicy getConsumerPullDeliverPolicy() {
    return this.consumerPullDeliverPolicy;
  }

  public String getConsumerPullFilterSubject() {
    return this.consumerPullFilterSubject;
  }

  public Long getConsumerPullMaxAcknowledgmentsPending() {
    return this.consumerPullMaxAcknowledgmentsPending;
  }

  public Long getConsumerPullMaxDeliverRetry() {
    return this.consumerPullMaxDeliverRetry;
  }

  public ReplayPolicy getConsumerPullReplayPolicy() {
    return this.consumerPullReplayPolicy;
  }

  public Integer getConsumerPullBatchSize() {
    return this.consumerPullBatchSize;
  }

  public Duration getConsumerPullBatchFirstMessageWait() {
    return this.consumerPullBatchFirstMessageWait;
  }
}
