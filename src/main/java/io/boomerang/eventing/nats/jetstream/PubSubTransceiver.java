package io.boomerang.eventing.nats.jetstream;

import io.boomerang.eventing.nats.ConnectionPrimer;
import io.boomerang.eventing.nats.jetstream.exception.AlreadySubscribedException;
import io.boomerang.eventing.nats.jetstream.exception.ConsumerNotFoundException;
import io.boomerang.eventing.nats.jetstream.exception.StreamNotFoundException;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StreamConfiguration;

/**
 * Publish and subscribe transceiver class is responsible for managing connection and various
 * properties for the NATS Jetstream {@code Stream} and {@code Consumer}. It can publish, as well as
 * subscribe for new messages.
 * 
 * @since 0.2.0
 * 
 * @note NATS Jetstream {@code Stream} will be automatically created if {@code PubSubConfiguration}
 *       {@link PubSubConfiguration#isAutomaticallyCreateStream isAutomaticallyCreateStream()}
 *       property is set to {@code true}. Otherwise, {@link PubSubTransceiver} will try to find the
 *       NATS Jetstream {@code Stream} by stream configuration's {@link StreamConfiguration#getName
 *       name}.
 * @note NATS Jetstream {@code Consumer} will be automatically created if
 *       {@code PubSubConfiguration} {@link PubSubConfiguration#isAutomaticallyCreateConsumer
 *       isAutomaticallyCreateConsumer()} property is set to {@code true}. Otherwise,
 *       {@link PubSubTransceiver} will try to find the NATS Jetstream {@code Consumer} by consumer
 *       configuration's {@link ConsumerConfiguration#getDurable() durable} name.
 */
public class PubSubTransceiver extends PubTransmitter implements PubSubTunnel {

  private final SubReceiver subReceiver;

  /**
   * Create a new {@link PubSubTransceiver} object with default configuration properties.
   * 
   * @param connectionPrimer Connection primer object.
   * @param streamConfiguration NATS Jetstream {@code Stream} configuration.
   * @param consumerConfiguration NATS Jetstream {@code Consumer} configuration.
   * @since 0.2.0
   */
  public PubSubTransceiver(ConnectionPrimer connectionPrimer,
      StreamConfiguration streamConfiguration, ConsumerConfiguration consumerConfiguration) {
    this(connectionPrimer, streamConfiguration, consumerConfiguration,
        new PubSubConfiguration.Builder().build());
  }

  /**
   * Create a new {@link PubSubTransceiver} object.
   * 
   * @param connectionPrimer Connection primer object.
   * @param streamConfiguration NATS Jetstream {@code Stream} configuration.
   * @param consumerConfiguration NATS Jetstream {@code Consumer} configuration.
   * @param pubSubConfiguration {@link PubSubConfiguration} object.
   * @since 0.2.0
   */
  public PubSubTransceiver(ConnectionPrimer connectionPrimer,
      StreamConfiguration streamConfiguration, ConsumerConfiguration consumerConfiguration,
      PubSubConfiguration pubSubConfiguration) {
    // @formatter:off
    super(connectionPrimer, streamConfiguration, new PubOnlyConfiguration.Builder()
        .automaticallyCreateStream(pubSubConfiguration.isAutomaticallyCreateStream())
        .build());
    // @formatter:on

    // Based on the consumer type, create the appropriate subscribe receiver
    switch (ConsumerManager.getConsumerType(consumerConfiguration)) {
      case PushBased:
        this.subReceiver = new PushBasedSubReceiver(connectionPrimer, streamConfiguration,
            consumerConfiguration, pubSubConfiguration);
        break;
      case PullBased:
      default:
        this.subReceiver = new PullBasedSubReceiver(connectionPrimer, streamConfiguration,
            consumerConfiguration, pubSubConfiguration);
        break;
    }
  }

  @Override
  public void subscribe(SubHandler handler)
      throws AlreadySubscribedException, StreamNotFoundException, ConsumerNotFoundException {
    subReceiver.subscribe(handler);
  }

  @Override
  public void unsubscribe() {
    subReceiver.unsubscribe();
  }

  @Override
  public Boolean isSubscribed() {
    return subReceiver.isSubscribed();
  }

  @Override
  public Boolean isSubscriptionActive() {
    return subReceiver.isSubscriptionActive();
  }
}
