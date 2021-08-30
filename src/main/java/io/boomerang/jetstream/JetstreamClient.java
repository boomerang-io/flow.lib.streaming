package io.boomerang.jetstream;

public interface JetstreamClient {

  /**
   * Publish a new message to NATS Jetstream server.
   * 
   * <p>
   * <b>Note:</b> When publishing a message, the framework will also create a new stream if this
   * does not exist. Stream parameters are defined by the external properties. See readme for more
   * information on properties.
   * </p>
   * 
   * @param subject The subject of the message.
   * @param message The message itself.
   * 
   * @return {@code Boolean} value indicating if the message was published successfully.
   */
  public Boolean publish(String subject, String message);

  /**
   * Subscribe to receive new messages on the provided subject.
   * 
   * <p>
   * <b>Note:</b> When subscribing to a subject, the framework will also create a new consumer of
   * the provided type if this does not exist. Consumer parameters are defined by the external
   * properties. See readme for more information on properties.
   * </p>
   * 
   * @param subject The subscription subject.
   * @param consumerType Consumer type. Can be {@link io.boomerang.jetstream.ConsumerType PushBased}
   *        or {@link io.boomerang.jetstream.ConsumerType PullBased}.
   * @param listener The {@link io.boomerang.jetstream.JetstreamMessageListener listener} for
   *        incoming messages.
   * @return {@code Boolean} value indicating if the subscription was successful.
   * @see <a href="https://docs.nats.io/jetstream/concepts">NATS Jetstream Concepts</a>
   * @see <a href="https://docs.nats.io/jetstream/concepts/consumers">About Consumers</a>
   */
  public Boolean subscribe(String subject, ConsumerType consumerType,
      JetstreamMessageListener listener);

  /**
   * Unsubscribe from receiving new messages on the provided subject.
   * 
   * @param subject The subject to unsubscribe from.
   * @return {@code Boolean} value indicating if the subscription was cancelled successfully.
   *         Returns {@code false} if there is no active subscription on the subject.
   */
  public Boolean unsubscribe(String subject);
}
