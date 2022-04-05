package io.boomerang.eventing.nats.jetstream;

import io.boomerang.eventing.nats.jetstream.exception.AlreadySubscribedException;
import io.boomerang.eventing.nats.jetstream.exception.ConsumerNotFoundException;
import io.boomerang.eventing.nats.jetstream.exception.StreamNotFoundException;

public interface SubOnlyTunnel {

  /**
   * Subscribe to receive new messages from the NATS Jetstream {@code Consumer}.
   * 
   * @param handler Subscription handler. This handler is assigned as a weak reference, thus the
   *        persistence of the handler must be managed by the code that invoked the subscription.
   * @throws AlreadySubscribedException Thrown when this communication tunnel is already subscribed
   *         to receive new messages.
   * @throws StreamNotFoundException Thrown when the provided {@code Stream} was not found on the
   *         server and {@link PubSubConfiguration#isAutomaticallyCreateStream} is set to
   *         {@code false}. Consider enabling
   *         {@link PubSubConfiguration#isAutomaticallyCreateStream} to automatically create a
   *         {@code Stream} on the NATS server.
   * @throws ConsumerNotFoundException Thrown when the provided {@code Consumer} was not found on
   *         the server and {@link PubSubConfiguration#isAutomaticallyCreateConsumer} is set to
   *         {@code false}. Consider enabling
   *         {@link PubSubConfiguration#isAutomaticallyCreateConsumer} to automatically create a
   *         {@code Stream} on the NATS server.
   * @since 0.3.0
   */
  public void subscribe(SubHandler handler)
      throws AlreadySubscribedException, StreamNotFoundException, ConsumerNotFoundException;

  /**
   * Unsubscribe from receiving new messages from the NATS Jetstream {@code Consumer}.
   * 
   * @since 0.3.0
   * @note Does nothing if this communication tunnel is not subscribed.
   */
  public void unsubscribe();

  /**
   * @return {@code true} if this communication tunnel is subscribed for receiving new messages from
   *         the NATS Jetstream {@code Consumer}, {@code false} otherwise.
   * @since 0.3.0
   * @see also For additional details on active subscription, see {@link #isSubscriptionActive()}
   *      property .
   * @note If {@code true}, this doesn't necessarily mean that the subscription is currently active,
   *       i. e. listening for new messages, but rather that the handler for subscription has been
   *       assigned. Once the subscription becomes active, the {@link #isSubscriptionActive()} will
   *       also return {@code true}.
   */
  public Boolean isSubscribed();

  /**
   * @return {@code true} if this communication tunnel is <b>subscribed and listening</b> for new
   *         messages from the NATS Jetstream {@code Consumer}, {@code false} otherwise.
   * @since 0.3.0
   * @note When subscribing to listen for new messages, the returned value will be {@code false} if
   *       the is no active connection to the NATS server. Once the connection is re-established and
   *       subscription is set up, this method will return {@code true} from that point on.
   * @note After a successful subscription, i. e. this method returns {@code true}, even if the
   *       connection is lost at any particular moment, this method will still return {@code true},
   *       since the subscription entity is build to stay active even if the connection to the NATS
   *       server is lost and then re-established.
   */
  public Boolean isSubscriptionActive();
}
