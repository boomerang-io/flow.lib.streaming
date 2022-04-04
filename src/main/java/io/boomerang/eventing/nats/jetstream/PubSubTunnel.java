package io.boomerang.eventing.nats.jetstream;

/**
 * Publish and subscribe communication tunnel interface. Can both publish and subscribe for sending
 * and receiving new messages from a NATS Jetstream {@code Stream} and {@code Consumer}.
 * 
 * @since 0.2.0
 */
public interface PubSubTunnel extends PubOnlyTunnel, SubOnlyTunnel {
}
