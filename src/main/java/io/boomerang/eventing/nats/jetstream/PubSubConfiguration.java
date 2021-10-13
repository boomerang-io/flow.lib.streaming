package io.boomerang.eventing.nats.jetstream;

/**
 * The {@link PubSubConfiguration} class specifies the configuration for
 * managing a NATS Jetstream {@code Stream} and {@code Consumer} on the server.
 * Properties are set using a {@link PubSubConfiguration.Builder Builder}.
 * 
 * @since 0.2.0
 */
final public class PubSubConfiguration {

  /**
   * The NATS Jetstream {@code Stream} will be created automatically on the server
   * if the value of {@link PubSubConfiguration#automaticallyCreateStream
   * automaticallyCreateStream} is {@code true}.
   * 
   * @since 0.2.0
   */
  private final Boolean automaticallyCreateStream;

  /**
   * The NATS Jetstream {@code Consumer} will be created automatically on the
   * server if the value of {@link PubSubConfiguration#automaticallyCreateConsumer
   * automaticallyCreateConsumer} is {@code true}.
   * 
   * @since 0.2.0
   */
  private final Boolean automaticallyCreateConsumer;

  /**
   * @since 0.2.0
   */
  public PubSubConfiguration(Boolean automaticallyCreateStream, Boolean automaticallyCreateConsumer) {
    this.automaticallyCreateStream = automaticallyCreateStream;
    this.automaticallyCreateConsumer = automaticallyCreateConsumer;
  }

  /**
   * @since 0.2.0
   */
  public Boolean getAutomaticallyCreateStream() {
    return this.automaticallyCreateStream;
  }

  /**
   * @since 0.2.0
   */
  public Boolean isAutomaticallyCreateStream() {
    return this.automaticallyCreateStream;
  }

  /**
   * @since 0.2.0
   */
  public Boolean getAutomaticallyCreateConsumer() {
    return this.automaticallyCreateConsumer;
  }

  /**
   * @since 0.2.0
   */
  public Boolean isAutomaticallyCreateConsumer() {
    return this.automaticallyCreateConsumer;
  }

  @Override
  public String toString() {
    // @formatter:off
    return "{" +
      " automaticallyCreateStream='" + isAutomaticallyCreateStream() + "'" +
      ", automaticallyCreateConsumer='" + isAutomaticallyCreateConsumer() + "'" +
      "}";
    // @formatter:on
  }

  /**
   * {@link PubSubConfiguration} is created using a {@link Builder}. The builder
   * supports chaining and will create a default set of options if no methods are
   * called.
   * 
   * <br/>
   * <br/>
   * 
   * {@code new PubSubConfiguration.Builder().build()} will create a new
   * {@link PubSubConfiguration} with default values.
   * 
   * @since 0.2.0
   */
  public static class Builder {

    /**
     * The NATS Jetstream {@code Stream} will be created automatically on the server
     * if the value of {@link PubSubConfiguration.Builder#automaticallyCreateStream
     * automaticallyCreateStream} is {@code true}.
     * 
     * @node Default value is {@code true}.
     * @since 0.2.0
     */
    private Boolean automaticallyCreateStream = true;

    /**
     * The NATS Jetstream {@code Consumer} will be created automatically on the
     * server if the value of
     * {@link PubSubConfiguration.Builder#automaticallyCreateConsumer
     * automaticallyCreateConsumer} is {@code true}.
     * 
     * @node Default value is {@code true}.
     * @since 0.2.0
     */
    private Boolean automaticallyCreateConsumer = true;

    /**
     * @since 0.2.0
     */
    public Builder() {
    }

    /**
     * @since 0.2.0
     */
    public Builder automaticallyCreateStream(Boolean automaticallyCreateStream) {
      this.automaticallyCreateStream = automaticallyCreateStream;
      return this;
    }

    /**
     * @since 0.2.0
     */
    public Builder automaticallyCreateConsumer(Boolean automaticallyCreateConsumer) {
      this.automaticallyCreateConsumer = automaticallyCreateConsumer;
      return this;
    }

    /**
     * @since 0.2.0
     */
    public PubSubConfiguration build() {
      // @formatter:off
      return new PubSubConfiguration(
          automaticallyCreateStream,
          automaticallyCreateConsumer
      );
      // @formatter:on
    }
  }
}
