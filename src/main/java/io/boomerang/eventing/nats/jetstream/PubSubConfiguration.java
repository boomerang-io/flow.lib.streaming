package io.boomerang.eventing.nats.jetstream;

/**
 * The {@link PubSubConfiguration} class specifies the configuration for
 * managing a NATS Jetstream {@code Stream} and {@code Consumer} on the server.
 * Properties are set using a {@link PubSubConfiguration.Builder Builder}.
 * 
 * @since 0.2.0
 */
public class PubSubConfiguration extends PubOnlyConfiguration {

  /**
   * The NATS Jetstream {@code Consumer} will be created automatically on the
   * server if the value of
   * {@link PubOnlyConfiguration#automaticallyCreateConsumer
   * automaticallyCreateConsumer} is {@code true}.
   * 
   * @since 0.2.0
   */
  private final Boolean automaticallyCreateConsumer;

  /**
   * @since 0.2.0
   */
  public PubSubConfiguration(Boolean automaticallyCreateStream, Boolean automaticallyCreateConsumer) {
    super(automaticallyCreateStream);
    this.automaticallyCreateConsumer = automaticallyCreateConsumer;
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
  public static class Builder extends PubOnlyConfiguration.Builder {

    /**
     * The NATS Jetstream {@code Consumer} will be created automatically on the
     * server if the value of
     * {@link PubOnlyConfiguration.Builder#automaticallyCreateConsumer
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
    public Builder automaticallyCreateConsumer(Boolean automaticallyCreateConsumer) {
      this.automaticallyCreateConsumer = automaticallyCreateConsumer;
      return this;
    }

    /**
     * @since 0.2.0
     */
    public PubSubConfiguration build() {
      PubOnlyConfiguration pubOnlyConfiguration = super.build();

      // @formatter:off
      return new PubSubConfiguration(
        pubOnlyConfiguration.getAutomaticallyCreateStream(),
          automaticallyCreateConsumer
      );
      // @formatter:on
    }
  }
}
