package io.boomerang.eventing.nats.jetstream;

/**
 * The {@link PubOnlyConfiguration} class specifies the configuration for
 * managing a NATS Jetstream {@code Stream} on the server. Properties are set
 * using a {@link PubOnlyConfiguration.Builder Builder}.
 * 
 * @since 0.2.0
 */
final public class PubOnlyConfiguration {

  /**
   * The NATS Jetstream {@code Stream} will be created automatically on the server
   * if the value of {@link PubOnlyConfiguration#automaticallyCreateStream
   * automaticallyCreateStream} is {@code true}.
   * 
   * @since 0.2.0
   */
  private final Boolean automaticallyCreateStream;

  /**
   * @since 0.2.0
   */
  public PubOnlyConfiguration(Boolean automaticallyCreateStream) {
    this.automaticallyCreateStream = automaticallyCreateStream;
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

  @Override
  public String toString() {
    // @formatter:off
    return "{" +
      " automaticallyCreateStream='" + isAutomaticallyCreateStream() + "'" +
      "}";
    // @formatter:on
  }

  /**
   * {@link PubOnlyConfiguration} is created using a {@link Builder}. The builder
   * supports chaining and will create a default set of options if no methods are
   * called.
   * 
   * <br/>
   * <br/>
   * 
   * {@code new PubOnlyConfiguration.Builder().build()} will create a new
   * {@link PubOnlyConfiguration} with default values.
   * 
   * @since 0.2.0
   */
  public static class Builder {

    /**
     * The NATS Jetstream {@code Stream} will be created automatically on the server
     * if the value of {@link PubOnlyConfiguration.Builder#automaticallyCreateStream
     * automaticallyCreateStream} is {@code true}.
     * 
     * @node Default value is {@code true}.
     * @since 0.2.0
     */
    private Boolean automaticallyCreateStream = true;

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
    public PubOnlyConfiguration build() {
      // @formatter:off
      return new PubOnlyConfiguration(
          automaticallyCreateStream
      );
      // @formatter:on
    }
  }
}
