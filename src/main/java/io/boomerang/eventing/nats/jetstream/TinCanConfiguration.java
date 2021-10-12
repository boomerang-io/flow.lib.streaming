package io.boomerang.eventing.nats.jetstream;

public class TinCanConfiguration {

  private final Boolean automaticallyCreateStream;

  private final Boolean automaticallyCreateConsumer;

  public TinCanConfiguration(Boolean automaticallyCreateStream, Boolean automaticallyCreateConsumer) {
    this.automaticallyCreateStream = automaticallyCreateStream;
    this.automaticallyCreateConsumer = automaticallyCreateConsumer;
  }

  public Boolean getAutomaticallyCreateStream() {
    return this.automaticallyCreateStream;
  }

  public Boolean isAutomaticallyCreateStream() {
    return this.automaticallyCreateStream;
  }

  public Boolean getAutomaticallyCreateConsumer() {
    return this.automaticallyCreateConsumer;
  }

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

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Boolean automaticallyCreateStream = true;

    private Boolean automaticallyCreateConsumer = true;

    public Builder() {
    }

    public Builder automaticallyCreateStream(Boolean automaticallyCreateStream) {
      this.automaticallyCreateStream = automaticallyCreateStream;
      return this;
    }

    public Builder automaticallyCreateConsumer(Boolean automaticallyCreateConsumer) {
      this.automaticallyCreateConsumer = automaticallyCreateConsumer;
      return this;
    }

    public TinCanConfiguration build() {

      // @formatter:off
      return new TinCanConfiguration(
          automaticallyCreateStream,
          automaticallyCreateConsumer
      );
      // @formatter:on
    }
  }
}
