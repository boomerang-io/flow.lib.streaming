package io.boomerang.eventing.nats.jetstream;

public class TinCanConfiguration {

  private final Boolean automaticallyCreateStream;

  private final Boolean automaticallyCreateConsumer;

  private final ConsumerType consumerType;

  public TinCanConfiguration(Boolean automaticallyCreateStream, Boolean automaticallyCreateConsumer,
      ConsumerType consumerType) {
    this.automaticallyCreateStream = automaticallyCreateStream;
    this.automaticallyCreateConsumer = automaticallyCreateConsumer;
    this.consumerType = consumerType;
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

  public ConsumerType getConsumerType() {
    return this.consumerType;
  }

  @Override
  public String toString() {
    // @formatter:off
    return "{" +
      " automaticallyCreateStream='" + isAutomaticallyCreateStream() + "'" +
      ", automaticallyCreateConsumer='" + isAutomaticallyCreateConsumer() + "'" +
      ", consumerType='" + getConsumerType() + "'" +
      "}";
    // @formatter:on
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Boolean automaticallyCreateStream = true;

    private Boolean automaticallyCreateConsumer = true;

    private ConsumerType consumerType = ConsumerType.PushBased;

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

    public Builder consumerType(ConsumerType consumerType) {
      this.consumerType = consumerType;
      return this;
    }

    public TinCanConfiguration build() {

      // @formatter:off
      return new TinCanConfiguration(
          automaticallyCreateStream,
          automaticallyCreateConsumer,
          consumerType
      );
      // @formatter:on
    }
  }
}
