package io.boomerang.eventing.nats.jetstream;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

class StreamManager {

  private static final Logger logger = LogManager.getLogger(StreamManager.class);

  /**
   * This helper method creates and returns a new Jetstream stream.
   * 
   * @param connection          NATS server {@link io.nats.client.Connection
   *                            Connection} object.
   * @param streamConfiguration {@link io.nats.client.api.StreamConfiguration
   *                            StreamConfiguration} for creating the new Stream.
   * @throws JetStreamApiException
   * @throws IOException
   * @return A new {@link io.nats.client.api.StreamInfo StreamInfo} object.
   */
  static StreamInfo createNewStream(Connection connection, StreamConfiguration streamConfiguration)
      throws IOException, JetStreamApiException {

    logger.debug("Initializing a new Jetstream stream with configuration: " + streamConfiguration);

    return connection.jetStreamManagement().addStream(streamConfiguration);
  }

  /**
   * This helper method returns information about a Jetstream stream if this
   * exists, {@code null} otherwise.
   * 
   * @param connection          NATS server {@link io.nats.client.Connection
   *                            Connection} object.
   * @param streamConfiguration {@link io.nats.client.api.StreamConfiguration
   *                            StreamConfiguration} for Stream lookup.
   * @throws JetStreamApiException
   * @throws IOException
   * @return Stream information. See {@link io.nats.client.api.StreamInfo
   *         StreamInfo}.
   */
  static StreamInfo getStreamInfo(Connection connection, StreamConfiguration streamConfiguration)
      throws IOException, JetStreamApiException {
    return getStreamInfo(connection, streamConfiguration, false);
  }

  /**
   * This helper method returns information about a Jetstream stream if this
   * exists or has been created, {@code null} otherwise.
   * 
   * @param connection          NATS server {@link io.nats.client.Connection
   *                            Connection} object.
   * @param streamConfiguration {@link io.nats.client.api.StreamConfiguration
   *                            StreamConfiguration} for Stream lookup.
   * @param createIfMissing     Set to {@code true} to try to create a new Stream
   *                            if this can't be found on the NATS server.
   * @throws JetStreamApiException
   * @throws IOException
   * @return Stream information. See {@link io.nats.client.api.StreamInfo
   *         StreamInfo}.
   */
  static StreamInfo getStreamInfo(Connection connection, StreamConfiguration streamConfiguration,
      Boolean createIfMissing) throws IOException, JetStreamApiException {

    try {
      return connection.jetStreamManagement().getStreamInfo(streamConfiguration.getName());
    } catch (JetStreamApiException e) {

      if (e.getErrorCode() == 404) {
        return createIfMissing ? createNewStream(connection, streamConfiguration) : null;
      } else {
        throw e;
      }
    }
  }

  /**
   * This helper method returns {@code true} if a stream exists. {@code false}
   * otherwise.
   * 
   * @param connection          NATS server {@link io.nats.client.Connection
   *                            Connection} object.
   * @param streamConfiguration {@link io.nats.client.api.StreamConfiguration
   *                            StreamConfiguration} for Stream lookup.
   * @return {@link java.lang.Boolean Boolean}.
   */
  static boolean streamExists(Connection connection, StreamConfiguration streamConfiguration) {
    try {
      return getStreamInfo(connection, streamConfiguration) != null;
    } catch (Exception e) {
      logger.error("An error occurred while retrieving Jetstream stream information!", e);
      return false;
    }
  }
}
