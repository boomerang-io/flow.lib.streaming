package io.boomerang.eventing.nats.jetstream;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.boomerang.eventing.nats.ConnectionPrimer;
import io.boomerang.eventing.nats.ConnectionPrimerListener;
import io.boomerang.eventing.nats.jetstream.exception.StreamNotFoundException;
import io.boomerang.eventing.nats.jetstream.exception.SubjectMismatchException;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.NatsMessage;

/**
 * Publish-only transmitter class is responsible for managing connection and various properties for
 * the NATS Jetstream {@code Stream} and can only publish new messages to it.
 * 
 * @since 0.2.0
 * 
 * @note NATS Jetstream {@code Stream} will be automatically created if {@code PubOnlyConfiguration}
 *       {@link PubOnlyConfiguration#isAutomaticallyCreateStream isAutomaticallyCreateStream()}
 *       property is set to {@code true}. Otherwise, {@link PubTransmitter} will try to find the
 *       NATS Jetstream {@code Stream} by stream configuration's {@link StreamConfiguration#getName
 *       name}.
 */
public class PubTransmitter implements PubOnlyTunnel, ConnectionPrimerListener {

  private static final Logger logger = LogManager.getLogger(PubTransmitter.class);

  protected final ConnectionPrimer connectionPrimer;

  protected final StreamConfiguration streamConfiguration;

  private final PubOnlyConfiguration pubOnlyConfiguration;

  private ConcurrentLinkedQueue<Message> failedMessages = new ConcurrentLinkedQueue<>(); 

  /**
   * Create a new {@link PubTransmitter} object with default configuration properties.
   * 
   * @param connectionPrimer Connection primer object.
   * @param streamConfiguration NATS Jetstream {@code Stream} configuration.
   * @since 0.2.0
   */
  public PubTransmitter(ConnectionPrimer connectionPrimer,
      StreamConfiguration streamConfiguration) {
    this(connectionPrimer, streamConfiguration, new PubOnlyConfiguration.Builder().build());
  }

  /**
   * Create a new {@link PubTransmitter} object.
   * 
   * @param connectionPrimer Connection primer object.
   * @param streamConfiguration NATS Jetstream {@code Stream} configuration.
   * @param pubOnlyConfiguration {@link PubOnlyConfiguration} object.
   * @since 0.2.0
   */
  public PubTransmitter(ConnectionPrimer connectionPrimer, StreamConfiguration streamConfiguration,
      PubOnlyConfiguration pubOnlyConfiguration) {
    this.connectionPrimer = connectionPrimer;
    this.streamConfiguration = streamConfiguration;
    this.pubOnlyConfiguration = pubOnlyConfiguration;
    this.connectionPrimer.addListener(this);
  }

  // TODO remove all System.out.printlns after testing
  @Override
  public void publish(String subject, String message)
      throws SubjectMismatchException {

    // Check if the subject matches stream's wildcard subject
    Boolean subjectMatches = streamConfiguration.getSubjects().stream()
        .anyMatch(wildcard -> SubjectMatchChecker.doSubjectsMatch(subject, wildcard));
    if (Boolean.FALSE.equals(subjectMatches)) {
      throw new SubjectMismatchException(
          "Subject \"" + subject + "\" does not match any subjects of the stream!");
    }

    Connection connection = connectionPrimer.getActiveConnection();

    if (connection == null) {
      // failed bc no connection, store message
      System.out.println("Add failed message to stream");
      failedMessages.add(createNATSMessage(subject, message));
      printObject(failedMessages);
    } else {
      try {
        Boolean automaticallyCreateStream = pubOnlyConfiguration.isAutomaticallyCreateStream();
        StreamInfo streamInfo = StreamManager.getStreamInfo(connection, streamConfiguration,
            automaticallyCreateStream);
        if (streamInfo == null && Boolean.FALSE.equals(automaticallyCreateStream)) {
          throw new StreamNotFoundException("Stream could not be found! Consider enabling "
              + "`automaticallyCreateStream` in `PubOnlyConfiguration`");
        }
        PublishAck publishAck = connection.jetStream().publish(createNATSMessage(subject, message));
        System.out.println("Publish!");
        printObject(publishAck);
        logger.debug("Message published to the stream! " + publishAck);
      } catch (IOException e) {
        // failed bc no connection, store message
        System.out.println("Add failed message to stream");
        failedMessages.add(createNATSMessage(subject, message));
        printObject(failedMessages);
      } catch (JetStreamApiException e) {
        // failed bc something else, throw exception
        e.printStackTrace();
      }
    }

  }

  private NatsMessage createNATSMessage(String subject, String message) {
    return NatsMessage.builder().subject(subject).data(message, StandardCharsets.UTF_8).build();
  }

  private void publishFailedMessages() {
    // similar to initial publish logic, except:
    // store message -> break
    // throw exception -> fail silently and clear messages
    Connection connection = connectionPrimer.getActiveConnection();

    while (true) {
      synchronized (this) {
        if (connection == null || failedMessages.isEmpty()) {
          // failed bc no connection or no more messages in queue, break
          break;
        } else {
          try {
            Boolean automaticallyCreateStream = pubOnlyConfiguration.isAutomaticallyCreateStream();
            StreamInfo streamInfo = StreamManager.getStreamInfo(connection, streamConfiguration,
                automaticallyCreateStream);
            if (streamInfo == null && Boolean.FALSE.equals(automaticallyCreateStream)) {
              failedMessages.clear();
            }
            System.out.println("***********************");
            System.out.println("queue before removing message:");
            System.out.println(failedMessages);
            System.out.println("***********************");
            // publish failed message at head of queue
            PublishAck publishAck = connection.jetStream().publish(failedMessages.peek());
            System.out.println("Re publishing previously failed!");
            printObject(publishAck);
            // remove head of queue
            failedMessages.poll();
            System.out.println("***********************");
            System.out.println("queue after removing message:");
            System.out.println(failedMessages);
            System.out.println("***********************");
          } catch (IOException e) {
            // failed bc no connection, break
            break;
          } catch (JetStreamApiException e) {
            // failed bc something else, fail silently and clear messages
            failedMessages.clear();
          }
        }
      }
    }
  }

  // TODO remove this after testing, also remove dependency from pom.xml
  private void printObject(Object object) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      System.out.println(objectMapper.writeValueAsString(object));
    } catch (JsonProcessingException e) {
      // log an error
    }
  }

  @Override
  public void connectionUpdated(ConnectionPrimer connectionPrimer) {
    Connection connection = connectionPrimer.getActiveConnection();
    if (connection != null) {
      System.out.println("republishing failed messages...");
      publishFailedMessages();
    }
  }
}
