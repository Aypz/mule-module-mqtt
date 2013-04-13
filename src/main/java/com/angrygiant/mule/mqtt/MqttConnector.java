/*
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.md file.
 * 
 */

package com.angrygiant.mule.mqtt;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttDefaultFilePersistence;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.mule.api.ConnectionException;
import org.mule.api.ConnectionExceptionCode;
import org.mule.api.MuleContext;
import org.mule.api.MuleEvent;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.Connect;
import org.mule.api.annotations.ConnectionIdentifier;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.Disconnect;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.Source;
import org.mule.api.annotations.ValidateConnection;
import org.mule.api.annotations.display.Password;
import org.mule.api.annotations.param.ConnectionKey;
import org.mule.api.annotations.param.Default;
import org.mule.api.annotations.param.Optional;
import org.mule.api.annotations.param.Payload;
import org.mule.api.callback.SourceCallback;
import org.mule.api.context.MuleContextAware;
import org.mule.util.StringUtils;

import com.angrygiant.mule.mqtt.holders.MqttTopicSubscriptionExpressionHolder;

/**
 * Mule MQTT Module
 * <p/>
 * {@sample.config ../../../doc/mqtt-connector.xml.sample mqtt:config-1}
 * <p/>
 * {@sample.config ../../../doc/mqtt-connector.xml.sample mqtt:config-2}
 * <p/>
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 * <p/>
 * <p>
 * The software in this package is published under the terms of the CPAL v1.0 license, a copy of
 * which has been included with this distribution in the LICENSE.md file.
 * </p>
 * <p>
 * Created with IntelliJ IDEA. User: dmiller@angrygiant.com Date: 9/21/12 Time: 9:57 AM
 * </p>
 * <p>
 * Module definition for publishing and subscribing to a given MQTT broker server.
 * </p>
 * 
 * @author dmiller@angrygiant.com
 */
@Connector(name = "mqtt", schemaVersion = "1.0", friendlyName = "MQTT", minMuleVersion = "3.3.0", description = "MQTT Module")
public class MqttConnector implements MuleContextAware
{
    public static enum DeliveryQoS
    {
        FIRE_AND_FORGET(0), AT_LEAST_ONCE(1), ONLY_ONCE(2);

        private final int code;

        private DeliveryQoS(final int code)
        {
            this.code = code;
        }

        public int getCode()
        {
            return code;
        }

        public static DeliveryQoS fromCode(final int code)
        {
            for (final DeliveryQoS qos : values())
            {
                if (qos.getCode() == code)
                {
                    return qos;
                }
            }
            throw new IllegalArgumentException(code + " is not a valid QoS value");
        }
    }

    private static final Log LOGGER = LogFactory.getLog(MqttConnector.class);

    public static final String MQTT_PROPERTIES_PREFIX = "mqtt";
    public static final String MQTT_TOPIC_NAME_PROPERTY = MQTT_PROPERTIES_PREFIX + ".topicName";
    public static final String MQTT_QOS_PROPERTY = MQTT_PROPERTIES_PREFIX + ".qos";
    public static final String MQTT_DELIVERY_TOKEN_VARIABLE = MQTT_PROPERTIES_PREFIX + ".deliveryToken";

    public static final String MQTT_DEFAULT_BROKER_URI = "tcp://localhost:1883";
    private static final String MQTT_DEFAULT_QOS_STRING = "AT_LEAST_ONCE";
    public static final DeliveryQoS MQTT_DEFAULT_QOS = DeliveryQoS.valueOf(MQTT_DEFAULT_QOS_STRING);

    /**
     * MQTT broker server URI.
     */
    @Configurable
    @Optional
    @Default(MQTT_DEFAULT_BROKER_URI)
    private String brokerServerUri;

    /**
     * Clean Session.
     */
    @Configurable
    @Optional
    @Default("true")
    private boolean cleanSession;

    /**
     * Username to log into broker with.
     */
    @Configurable
    @Optional
    private String username;

    /**
     * Password to log into broker with.
     */
    @Configurable
    @Optional
    @Password
    private String password;

    /**
     * Connection Timeout.
     */
    @Configurable
    @Optional
    @Default("30")
    private int connectionTimeout = 30;

    /**
     * Last Will and Testimate Topic
     */
    @Configurable
    @Optional
    private String lwtTopicName;

    /**
     * Last Will and Testimate message.
     */
    @Configurable
    @Optional
    private String lwtMessage;

    /**
     * Last Will and Testimate QOS.
     */
    @Configurable
    @Optional
    @Default("2")
    private int lwtQos;

    /**
     * Last Will and Testimate retention.
     */
    @Configurable
    @Optional
    @Default("false")
    private boolean lwtRetained;

    /**
     * Keep-alive interval.
     */
    @Configurable
    @Optional
    @Default("60")
    private int keepAliveInterval = 60;

    /**
     * Directory on the machine where message persistence can be stored to disk.
     */
    @Configurable
    @Optional
    private String persistenceLocation;

    private MuleContext muleContext;
    private String clientId;
    private MqttClient client;
    private MqttConnectOptions connectOptions;

    /**
     * Connects the MQTT client.
     * 
     * @param clientId Client identifier for the broker.
     */
    @Connect
    public void connect(@ConnectionKey final String clientId) throws ConnectionException
    {
        this.clientId = clientId;

        final MqttClientPersistence clientPersistence = initializeClientPersistence();

        setupConnectOptions();

        try
        {
            LOGGER.debug("Creating client with ID of " + clientId);
            client = new MqttClient(getBrokerServerUri(), clientId, clientPersistence);
        }
        catch (final MqttException me)
        {
            throw new ConnectionException(ConnectionExceptionCode.UNKNOWN, null,
                "Failed to create the MQTT client", me);
        }

        if ((StringUtils.isNotBlank(getLwtTopicName())) && (StringUtils.isNotEmpty(getLwtMessage())))
        {
            LOGGER.debug("Setting up last will information...");
            final MqttTopic lwtTopic = client.getTopic(getLwtTopicName());
            connectOptions.setWill(lwtTopic, getLwtMessage().getBytes(), getLwtQos(), false);
            LOGGER.info("Last will information configured");
        }

        try
        {
            LOGGER.debug("Connecting client with ID of " + clientId);
            client.connect(connectOptions);
        }
        catch (final MqttException me)
        {
            throw new ConnectionException(ConnectionExceptionCode.UNKNOWN, null,
                "Failed to connect the MQTT client", me);
        }

        LOGGER.info("MQTT client successfully connected with ID: " + clientId + " at: "
                    + getBrokerServerUri());
    }

    private MqttClientPersistence initializeClientPersistence() throws ConnectionException
    {
        if (StringUtils.isBlank(getPersistenceLocation()))
        {
            return null;
        }

        try
        {
            final MqttClientPersistence clientPersistence = new MqttDefaultFilePersistence(
                getPersistenceLocation());
            LOGGER.info("File persistence activated at: " + getPersistenceLocation());
            return clientPersistence;
        }
        catch (final MqttPersistenceException mpe)
        {
            throw new ConnectionException(ConnectionExceptionCode.UNKNOWN, "",
                "Error creating file persistence for messages", mpe);
        }
    }

    /**
     * Method that sets up the MqttConnectOptions class for use. This reads the settings given via
     * the mqtt:config element.
     */
    private void setupConnectOptions()
    {
        connectOptions = new MqttConnectOptions();
        connectOptions.setCleanSession(isCleanSession());
        connectOptions.setConnectionTimeout(getConnectionTimeout());
        connectOptions.setKeepAliveInterval(getKeepAliveInterval());
        connectOptions.setUserName(getUsername());

        if (StringUtils.isNotBlank(getPassword()))
        {
            connectOptions.setPassword(getPassword().toCharArray());
        }
    }

    /**
     * Disconnects the client.
     * 
     * @throws MqttException
     */
    @Disconnect
    public void disconnect() throws MqttException
    {
        if ((client != null) && (client.isConnected()))
        {
            LOGGER.info("Diconnecting from MQTT broker...");
            client.disconnect();
        }

        client = null;
        connectOptions = null;
    }

    /**
     * Are we connected
     */
    @ValidateConnection
    public boolean isConnected()
    {
        return client != null && client.isConnected();
    }

    /**
     * Connection Identifier
     */
    @ConnectionIdentifier
    public String getClientId()
    {
        return clientId;
    }

    /**
     * Publish a message to a topic. If sucessful a flow variable named
     * {@link MqttConnector#MQTT_DELIVERY_TOKEN_VARIABLE} will contain the {@link MqttDeliveryToken}
     * that can be used for further awaiting completion.
     * <p/>
     * {@sample.xml ../../../doc/mqtt-connector.xml.sample mqtt:publish-1}
     * <p/>
     * {@sample.xml ../../../doc/mqtt-connector.xml.sample mqtt:publish-2}
     * 
     * @param topicName topic to publish message to.
     * @param waitForCompletionTimeOut time in milliseconds to wait for the delivery to occur.
     * @param qos QoS level to use when publishing message.
     * @param messagePayload the payload that will be published over MQTT.
     * @param muleEvent the in-flight {@link MuleEvent}.
     * @return the <code>byte[]</code> that was published.
     * @throws MqttException thrown if the MQTT publish fails.
     */
    @Processor
    @Inject
    public byte[] publish(final String topicName,
                          @Optional final Long waitForCompletionTimeOut,
                          @Optional @Default(MQTT_DEFAULT_QOS_STRING) final DeliveryQoS qos,
                          @Payload final byte[] messagePayload,
                          final MuleEvent muleEvent) throws MqttException
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Retrieving topic '" + topicName + "'");
        }

        final MqttTopic topic = client.getTopic(topicName);

        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Preparing message");
        }

        final MqttMessage mqttMessage = new MqttMessage(messagePayload);
        mqttMessage.setQos(qos.getCode());

        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Publishing message to broker with QoS: " + qos);
        }

        final MqttDeliveryToken token = topic.publish(mqttMessage);

        if (waitForCompletionTimeOut != null)
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Waiting for completion for a maximum of " + waitForCompletionTimeOut + "ms");
            }

            token.waitForCompletion(waitForCompletionTimeOut);
        }

        // exposed as a flowVar so further/custom completion handling can be done downstream
        muleEvent.setFlowVariable(MQTT_DELIVERY_TOKEN_VARIABLE, token);

        return messagePayload;
    }

    /**
     * Subscribe to a single or multiple topic filters.
     * <p/>
     * {@sample.xml ../../../doc/mqtt-connector.xml.sample mqtt:subscribe-1}
     * <p/>
     * {@sample.xml ../../../doc/mqtt-connector.xml.sample mqtt:subscribe-2}
     * 
     * @param topicFilter single topic filter to subscribe to.
     * @param qos QoS level to use when subscribing to a single topic.
     * @param topicSubscriptions a {@link List} of {@link MqttTopicSubscription} to subscribe to.
     * @param callback the {@link SourceCallback} used by Mule to dispatch the received messages.
     * @throws ConnectionException thrown if the MQTT subscribe fails.
     */
    @Source
    public void subscribe(@Optional final String topicFilter,
                          @Optional @Default(MQTT_DEFAULT_QOS_STRING) final DeliveryQoS qos,
                          @Optional final List<MqttTopicSubscription> topicSubscriptions,
                          final SourceCallback callback) throws ConnectionException
    {
        final List<MqttTopicSubscription> actualSubscriptions = new ArrayList<MqttTopicSubscription>();
        if (topicSubscriptions != null)
        {
            for (final Object o : topicSubscriptions)
            {
                // FIXME DEVKIT-351 why is DevKit passing a MqttTopicSubscriptionExpressionHolder?
                final MqttTopicSubscriptionExpressionHolder holder = (MqttTopicSubscriptionExpressionHolder) o;

                if (holder.getTopicFilter() != null)
                {
                    final MqttTopicSubscription topicSubscription = new MqttTopicSubscription(
                        holder.getTopicFilter().toString(),
                        holder.getQos() == null ? null : DeliveryQoS.valueOf(holder.getQos().toString()));

                    actualSubscriptions.add(new MqttTopicSubscription(topicSubscription.getTopicFilter(),
                        topicSubscription.getQos()));
                }
            }
        }
        if (StringUtils.isNotBlank(topicFilter))
        {
            actualSubscriptions.add(new MqttTopicSubscription(topicFilter, qos));
        }

        Validate.notEmpty(actualSubscriptions, "No topic filter has been defined to subscribe to");

        new MqttTopicListener(this, callback, actualSubscriptions).connect();
    }

    // Getters and Setters
    public MuleContext getMuleContext()
    {
        return muleContext;
    }

    public void setMuleContext(final MuleContext muleContext)
    {
        this.muleContext = muleContext;
    }

    public MqttClient getMqttClient()
    {
        return client;
    }

    public String getBrokerServerUri()
    {
        return brokerServerUri;
    }

    public void setBrokerServerUri(final String brokerServerUri)
    {
        this.brokerServerUri = brokerServerUri;
    }

    public boolean isCleanSession()
    {
        return cleanSession;
    }

    public void setCleanSession(final boolean cleanSession)
    {
        this.cleanSession = cleanSession;
    }

    public String getUsername()
    {
        return username;
    }

    public void setUsername(final String username)
    {
        this.username = username;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(final String password)
    {
        this.password = password;
    }

    public int getConnectionTimeout()
    {
        return connectionTimeout;
    }

    public void setConnectionTimeout(final int connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
    }

    public String getLwtTopicName()
    {
        return lwtTopicName;
    }

    public void setLwtTopicName(final String lwtTopicName)
    {
        this.lwtTopicName = lwtTopicName;
    }

    public String getLwtMessage()
    {
        return lwtMessage;
    }

    public void setLwtMessage(final String lwtMessage)
    {
        this.lwtMessage = lwtMessage;
    }

    public int getKeepAliveInterval()
    {
        return keepAliveInterval;
    }

    public void setKeepAliveInterval(final int keepAliveInterval)
    {
        this.keepAliveInterval = keepAliveInterval;
    }

    public String getPersistenceLocation()
    {
        return persistenceLocation;
    }

    public void setPersistenceLocation(final String persistenceLocation)
    {
        this.persistenceLocation = persistenceLocation;
    }

    public int getLwtQos()
    {
        return lwtQos;
    }

    public void setLwtQos(final int lwtQos)
    {
        this.lwtQos = lwtQos;
    }

    public boolean isLwtRetained()
    {
        return lwtRetained;
    }

    public void setLwtRetained(final boolean lwtRetained)
    {
        this.lwtRetained = lwtRetained;
    }
}
