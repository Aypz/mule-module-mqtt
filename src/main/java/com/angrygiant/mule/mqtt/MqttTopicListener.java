
package com.angrygiant.mule.mqtt;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.mule.api.callback.SourceCallback;

/**
 * Mule MQTT Module
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
 * TopicListener is responsible for initiating and holding a connection to the MQTT broker for topic
 * subscription.
 * </p>
 * 
 * @author dmiller@angrygiant.com
 */
public class MqttTopicListener implements MqttCallback
{
    private static final Logger LOGGER = Logger.getLogger(MqttTopicListener.class);

    public static final String TOPIC_NAME = "mqtt.topic.name";
    public static final String MESSAGE_DUPLICATE = "mqtt.message.duplicated";
    public static final String MESSAGE_RETAIN = "mqtt.message.retained";
    public static final String MESSAGE_QOS = "mqtt.message.qos";
    public static final String CLIENT_ID = "mqtt.client.id";
    public static final String CLIENT_URI = "mqtt.client.serverUri";

    private final MqttClient client;
    private final SourceCallback callback;
    private final MqttConnectOptions connectOptions;
    private final String topicName;
    private int qos = 2;
    private long subscriptionDelay = 500;

    public MqttTopicListener(final MqttClient client,
                             final SourceCallback callback,
                             final String topicName,
                             final MqttConnectOptions connectOptions,
                             final long subscriptionDelay)
    {
        this(client, callback, topicName, connectOptions, subscriptionDelay, 2);
    }

    public MqttTopicListener(final MqttClient client,
                             final SourceCallback callback,
                             final String topicName,
                             final MqttConnectOptions connectOptions,
                             final long subscriptionDelay,
                             final int qos)
    {
        this.client = client;
        this.callback = callback;
        this.topicName = topicName;
        this.connectOptions = connectOptions;
        this.subscriptionDelay = subscriptionDelay;
        this.qos = qos;
    }

    public void connectionLost(final Throwable throwable)
    {
        LOGGER.warn("AH! You got throw off the broker! - Attempting to reconnect...");
        LOGGER.trace(throwable);

        if (!canReconnect())
        {
            LOGGER.error("Can't reconnect to broker! No Messages will be received!");
        }
    }

    private boolean canReconnect()
    {
        boolean reconnected = false;

        if (!client.isConnected())
        {
            try
            {
                LOGGER.debug("Sending disconnect for client...");
                client.disconnect();

                LOGGER.debug("Setting callback method to myself...");
                client.setCallback(this);

                LOGGER.debug("Connecting with connection options provided from mqtt:config element...");
                client.connect(connectOptions);

                LOGGER.debug("Sleeping to waiting for established connection...");
                Thread.sleep(subscriptionDelay);

                LOGGER.debug("Subscribing to topic " + topicName + " with QOS of " + qos);
                client.subscribe(this.topicName, this.qos);

                LOGGER.debug("Reconnection was successful - setting flag to true");
                reconnected = true;
            }
            catch (final MqttException e)
            {
                LOGGER.error("MQTT Error while reconnecting for subscription: ", e);
            }
            catch (final InterruptedException e)
            {
                LOGGER.error("Sleep Error while reconnecting for subscription: ", e);
            }
        }

        LOGGER.debug("Reconnection is determined as " + (client.isConnected() && reconnected));
        return client.isConnected() && reconnected;
    }

    public void messageArrived(final MqttTopic mqttTopic, final MqttMessage mqttMessage) throws Exception
    {
        LOGGER.info("Creating new Mule message - got something on " + mqttTopic.getName());

        final Map<String, Object> properties = new HashMap<String, Object>();
        properties.put(MESSAGE_DUPLICATE, mqttMessage.isDuplicate());
        properties.put(MESSAGE_QOS, mqttMessage.getQos());
        properties.put(MESSAGE_RETAIN, mqttMessage.isRetained());
        properties.put(TOPIC_NAME, mqttTopic.getName());
        properties.put(CLIENT_ID, this.client.getClientId());
        properties.put(CLIENT_URI, this.client.getServerURI());

        this.callback.process(mqttMessage.getPayload(), properties);
    }

    public void deliveryComplete(final MqttDeliveryToken mqttDeliveryToken)
    {
        LOGGER.trace("Message Delivered");
    }
}
