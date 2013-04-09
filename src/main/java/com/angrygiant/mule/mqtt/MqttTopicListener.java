
package com.angrygiant.mule.mqtt;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.mule.api.callback.SourceCallback;

import com.angrygiant.mule.mqtt.MqttConnector.DeliveryQoS;

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
    private static final Log LOGGER = LogFactory.getLog(MqttTopicListener.class);

    private final MqttConnector connector;
    private final SourceCallback callback;

    public MqttTopicListener(final MqttConnector connector, final SourceCallback callback)
    {
        this.connector = connector;
        this.callback = callback;
    }

    public void connectionLost(final Throwable throwable)
    {
        LOGGER.error("Disconnecting connector after losing connection", throwable);

        try
        {
            connector.disconnect();
        }
        catch (final MqttException me)
        {
            LOGGER.debug("Failed to cleanly disconnect connector", me);
        }
    }

    public void messageArrived(final MqttTopic mqttTopic, final MqttMessage mqttMessage) throws Exception
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Message arrived on topic: " + mqttTopic.getName() + " is: " + mqttMessage);
        }

        final Map<String, Object> properties = new HashMap<String, Object>();
        properties.put(MqttConnector.MQTT_TOPIC_NAME_PROPERTY, mqttTopic.getName());
        properties.put(MqttConnector.MQTT_QOS_PROPERTY, DeliveryQoS.fromCode(mqttMessage.getQos()));

        callback.process(mqttMessage.getPayload(), properties);
    }

    public void deliveryComplete(final MqttDeliveryToken mqttDeliveryToken)
    {
        // NOOP
    }
}
