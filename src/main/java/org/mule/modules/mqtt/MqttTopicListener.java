/*
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.md file.
 * 
 */

package org.mule.modules.mqtt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.mule.api.ConnectionException;
import org.mule.api.ConnectionExceptionCode;
import org.mule.api.callback.SourceCallback;
import org.mule.api.config.MuleProperties;
import org.mule.api.retry.RetryCallback;
import org.mule.api.retry.RetryContext;
import org.mule.api.retry.RetryPolicyTemplate;
import org.mule.modules.mqtt.MqttConnector.DeliveryQoS;

/**
 * Topic Listener for the Mule MQTT Module.
 * 
 * @author dmiller@angrygiant.com
 */
// TODO remove all the locally managed reconnection strategy when DEVKIT-353 is done
public class MqttTopicListener implements MqttCallback
{
    private static final Log LOGGER = LogFactory.getLog(MqttTopicListener.class);

    private final MqttConnector connector;
    private final SourceCallback callback;
    private final List<MqttTopicSubscription> subscriptions;

    public MqttTopicListener(final MqttConnector connector,
                             final SourceCallback callback,
                             final List<MqttTopicSubscription> subscriptions)
    {
        this.connector = connector;
        this.callback = callback;
        this.subscriptions = subscriptions;
    }

    public void connect() throws ConnectionException
    {
        final String[] topicFilters = new String[subscriptions.size()];
        final int[] qoss = new int[subscriptions.size()];
        int i = 0;
        for (final MqttTopicSubscription subscription : subscriptions)
        {
            topicFilters[i] = subscription.getTopicFilter();
            qoss[i] = subscription.getQos().getCode();
            i++;
        }

        try
        {
            connector.getMqttClient().setCallback(this);
            connector.getMqttClient().subscribe(topicFilters, qoss);
        }
        catch (final MqttException me)
        {
            throw new ConnectionException(ConnectionExceptionCode.UNKNOWN, null, "Subscription Error", me);
        }

        LOGGER.info("Subscribed to: " + subscriptions);
    }

    public void connectionLost(final Throwable throwable)
    {
        try
        {
            reconnect(throwable);
        }
        catch (final Exception e)
        {
            LOGGER.error("Failed to reconnect listener for: " + subscriptions, e);
        }
    }

    private void reconnect(final Throwable throwable) throws Exception
    {
        final RetryPolicyTemplate retryPolicyTemplate = connector.getMuleContext()
            .getRegistry()
            .lookupObject(MuleProperties.OBJECT_DEFAULT_RETRY_POLICY_TEMPLATE);

        retryPolicyTemplate.execute(new RetryCallback()
        {
            @Override
            public String getWorkDescription()
            {
                return "Reconnection of listener for: " + subscriptions;
            }

            @Override
            public void doWork(final RetryContext context) throws Exception
            {
                LOGGER.error("Disconnecting connector after losing connection", throwable);

                try
                {
                    connector.disconnect();
                }
                catch (final MqttException me)
                {
                    LOGGER.warn("Failed to cleanly disconnect connector", me);
                }

                connector.connect(connector.getActiveClientId());

                connect();
            }
        }, connector.getMuleContext().getWorkManager());
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
