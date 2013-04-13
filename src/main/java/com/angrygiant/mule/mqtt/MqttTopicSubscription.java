/*
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.md file.
 * 
 */

package com.angrygiant.mule.mqtt;

import com.angrygiant.mule.mqtt.MqttConnector.DeliveryQoS;

public class MqttTopicSubscription
{
    private String topicFilter;
    private DeliveryQoS qos;

    public MqttTopicSubscription()
    {
        // NOOP
    }

    public MqttTopicSubscription(final String topicFilter, final DeliveryQoS qos)
    {
        this.topicFilter = topicFilter;
        this.qos = qos;
    }

    public String getTopicFilter()
    {
        return topicFilter;
    }

    public void setTopicFilter(final String topicFilter)
    {
        this.topicFilter = topicFilter;
    }

    public DeliveryQoS getQos()
    {
        return qos == null ? MqttConnector.MQTT_DEFAULT_QOS : qos;
    }

    public void setQos(final DeliveryQoS qos)
    {
        this.qos = qos;
    }

    @Override
    public String toString()
    {
        return "Topic Filter: " + getTopicFilter() + " - QoS: " + getQos();
    }
}
