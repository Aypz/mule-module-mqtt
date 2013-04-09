
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
