/*
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.md file.
 * 
 */

package com.angrygiant.mule.mqtt;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;
import org.mule.api.MuleEventContext;
import org.mule.api.MuleMessage;
import org.mule.tck.functional.EventCallback;
import org.mule.tck.functional.FunctionalTestComponent;
import org.mule.tck.junit4.FunctionalTestCase;

public class MqttITCase extends FunctionalTestCase
{
    @Override
    protected String getConfigResources()
    {
        return "mqtt-connector-tests-config.xml";
    }

    @Test
    public void publishAndSubscribe() throws Exception
    {
        final CountDownLatch subscriberSingleFilterComponentCountDownLatch = setupTestComponentForExpectedMessageCount(
            "subscriberSingleFilter", 1);

        final CountDownLatch subscriberMultiFiltersComponentCountDownLatch = setupTestComponentForExpectedMessageCount(
            "subscriberMultiFilters", 2);

        final String testTopicPayload = RandomStringUtils.randomAlphanumeric(20);
        MuleMessage result = muleContext.getClient().send("vm://publisher.in", testTopicPayload,
            Collections.<String, Object> singletonMap("topicName", "test/topic"));

        assertThat(result.getPayloadAsString(), is(testTopicPayload));

        final String testOtherPayload = RandomStringUtils.randomAlphanumeric(20);
        result = muleContext.getClient().send("vm://publisher.in", testOtherPayload,
            Collections.<String, Object> singletonMap("topicName", "test/other"));

        assertThat(result.getPayloadAsString(), is(testOtherPayload));

        subscriberSingleFilterComponentCountDownLatch.await(getTestTimeoutSecs(), TimeUnit.SECONDS);
        subscriberMultiFiltersComponentCountDownLatch.await(getTestTimeoutSecs(), TimeUnit.SECONDS);

        assertThat(getReceivedMessagePayloads("subscriberSingleFilter"), is(Arrays.asList(testTopicPayload)));
        assertThat(getReceivedMessagePayloads("subscriberMultiFilters"),
            is(Arrays.asList(testTopicPayload, testOtherPayload)));
    }

    private List<String> getReceivedMessagePayloads(final String flowName) throws Exception
    {
        final FunctionalTestComponent functionalTestComponent = getFunctionalTestComponent(flowName);
        final List<String> receivedMessagePayloads = new ArrayList<String>();
        for (int i = 1; i <= functionalTestComponent.getReceivedMessagesCount(); i++)
        {
            receivedMessagePayloads.add(new String((byte[]) functionalTestComponent.getReceivedMessage(i)));
        }
        return receivedMessagePayloads;
    }

    private CountDownLatch setupTestComponentForExpectedMessageCount(final String flowName,
                                                                     final int expectedMessageCount)
        throws Exception
    {
        final CountDownLatch countDownLatch = new CountDownLatch(expectedMessageCount);
        final FunctionalTestComponent functionalTestComponent = getFunctionalTestComponent(flowName);
        functionalTestComponent.setEventCallback(new EventCallback()
        {
            @Override
            public void eventReceived(final MuleEventContext context, final Object component)
                throws Exception
            {
                countDownLatch.countDown();
            }
        });
        return countDownLatch;
    }
}
