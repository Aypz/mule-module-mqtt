/*
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.md file.
 * 
 */

package com.angrygiant.mule.mqtt;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.demux.DemuxingProtocolDecoder;
import org.apache.mina.filter.codec.demux.DemuxingProtocolEncoder;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.dna.mqtt.moquette.messaging.spi.impl.SimpleMessaging;
import org.dna.mqtt.moquette.proto.ConnAckEncoder;
import org.dna.mqtt.moquette.proto.ConnectDecoder;
import org.dna.mqtt.moquette.proto.DisconnectDecoder;
import org.dna.mqtt.moquette.proto.MQTTLoggingFilter;
import org.dna.mqtt.moquette.proto.PingReqDecoder;
import org.dna.mqtt.moquette.proto.PingRespEncoder;
import org.dna.mqtt.moquette.proto.PubAckDecoder;
import org.dna.mqtt.moquette.proto.PubAckEncoder;
import org.dna.mqtt.moquette.proto.PubCompDecoder;
import org.dna.mqtt.moquette.proto.PubCompEncoder;
import org.dna.mqtt.moquette.proto.PubCompMessage;
import org.dna.mqtt.moquette.proto.PubRecDecoder;
import org.dna.mqtt.moquette.proto.PubRecEncoder;
import org.dna.mqtt.moquette.proto.PubRelDecoder;
import org.dna.mqtt.moquette.proto.PubRelEncoder;
import org.dna.mqtt.moquette.proto.PublishDecoder;
import org.dna.mqtt.moquette.proto.PublishEncoder;
import org.dna.mqtt.moquette.proto.SubAckEncoder;
import org.dna.mqtt.moquette.proto.SubscribeDecoder;
import org.dna.mqtt.moquette.proto.UnsubAckEncoder;
import org.dna.mqtt.moquette.proto.UnsubscribeDecoder;
import org.dna.mqtt.moquette.proto.messages.ConnAckMessage;
import org.dna.mqtt.moquette.proto.messages.PingRespMessage;
import org.dna.mqtt.moquette.proto.messages.PubAckMessage;
import org.dna.mqtt.moquette.proto.messages.PubRecMessage;
import org.dna.mqtt.moquette.proto.messages.PubRelMessage;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;
import org.dna.mqtt.moquette.proto.messages.SubAckMessage;
import org.dna.mqtt.moquette.proto.messages.UnsubAckMessage;
import org.dna.mqtt.moquette.server.MQTTHandler;

public class MqttTestBroker
{
    private NioSocketAcceptor nioSocketAcceptor;
    private SimpleMessaging messaging;

    public void startServer(final int port) throws IOException
    {
        final DemuxingProtocolDecoder decoder = new DemuxingProtocolDecoder();
        decoder.addMessageDecoder(new ConnectDecoder());
        decoder.addMessageDecoder(new PublishDecoder());
        decoder.addMessageDecoder(new PubAckDecoder());
        decoder.addMessageDecoder(new PubRelDecoder());
        decoder.addMessageDecoder(new PubRecDecoder());
        decoder.addMessageDecoder(new PubCompDecoder());
        decoder.addMessageDecoder(new SubscribeDecoder());
        decoder.addMessageDecoder(new UnsubscribeDecoder());
        decoder.addMessageDecoder(new DisconnectDecoder());
        decoder.addMessageDecoder(new PingReqDecoder());

        final DemuxingProtocolEncoder encoder = new DemuxingProtocolEncoder();

        encoder.addMessageEncoder(ConnAckMessage.class, new ConnAckEncoder());
        encoder.addMessageEncoder(SubAckMessage.class, new SubAckEncoder());
        encoder.addMessageEncoder(UnsubAckMessage.class, new UnsubAckEncoder());
        encoder.addMessageEncoder(PubAckMessage.class, new PubAckEncoder());
        encoder.addMessageEncoder(PubRecMessage.class, new PubRecEncoder());
        encoder.addMessageEncoder(PubCompMessage.class, new PubCompEncoder());
        encoder.addMessageEncoder(PubRelMessage.class, new PubRelEncoder());
        encoder.addMessageEncoder(PublishMessage.class, new PublishEncoder());
        encoder.addMessageEncoder(PingRespMessage.class, new PingRespEncoder());

        nioSocketAcceptor = new NioSocketAcceptor();

        nioSocketAcceptor.getFilterChain().addLast("logger", new MQTTLoggingFilter("SERVER LOG"));
        nioSocketAcceptor.getFilterChain().addLast("codec", new ProtocolCodecFilter(encoder, decoder));

        final MQTTHandler handler = new MQTTHandler();
        messaging = SimpleMessaging.getInstance();
        messaging.init();

        handler.setMessaging(messaging);

        nioSocketAcceptor.setHandler(handler);
        nioSocketAcceptor.setReuseAddress(true);
        nioSocketAcceptor.getSessionConfig().setReuseAddress(true);
        nioSocketAcceptor.getSessionConfig().setReadBufferSize(2048);
        nioSocketAcceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 10);
        nioSocketAcceptor.getStatistics().setThroughputCalculationInterval(10);
        nioSocketAcceptor.getStatistics().updateThroughput(System.currentTimeMillis());
        nioSocketAcceptor.bind(new InetSocketAddress(port));
    }

    public void stopServer()
    {
        messaging.stop();

        for (final IoSession session : nioSocketAcceptor.getManagedSessions().values())
        {
            if ((session.isConnected()) && (!(session.isClosing())))
            {
                session.close(false);
            }
        }

        nioSocketAcceptor.unbind();
        nioSocketAcceptor.dispose();
    }
}
