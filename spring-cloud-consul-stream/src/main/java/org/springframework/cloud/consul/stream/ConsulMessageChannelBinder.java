/*
 * Copyright 2013-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.consul.stream;

import lombok.extern.apachecommons.CommonsLog;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.cloud.stream.binder.AbstractBinderPropertiesAccessor;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.EmbeddedHeadersMessageConverter;
import org.springframework.cloud.stream.binder.MessageChannelBinderSupport;
import org.springframework.cloud.stream.binder.MessageValues;
import org.springframework.expression.Expression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.Properties;

/**
 * @author Spencer Gibb
 */
@CommonsLog
public class ConsulMessageChannelBinder extends MessageChannelBinderSupport implements DisposableBean {

	private final EmbeddedHeadersMessageConverter embeddedHeadersMessageConverter = new
			EmbeddedHeadersMessageConverter();
	private final String[] headersToMap;

	private final EventService eventService;

	public ConsulMessageChannelBinder(EventService eventService) {
		this.eventService = eventService;

		this.headersToMap = BinderHeaders.STANDARD_HEADERS;
	}

	@Override
	public void bindConsumer(String name, MessageChannel moduleInputChannel, Properties properties) {
		if (name.startsWith(P2P_NAMED_CHANNEL_TYPE_PREFIX)) {
			throw new UnsupportedOperationException("consul events do not support queues");
		}
		ConsulPropertiesAccessor accessor = new ConsulPropertiesAccessor(properties);
		String queueName = "queue." + name;
		int partitionIndex = accessor.getPartitionIndex();
		if (partitionIndex >= 0) {
			queueName += "-" + partitionIndex;
		}
		MessageProducerSupport adapter = new ConsulInboundChannelAdapter(eventService);//createInboundAdapter(accessor, queueName);
		doRegisterConsumer(name, name + (partitionIndex >= 0 ? "-" + partitionIndex : ""), moduleInputChannel, adapter,
				accessor);
		bindExistingProducerDirectlyIfPossible(name, moduleInputChannel);
	}

	@Override
	public void bindPubSubConsumer(String name, MessageChannel moduleInputChannel, Properties properties) {
		if (logger.isInfoEnabled()) {
			logger.info("declaring pubsub for inbound: " + name);
		}
		ConsulInboundChannelAdapter adapter = new ConsulInboundChannelAdapter(eventService);
		adapter.setBeanFactory(this.getBeanFactory());
		//adapter.setSerializer(null);
		//adapter.setTopics(applyPubSub(name));
		doRegisterConsumer(name, name, moduleInputChannel, adapter, new ConsulPropertiesAccessor(properties));
	}

	private void doRegisterConsumer(String bindingName, String channelName, MessageChannel moduleInputChannel,
									MessageProducerSupport adapter, ConsulPropertiesAccessor properties) {
		DirectChannel bridgeToModuleChannel = new DirectChannel();
		bridgeToModuleChannel.setBeanFactory(this.getBeanFactory());
		bridgeToModuleChannel.setBeanName(channelName + ".bridge");
		MessageChannel bridgeInputChannel = bridgeToModuleChannel; //addRetryIfNeeded(channelName, bridgeToModuleChannel, properties);
		adapter.setOutputChannel(bridgeInputChannel);
		adapter.setBeanName("inbound." + bindingName);
		adapter.afterPropertiesSet();
		Binding consumerBinding = Binding.forConsumer(bindingName, adapter, moduleInputChannel, properties);
		addBinding(consumerBinding);
		ReceivingHandler convertingBridge = new ReceivingHandler();
		convertingBridge.setOutputChannel(moduleInputChannel);
		convertingBridge.setBeanName(channelName + ".bridge.handler");
		convertingBridge.afterPropertiesSet();
		bridgeToModuleChannel.subscribe(convertingBridge);
		consumerBinding.start();
	}

	@Override
	public void bindProducer(String name, MessageChannel moduleOutputChannel, Properties properties) {
		if (name.startsWith(P2P_NAMED_CHANNEL_TYPE_PREFIX)) {
			throw new UnsupportedOperationException("consul events do not support queues");
		}
		ConsulPropertiesAccessor accessor = new ConsulPropertiesAccessor(properties);
		if (!bindNewProducerDirectlyIfPossible(name, (SubscribableChannel) moduleOutputChannel, accessor)) {
			String partitionKeyExtractorClass = accessor.getPartitionKeyExtractorClass();
			Expression partitionKeyExpression = accessor.getPartitionKeyExpression();
			ConsulOutboundEndpoint queue = new ConsulOutboundEndpoint(eventService.consul);
			String queueName = "queue." + name;
			/*if (partitionKeyExpression == null && !StringUtils.hasText(partitionKeyExtractorClass)) {
				queue = new RedisQueueOutboundChannelAdapter(queueName, this.connectionFactory);
			}
			else {
				queue = new RedisQueueOutboundChannelAdapter(
						parser.parseExpression(buildPartitionRoutingExpression(queueName)), this.connectionFactory);
			}
			queue.setIntegrationEvaluationContext(this.evaluationContext);*/
			queue.setBeanFactory(this.getBeanFactory());
			queue.afterPropertiesSet();
			doRegisterProducer(name, moduleOutputChannel, queue, accessor);
		}
	}

	@Override
	public void bindPubSubProducer(String name, MessageChannel moduleOutputChannel, Properties properties) {
		ConsulOutboundEndpoint topic = new ConsulOutboundEndpoint(eventService.consul);
		topic.setBeanFactory(this.getBeanFactory());
		//topic.setTopic(applyPubSub(name));
		topic.afterPropertiesSet();
		doRegisterProducer(name, moduleOutputChannel, topic, new ConsulPropertiesAccessor(properties));
	}

	private void doRegisterProducer(final String name, MessageChannel moduleOutputChannel, MessageHandler delegate,
									ConsulPropertiesAccessor properties) {
		this.doRegisterProducer(name, moduleOutputChannel, delegate, null, properties);
	}

	private void doRegisterProducer(final String name, MessageChannel moduleOutputChannel, MessageHandler delegate,
									String replyTo, ConsulPropertiesAccessor properties) {
		Assert.isInstanceOf(SubscribableChannel.class, moduleOutputChannel);
		MessageHandler handler = new SendingHandler(delegate, replyTo, properties);
		EventDrivenConsumer consumer = new EventDrivenConsumer((SubscribableChannel) moduleOutputChannel, handler);
		consumer.setBeanFactory(this.getBeanFactory());
		consumer.setBeanName("outbound." + name);
		consumer.afterPropertiesSet();
		Binding producerBinding = Binding.forProducer(name, moduleOutputChannel, consumer, properties);
		addBinding(producerBinding);
		producerBinding.start();
	}
	@Override
	public void bindRequestor(String name, MessageChannel requests, MessageChannel replies, Properties properties) {
		throw new UnsupportedOperationException("consul events do not support async replies");
	}

	@Override
	public void bindReplier(String name, MessageChannel requests, MessageChannel replies, Properties properties) {
		throw new UnsupportedOperationException("consul events do not support async replies");
	}

	@Override
	public void destroy() throws Exception {

	}

	private static class ConsulPropertiesAccessor extends AbstractBinderPropertiesAccessor {
		public ConsulPropertiesAccessor(Properties properties) {
			super(properties);
		}
	}

	private class ReceivingHandler extends AbstractReplyProducingMessageHandler {

		public ReceivingHandler() {
			super();
			this.setBeanFactory(ConsulMessageChannelBinder.this.getBeanFactory());
		}

		@SuppressWarnings("unchecked")
		@Override
		protected Object handleRequestMessage(Message<?> requestMessage) {
			MessageValues theRequestMessage;
			try {
				theRequestMessage = embeddedHeadersMessageConverter.extractHeaders((Message<byte[]>) requestMessage, true);
			}
			catch (Exception e) {
				logger.error(EmbeddedHeadersMessageConverter.decodeExceptionMessage(requestMessage), e);
				theRequestMessage = new MessageValues(requestMessage);
			}
			return deserializePayloadIfNecessary(theRequestMessage).toMessage(getMessageBuilderFactory());
		}

		@Override
		protected boolean shouldCopyRequestHeaders() {
			// prevent returned message from being copied in superclass
			return false;
		}
	}

	private class SendingHandler extends AbstractMessageHandler {

		private final MessageHandler delegate;

		private final String replyTo;

		private final PartitioningMetadata partitioningMetadata;


		private SendingHandler(MessageHandler delegate, String replyTo, ConsulPropertiesAccessor properties) {
			this.delegate = delegate;
			this.replyTo = replyTo;
			this.partitioningMetadata = new PartitioningMetadata(properties, properties.getNextModuleCount());
			this.setBeanFactory(ConsulMessageChannelBinder.this.getBeanFactory());
		}

		@Override
		protected void handleMessageInternal(Message<?> message) throws Exception {
			MessageValues transformed = serializePayloadIfNecessary(message);

			if (replyTo != null) {
				transformed.put(BinderHeaders.REPLY_TO, this.replyTo);
			}
			if (this.partitioningMetadata.isPartitionedModule()) {

				transformed.put(PARTITION_HEADER, determinePartition(message, this.partitioningMetadata));
			}

			byte[] messageToSend = embeddedHeadersMessageConverter.embedHeaders(transformed,
					ConsulMessageChannelBinder.this.headersToMap);
			delegate.handleMessage(MessageBuilder.withPayload(messageToSend).copyHeaders(transformed).build());
		}

	}
}
