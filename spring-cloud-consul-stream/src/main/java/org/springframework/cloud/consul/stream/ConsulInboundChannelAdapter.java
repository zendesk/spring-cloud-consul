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

import static org.springframework.util.Base64Utils.decodeFromString;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.scheduling.annotation.Scheduled;

import com.ecwid.consul.v1.event.model.Event;

/**
 * Adapter that receives Messages from Consul Events, converts them into Spring
 * Integration Messages, and sends the results to a Message Channel.
 * @author Spencer Gibb
 */
public class ConsulInboundChannelAdapter extends MessageProducerSupport {

	private EventService eventService;
	private final ScheduledExecutorService executor;
	private ScheduledFuture<?> future;

	public ConsulInboundChannelAdapter(EventService eventService) {
		this.eventService = eventService;
		this.executor = Executors.newScheduledThreadPool(1);
	}

	// link eventService to sendMessage
	/*
	 * Map<String, Object> headers =
	 * headerMapper.toHeadersFromRequest(message.getMessageProperties()); if
	 * (messageListenerContainer.getAcknowledgeMode() == AcknowledgeMode.MANUAL) {
	 * headers.put(AmqpHeaders.DELIVERY_TAG,
	 * message.getMessageProperties().getDeliveryTag()); headers.put(AmqpHeaders.CHANNEL,
	 * channel); }
	 * sendMessage(AmqpInboundChannelAdapter.this.getMessageBuilderFactory().withPayload
	 * (payload).copyHeaders(headers).build());
	 */

	// start thread
	// make blocking calls
	// foreach event -> send message

	@Override
	protected void doStart() {
		future = executor.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				List<Event> events = eventService.watch();
				for (Event event : events) {
					// Map<String, Object> headers = new HashMap<>();
					// headers.put(MessageHeaders.REPLY_CHANNEL, outputChannel.)
					byte[] bytes = decodeFromString(event.getPayload());
					String decoded = new String(bytes);
					bytes = decodeFromString(decoded);
					sendMessage(getMessageBuilderFactory().withPayload(bytes)
							// TODO: support headers
							.build());
				}
			}
		}, 10, 10, TimeUnit.MILLISECONDS);
	}

	@Override
	protected void doStop() {
		if (future != null) {
			future.cancel(true);
		}
	}
}
