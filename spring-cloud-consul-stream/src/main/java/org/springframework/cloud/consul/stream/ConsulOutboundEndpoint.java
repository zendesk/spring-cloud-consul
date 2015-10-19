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

import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.Message;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.event.model.Event;
import com.ecwid.consul.v1.event.model.EventParams;
import org.springframework.util.Base64Utils;

import java.nio.charset.Charset;

/**
 * Adapter that converts and sends Messages as Consul events
 * @author Spencer Gibb
 */
public class ConsulOutboundEndpoint extends AbstractReplyProducingMessageHandler {

	protected final ConsulClient consul;

	public ConsulOutboundEndpoint(ConsulClient consul) {
		this.consul = consul;
	}

	@Override
	protected Object handleRequestMessage(Message<?> requestMessage) {
		Object payload = requestMessage.getPayload();
		// TODO: support headers
		// TODO: support consul event filters: NodeFilter, ServiceFilter, TagFilter

		String encodedPayload = null;

		if (payload instanceof byte[]) {
			encodedPayload = new String((byte[])payload, Charset.forName("UTF-8"));
		} else {
			throw new RuntimeException("Unable to encode payload of type: "+payload.getClass());
		}

		Response<Event> event = consul.eventFire("springCloudBus", encodedPayload,
				new EventParams(), QueryParams.DEFAULT);
		// TODO: return event?
		return null;
	}
}
