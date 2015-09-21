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
import org.springframework.cloud.stream.binder.MessageChannelBinderSupport;
import org.springframework.messaging.MessageChannel;

import java.util.Properties;

/**
 * @author Spencer Gibb
 */
@CommonsLog
public class ConsulMessageChannelBinder extends MessageChannelBinderSupport implements DisposableBean {
	@Override
	public void bindConsumer(String name, MessageChannel inboundBindTarget, Properties properties) {
		//throw new UnsupportedOperationException("consul events do not support queues");
	}

	@Override
	public void bindPubSubConsumer(String name, MessageChannel inboundBindTarget, Properties properties) {
		if (logger.isInfoEnabled()) {
			logger.info("declaring pubsub for inbound: " + name);
		}
	}

	@Override
	public void bindProducer(String name, MessageChannel outboundBindTarget, Properties properties) {
		//throw new UnsupportedOperationException("consul events do not support queues");
	}

	@Override
	public void bindPubSubProducer(String name, MessageChannel outboundBindTarget, Properties properties) {

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
}
