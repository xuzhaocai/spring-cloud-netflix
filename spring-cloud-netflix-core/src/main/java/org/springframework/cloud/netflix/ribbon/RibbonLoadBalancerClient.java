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

package org.springframework.cloud.netflix.ribbon;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.LoadBalancerClient;
import org.springframework.cloud.client.loadbalancer.LoadBalancerRequest;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import com.netflix.client.config.IClientConfig;
import com.netflix.loadbalancer.ILoadBalancer;
import com.netflix.loadbalancer.Server;

/**
 * @author Spencer Gibb
 * @author Dave Syer
 * @author Ryan Baxter
 *
 *
 *
 * RibbonLoadBalancerClient
 *
 */
public class RibbonLoadBalancerClient implements LoadBalancerClient {

	private SpringClientFactory clientFactory;

	public RibbonLoadBalancerClient(SpringClientFactory clientFactory) {
		this.clientFactory = clientFactory;
	}


	/**
	 * 重组 uri方法
	 * @param instance
	 * @param original a URI with the host as a logical service name
	 * @return
	 */
	@Override
	public URI reconstructURI(ServiceInstance instance, URI original) {
		Assert.notNull(instance, "instance can not be null");
		// 获取serviceId
		String serviceId = instance.getServiceId();

		// 获取context
		RibbonLoadBalancerContext context = this.clientFactory
				.getLoadBalancerContext(serviceId);
		Server server = new Server(instance.getHost(), instance.getPort());

		// 获取client配置
		IClientConfig clientConfig = clientFactory.getClientConfig(serviceId);
		ServerIntrospector serverIntrospector = serverIntrospector(serviceId);



		// 更新https
		URI uri = RibbonUtils.updateToHttpsIfNeeded(original, clientConfig,
				serverIntrospector, server);

		// 拼接uri
		return context.reconstructURIWithServer(server, uri);
	}

	@Override
	public ServiceInstance choose(String serviceId) {
		Server server = getServer(serviceId);
		if (server == null) {
			return null;
		}
		return new RibbonServer(serviceId, server, isSecure(server, serviceId),
				serverIntrospector(serviceId).getMetadata(server));
	}

	/**
	 *
	 * 执行
	 * @param serviceId the service id to look up the LoadBalancer  服务id   ServiceA
	 * @param request allows implementations to execute pre and post actions such as
	 * incrementing metrics
	 * @param <T>
	 * @return
	 * @throws IOException
	 */
	@Override
	public <T> T execute(String serviceId, LoadBalancerRequest<T> request) throws IOException {


		// 获取ribbon 的loadBalancer
		ILoadBalancer loadBalancer = getLoadBalancer(serviceId);

		// 然后使用loadBalancer 选出一个server
		Server server = getServer(loadBalancer);
		if (server == null) {
			throw new IllegalStateException("No instances available for " + serviceId);
		}

		// 封装ribbonServer
		RibbonServer ribbonServer = new RibbonServer(serviceId, server, isSecure(server,
				serviceId), serverIntrospector(serviceId).getMetadata(server));


		//执行
		return execute(serviceId, ribbonServer, request);
	}

	@Override
	public <T> T execute(String serviceId, ServiceInstance serviceInstance, LoadBalancerRequest<T> request) throws IOException {
		Server server = null;
		if(serviceInstance instanceof RibbonServer) {

			// 获取server
			server = ((RibbonServer)serviceInstance).getServer();
		}
		if (server == null) {
			throw new IllegalStateException("No instances available for " + serviceId);
		}
		// 获取context
		RibbonLoadBalancerContext context = this.clientFactory
				.getLoadBalancerContext(serviceId);


		RibbonStatsRecorder statsRecorder = new RibbonStatsRecorder(context, server);

		try {

			// 继续执行
			T returnVal = request.apply(serviceInstance);

			//记录状态的
			statsRecorder.recordStats(returnVal);
			return returnVal;
		}
		// catch IOException and rethrow so RestTemplate behaves correctly
		catch (IOException ex) {
			statsRecorder.recordStats(ex);
			throw ex;
		}
		catch (Exception ex) {
			statsRecorder.recordStats(ex);
			ReflectionUtils.rethrowRuntimeException(ex);
		}
		return null;
	}

	private ServerIntrospector serverIntrospector(String serviceId) {
		ServerIntrospector serverIntrospector = this.clientFactory.getInstance(serviceId,
				ServerIntrospector.class);
		if (serverIntrospector == null) {
			serverIntrospector = new DefaultServerIntrospector();
		}
		return serverIntrospector;
	}

	private boolean isSecure(Server server, String serviceId) {
		IClientConfig config = this.clientFactory.getClientConfig(serviceId);
		ServerIntrospector serverIntrospector = serverIntrospector(serviceId);
		return RibbonUtils.isSecure(config, serverIntrospector, server);
	}

	protected Server getServer(String serviceId) {
		return getServer(getLoadBalancer(serviceId));
	}
	// 使用loadbalancer 选出一个server
	protected Server getServer(ILoadBalancer loadBalancer) {
		if (loadBalancer == null) {
			return null;
		}

		// 调用choose方法
		return loadBalancer.chooseServer("default"); // TODO: better handling of key
	}

	/**
	 * 根据服务id获取 loadbalaner
	 * @param serviceId
	 * @return
	 */
	protected ILoadBalancer getLoadBalancer(String serviceId) {

		// SpringClientFactory
		return this.clientFactory.getLoadBalancer(serviceId);
	}

	public static class RibbonServer implements ServiceInstance {
		private final String serviceId;
		private final Server server;
		private final boolean secure;
		private Map<String, String> metadata;

		public RibbonServer(String serviceId, Server server) {
			this(serviceId, server, false, Collections.<String, String> emptyMap());
		}

		public RibbonServer(String serviceId, Server server, boolean secure,
				Map<String, String> metadata) {
			this.serviceId = serviceId;//服务id
			this.server = server;  // server信息
			this.secure = secure;
			this.metadata = metadata;
		}

		@Override
		public String getServiceId() {
			return this.serviceId;
		}

		@Override
		public String getHost() {
			return this.server.getHost();
		}

		@Override
		public int getPort() {
			return this.server.getPort();
		}

		@Override
		public boolean isSecure() {
			return this.secure;
		}

		@Override
		public URI getUri() {
			return DefaultServiceInstance.getUri(this);
		}

		@Override
		public Map<String, String> getMetadata() {
			return this.metadata;
		}

		public Server getServer() {
			return this.server;
		}

		@Override
		public String toString() {
			final StringBuffer sb = new StringBuffer("RibbonServer{");
			sb.append("serviceId='").append(serviceId).append('\'');
			sb.append(", server=").append(server);
			sb.append(", secure=").append(secure);
			sb.append(", metadata=").append(metadata);
			sb.append('}');
			return sb.toString();
		}
	}

}
