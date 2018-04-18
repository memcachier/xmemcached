package net.rubyeye.xmemcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.rubyeye.xmemcached.auth.AuthInfo;
import net.rubyeye.xmemcached.buffer.BufferAllocator;
import net.rubyeye.xmemcached.buffer.SimpleBufferAllocator;
import net.rubyeye.xmemcached.command.TextCommandFactory;
import net.rubyeye.xmemcached.impl.ArrayMemcachedSessionLocator;
import net.rubyeye.xmemcached.impl.DefaultKeyProvider;
import net.rubyeye.xmemcached.impl.RandomMemcachedSessionLocaltor;
import net.rubyeye.xmemcached.transcoders.SerializingTranscoder;
import net.rubyeye.xmemcached.transcoders.Transcoder;
import net.rubyeye.xmemcached.utils.AddrUtil;
import net.rubyeye.xmemcached.utils.MemcachedServer;
import net.rubyeye.xmemcached.utils.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.code.yanf4j.config.Configuration;
import com.google.code.yanf4j.core.SocketOption;
import com.google.code.yanf4j.core.impl.StandardSocketOption;

/**
 * Builder pattern.Configure XmemcachedClient's options,then build it
 *
 * @author dennis
 *
 */
public class XMemcachedClientBuilder implements MemcachedClientBuilder {

  private static final Logger log = LoggerFactory.getLogger(XMemcachedClientBuilder.class);

  protected MemcachedSessionLocator sessionLocator = new ArrayMemcachedSessionLocator();
  protected BufferAllocator bufferAllocator = new SimpleBufferAllocator();
  protected Configuration configuration = getDefaultConfiguration();
  protected List<MemcachedServer> mcServerList = new ArrayList<MemcachedServer>();

  protected long connectTimeout = MemcachedClient.DEFAULT_CONNECT_TIMEOUT;

  protected int connectionPoolSize = MemcachedClient.DEFAULT_CONNECTION_POOL_SIZE;

  @SuppressWarnings("unchecked")
  protected final Map<SocketOption, Object> socketOptions = getDefaultSocketOptions();

  protected List<MemcachedClientStateListener> stateListeners =
      new ArrayList<MemcachedClientStateListener>();

  protected String name;

  protected boolean failureMode;

  protected boolean sanitizeKeys;

  protected KeyProvider keyProvider = DefaultKeyProvider.INSTANCE;

  protected int maxQueuedNoReplyOperations = MemcachedClient.DEFAULT_MAX_QUEUED_NOPS;

  protected long healSessionInterval = MemcachedClient.DEFAULT_HEAL_SESSION_INTERVAL;

  protected boolean enableHealSession = true;

  protected long opTimeout = MemcachedClient.DEFAULT_OP_TIMEOUT;

  public long getOpTimeout() {
    return opTimeout;
  }

  public void setOpTimeout(long opTimeout) {
    if (opTimeout <= 0)
      throw new IllegalArgumentException("Invalid opTimeout value:" + opTimeout);
    this.opTimeout = opTimeout;
  }

  public int getMaxQueuedNoReplyOperations() {
    return maxQueuedNoReplyOperations;
  }

  public long getHealSessionInterval() {
    return healSessionInterval;
  }

  public void setHealSessionInterval(long healSessionInterval) {
    this.healSessionInterval = healSessionInterval;
  }

  public boolean isEnableHealSession() {
    return enableHealSession;
  }

  public void setEnableHealSession(boolean enableHealSession) {
    this.enableHealSession = enableHealSession;
  }

  /**
   * Set max queued noreply operations number
   *
   * @see MemcachedClient#DEFAULT_MAX_QUEUED_NOPS
   * @param maxQueuedNoReplyOperations
   * @since 1.3.8
   */
  public void setMaxQueuedNoReplyOperations(int maxQueuedNoReplyOperations) {
    this.maxQueuedNoReplyOperations = maxQueuedNoReplyOperations;
  }

  public void setSanitizeKeys(boolean sanitizeKeys) {
    this.sanitizeKeys = sanitizeKeys;
  }

  public void addStateListener(MemcachedClientStateListener stateListener) {
    this.stateListeners.add(stateListener);
  }

  @SuppressWarnings("unchecked")
  public void setSocketOption(SocketOption socketOption, Object value) {
    if (socketOption == null) {
      throw new NullPointerException("Null socketOption");
    }
    if (value == null) {
      throw new NullPointerException("Null value");
    }
    if (!socketOption.type().equals(value.getClass())) {
      throw new IllegalArgumentException("Expected " + socketOption.type().getSimpleName()
          + " value,but givend " + value.getClass().getSimpleName());
    }
    this.socketOptions.put(socketOption, value);
  }

  @SuppressWarnings("unchecked")
  public Map<SocketOption, Object> getSocketOptions() {
    return this.socketOptions;
  }

  public final void setConnectionPoolSize(int poolSize) {
    if (this.connectionPoolSize <= 0) {
      throw new IllegalArgumentException("poolSize<=0");
    }
    this.connectionPoolSize = poolSize;
  }

  public void removeStateListener(MemcachedClientStateListener stateListener) {
    this.stateListeners.remove(stateListener);
  }

  public long getConnectTimeout() {
    return connectTimeout;
  }

  public void setConnectTimeout(long connectTimeout) {
    this.connectTimeout = connectTimeout;
  }

  public void setStateListeners(List<MemcachedClientStateListener> stateListeners) {
    if (stateListeners == null) {
      throw new IllegalArgumentException("Null state listeners");
    }
    this.stateListeners = stateListeners;
  }

  protected CommandFactory commandFactory = new TextCommandFactory();

  @SuppressWarnings("unchecked")
  public static final Map<SocketOption, Object> getDefaultSocketOptions() {
    Map<SocketOption, Object> map = new HashMap<SocketOption, Object>();
    map.put(StandardSocketOption.TCP_NODELAY, MemcachedClient.DEFAULT_TCP_NO_DELAY);
    map.put(StandardSocketOption.SO_RCVBUF, MemcachedClient.DEFAULT_TCP_RECV_BUFF_SIZE);
    map.put(StandardSocketOption.SO_KEEPALIVE, MemcachedClient.DEFAULT_TCP_KEEPLIVE);
    map.put(StandardSocketOption.SO_SNDBUF, MemcachedClient.DEFAULT_TCP_SEND_BUFF_SIZE);
    map.put(StandardSocketOption.SO_LINGER, 0);
    map.put(StandardSocketOption.SO_REUSEADDR, true);
    return map;
  }

  public static final Configuration getDefaultConfiguration() {
    final Configuration configuration = new Configuration();
    configuration.setSessionReadBufferSize(MemcachedClient.DEFAULT_SESSION_READ_BUFF_SIZE);
    configuration.setReadThreadCount(MemcachedClient.DEFAULT_READ_THREAD_COUNT);
    configuration.setSessionIdleTimeout(MemcachedClient.DEFAULT_SESSION_IDLE_TIMEOUT);
    configuration.setWriteThreadCount(0);
    return configuration;
  }

  public boolean isFailureMode() {
    return this.failureMode;
  }

  public void setFailureMode(boolean failureMode) {
    this.failureMode = failureMode;
  }

  public final CommandFactory getCommandFactory() {
    return this.commandFactory;
  }

  public final void setCommandFactory(CommandFactory commandFactory) {
    this.commandFactory = commandFactory;
  }

  @SuppressWarnings({"rawtypes"})
  protected Transcoder transcoder = new SerializingTranscoder();

  /**
   * Create a XMemcachedClientBuilder for a list of memcached servers. The string
   * needs to be formatted as a space spearated list of hostname:port entries,
   * e.g., "my.server.co:11211 my.other.server.co:11211"
   *
   * @deprecated Use {@link #createFromAddressList(List<InetSocketAddress> addressList)}
   *             instead. The string can be converted into an address list with
   *             {@link AddrUtil#getAddresses(String addressList)}.
   */
  @Deprecated
  public XMemcachedClientBuilder(String addressList) {
    this(AddrUtil.getAddresses(addressList));
  }

  /**
   * Create a XMemcachedClientBuilder for a list of memcached servers.
   *
   * @deprecated Use {@link #createFromAddressList(List<InetSocketAddress> addressList)}
   *             instead.
   */
  @Deprecated
  public XMemcachedClientBuilder(List<InetSocketAddress> addressList) {
    if (addressList == null || addressList.size() < 1) {
      throw new IllegalArgumentException(
          "There needs to be at least one server in the address list");
    }
    for (InetSocketAddress addr : addressList) {
      this.mcServerList.add(new MemcachedServer(addr));
    }
  }

  /**
   * Create a XMemcachedClientBuilder for a list of memcached servers and their weights.
   *
   * @deprecated Use {@link #createFromServerList(List<MemcachedServer> serverList)}
   *             instead.
   */
  @Deprecated
  public XMemcachedClientBuilder(List<InetSocketAddress> addressList, int[] weights) {
    if (weights == null) {
      throw new IllegalArgumentException("Weights cannot be null");
    }
    if (addressList == null || addressList.size() < 1) {
      throw new IllegalArgumentException(
          "There needs to be at least one server in the address list");
    }
    if (addressList.size() > weights.length) {
      throw new IllegalArgumentException(
          "Weights Array's length needs to match or exeed number of servers");
    }

    int i = 0;
    for (InetSocketAddress addr : addressList) {
      this.mcServerList.add(new MemcachedServer(addr, weights[i]));
      ++i;
    }
  }

  /**
   * @deprecated Use {@link #createFromServerList(List<MemcachedServer> serverList)}
   *             instead.
   */
  @Deprecated
  public XMemcachedClientBuilder(Map<InetSocketAddress, InetSocketAddress> addressMap) {
    if (addressMap == null || addressMap.size() < 1) {
      throw new IllegalArgumentException(
          "There needs to be at least one server in the address map");
    }
    for (Map.Entry<InetSocketAddress, InetSocketAddress> entry : addressMap.entrySet()) {
      this.mcServerList.add(new MemcachedServer(entry.getKey(), entry.getValue()));
    }
  }

  /**
   * @deprecated Use {@link #createFromServerList(List<MemcachedServer> serverList)}
   *             instead.
   */
  @Deprecated
  public XMemcachedClientBuilder(Map<InetSocketAddress, InetSocketAddress> addressMap,
      int[] weights) {
    if (weights == null) {
      throw new IllegalArgumentException("Weights cannot be null");
    }
    if (addressMap == null || addressMap.size() < 1) {
      throw new IllegalArgumentException(
          "There needs to be at least one server in the address map");
    }
    if (addressMap.size() > weights.length) {
      throw new IllegalArgumentException(
          "Weights Array's length needs to match or exeed number of servers");
    }

    int i = 0;
    for (Map.Entry<InetSocketAddress, InetSocketAddress> entry : addressMap.entrySet()) {
      this.mcServerList.add(new MemcachedServer(entry.getKey(), entry.getValue(), weights[i]));
      ++i;
    }
  }

  /**
   * @deprecated Will be made private in future release. You should only create
   *             a XMemcachedClientBuilder with actual server.
   */
  @Deprecated
  public XMemcachedClientBuilder() {
    this.mcServerList = null;
  }

  /**
   * Create a XMemcachedClientBuilder for a list of memcached servers.
   */
  public static XMemcachedClientBuilder createFromServerList(List<MemcachedServer> serverList) {
    if (serverList == null || serverList.size() < 1) {
      throw new IllegalArgumentException(
          "There needs to be at least one server in the server list");
    }
    XMemcachedClientBuilder builder = new XMemcachedClientBuilder();
    builder.setMemcachedServerList(serverList);
    return builder;
  }

  /**
   * Create a XMemcachedClientBuilder for a list of server addresses.
   */
  public static XMemcachedClientBuilder createFromAddressList(List<InetSocketAddress> addressList) {
    if (addressList == null || addressList.size() < 1) {
      throw new IllegalArgumentException(
          "There needs to be at least one address in the address list");
    }
    XMemcachedClientBuilder builder = new XMemcachedClientBuilder(addressList);
    return builder;
  }

  private void setMemcachedServerList(List<MemcachedServer> serverList) {
    this.mcServerList = serverList;
  }

  /*
   * (non-Javadoc)
   *
   * @see net.rubyeye.xmemcached.MemcachedClientBuilder#getSessionLocator()
   */
  public MemcachedSessionLocator getSessionLocator() {
    return this.sessionLocator;
  }

  /*
   * (non-Javadoc)
   *
   * @see net.rubyeye.xmemcached.MemcachedClientBuilder#setSessionLocator(net. rubyeye
   * .xmemcached.MemcachedSessionLocator)
   */
  public void setSessionLocator(MemcachedSessionLocator sessionLocator) {
    if (sessionLocator == null) {
      throw new IllegalArgumentException("Null SessionLocator");
    }
    this.sessionLocator = sessionLocator;
  }

  /*
   * (non-Javadoc)
   *
   * @see net.rubyeye.xmemcached.MemcachedClientBuilder#getBufferAllocator()
   */
  public BufferAllocator getBufferAllocator() {
    return this.bufferAllocator;
  }

  /*
   * (non-Javadoc)
   *
   * @see net.rubyeye.xmemcached.MemcachedClientBuilder#setBufferAllocator(net.
   * rubyeye.xmemcached.buffer.BufferAllocator)
   */
  public void setBufferAllocator(BufferAllocator bufferAllocator) {
    if (bufferAllocator == null) {
      throw new IllegalArgumentException("Null bufferAllocator");
    }
    this.bufferAllocator = bufferAllocator;
  }

  /*
   * (non-Javadoc)
   *
   * @see net.rubyeye.xmemcached.MemcachedClientBuilder#getConfiguration()
   */
  public Configuration getConfiguration() {
    return this.configuration;
  }

  /*
   * (non-Javadoc)
   *
   * @see net.rubyeye.xmemcached.MemcachedClientBuilder#setConfiguration(com.google
   * .code.yanf4j.config.Configuration)
   */
  public void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }

  /*
   * (non-Javadoc)
   *
   * @see net.rubyeye.xmemcached.MemcachedClientBuilder#build()
   */
  public MemcachedClient build() throws IOException {
    XMemcachedClient memcachedClient;
    // kestrel protocol use random session locator.
    if (this.commandFactory.getProtocol() == Protocol.Kestrel
        && !(this.sessionLocator instanceof RandomMemcachedSessionLocaltor)) {
      log.warn(
          "Recommend to use `net.rubyeye.xmemcached.impl.RandomMemcachedSessionLocaltor` as session locator for kestrel protocol.");
    }

    memcachedClient = new XMemcachedClient(this.sessionLocator, this.bufferAllocator,
        this.configuration, this.socketOptions, this.commandFactory, this.transcoder,
        this.mcServerList, this.stateListeners, this.connectionPoolSize, this.connectTimeout,
        this.name, this.failureMode);

    this.configureClient(memcachedClient);
    return memcachedClient;
  }

  protected void configureClient(XMemcachedClient memcachedClient) {
    if (this.commandFactory.getProtocol() == Protocol.Kestrel) {
      memcachedClient.setOptimizeGet(false);
    }
    memcachedClient.setConnectTimeout(connectTimeout);
    memcachedClient.setSanitizeKeys(sanitizeKeys);
    memcachedClient.setKeyProvider(this.keyProvider);
    memcachedClient.setOpTimeout(this.opTimeout);
    memcachedClient.setHealSessionInterval(this.healSessionInterval);
    memcachedClient.setEnableHealSession(this.enableHealSession);
    memcachedClient.setMaxQueuedNoReplyOperations(this.maxQueuedNoReplyOperations);
  }

  @SuppressWarnings("rawtypes")
  public Transcoder getTranscoder() {
    return this.transcoder;
  }

  /*
   * (non-Javadoc)
   *
   * @see net.rubyeye.xmemcached.MemcachedClientBuilder#setTranscoder(transcoder)
   */
  public void setTranscoder(Transcoder transcoder) {
    if (transcoder == null) {
      throw new IllegalArgumentException("Null Transcoder");
    }
    this.transcoder = transcoder;
  }

  public Map<InetSocketAddress, AuthInfo> getAuthInfoMap() {
    Map<InetSocketAddress, AuthInfo> authInfoMap = new HashMap<InetSocketAddress, AuthInfo>();
    for (MemcachedServer server : this.mcServerList) {
      authInfoMap.put(server.getMainAddress(), server.getAuthInfo());
    }
    return authInfoMap;
  }

  /*
   * (non-Javadoc)
   *
   * @see net.rubyeye.xmemcached.MemcachedClientBuilder#setKeyProvider()
   */
  public void setKeyProvider(KeyProvider keyProvider) {
    if (keyProvider == null)
      throw new IllegalArgumentException("null key provider");
    this.keyProvider = keyProvider;
  }

  /*
   * (non-Javadoc)
   *
   * @see net.rubyeye.xmemcached.MemcachedClientBuilder#addAuthInfo()
   */
  public void addAuthInfo(InetSocketAddress address, AuthInfo authInfo) {
    for (MemcachedServer server : this.mcServerList) {
      if ((AddrUtil.getServerString(server.getMainAddress()))
          .equals(AddrUtil.getServerString(address))) {
        server.setAuthInfo(authInfo);
      }
    }
  }

  public void removeAuthInfo(InetSocketAddress address) {
    for (MemcachedServer server : this.mcServerList) {
      if ((AddrUtil.getServerString(server.getMainAddress()))
          .equals(AddrUtil.getServerString(address))) {
        server.setAuthInfo(null);
      }
    }
  }

  public void setAuthInfoMap(Map<InetSocketAddress, AuthInfo> authInfoMap) {
    for (MemcachedServer server : this.mcServerList) {
      server.setAuthInfo(authInfoMap.get(server.getMainAddress()));
    }
  }

  public String getName() {
    return this.name;
  }

  /*
   * (non-Javadoc)
   *
   * @see net.rubyeye.xmemcached.MemcachedClientBuilder#setName()
   */
  public void setName(String name) {
    this.name = name;

  }

  public void setSelectorPoolSize(int selectorPoolSize) {
    getConfiguration().setSelectorPoolSize(selectorPoolSize);
  }

}
