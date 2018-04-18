/**
 * Copyright [2009-2010] [dennis zhuang(killme2008@gmail.com)] Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance with the License. You
 * may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License
 */
package net.rubyeye.xmemcached.utils;

import java.net.InetSocketAddress;
import net.rubyeye.xmemcached.auth.AuthInfo;

/**
 * A MemcachedServer represents a server node. It encapsulates its main address,
 * its standby address (if it exists), its weight (default is 1), and its
 * AuthInfo (if needed).
 */
public class MemcachedServer {
  private final InetSocketAddress mainAddr;
  private final InetSocketAddress standbyAddr;
  private final int weight;
  private AuthInfo authInfo;

  public MemcachedServer(InetSocketAddress mainAddr) {
    this(mainAddr, null, 1);
  }

  public MemcachedServer(InetSocketAddress mainAddr, int weight) {
    this(mainAddr, null, weight);
  }

  public MemcachedServer(InetSocketAddress mainAddr, InetSocketAddress standbyAddr) {
    this(mainAddr, standbyAddr, 1);
  }

  public MemcachedServer(InetSocketAddress mainAddr, InetSocketAddress standbyAddr, int weight) {
    if (weight <= 0) {
      throw new IllegalArgumentException("Weight cannot be <= 0");
    }
    this.mainAddr = mainAddr;
    this.standbyAddr = standbyAddr;
    this.weight = weight;
  }

  public void setAuthInfo(AuthInfo authInfo) {

  }

  public AuthInfo getAuthInfo() {
    return this.authInfo;
  }

  public InetSocketAddress getMainAddress() {
    return this.mainAddr;
  }

  public InetSocketAddress getStandbyAddress() {
    return this.standbyAddr;
  }

  public int getWeight() {
    return this.weight;
  }
}
