/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.network;

/*
 * Transport layer for PLAINTEXT communication
 */

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.SelectionKey;

import java.security.Principal;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * kafk的网络传输层
 */
public class PlaintextTransportLayer implements TransportLayer {
    private final SelectionKey key;
    private final SocketChannel socketChannel;
    private final Principal principal = KafkaPrincipal.ANONYMOUS;

    public PlaintextTransportLayer(SelectionKey key) throws IOException {
        this.key = key;
        this.socketChannel = (SocketChannel) key.channel();
    }

    @Override
    public boolean ready() {
        return true;
    }

    @Override
    public boolean finishConnect() throws IOException {
        //完成的最后的网络的连接
        boolean connected = socketChannel.finishConnect();
        if (connected)
            //取消了OP_CONNECT事件
            //增加了OP_READ 事件 我们这儿的这个key对应的KafkaChannel是不是就可以接受服务
            //端发送回来的响应了。
            key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
        return connected;
    }

    @Override
    public void disconnect() {
        key.cancel();
    }

    @Override
    public SocketChannel socketChannel() {
        return socketChannel;
    }

    @Override
    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    @Override
    public boolean isConnected() {
        return socketChannel.isConnected();
    }

    /**
     * Closes this channel
     *
     * @throws IOException If I/O error occurs
     */
    @Override
    public void close() throws IOException {
        try {
            socketChannel.socket().close();
            socketChannel.close();
        } finally {
            key.attach(null);
            key.cancel();
        }
    }

    /**
     * Performs SSL handshake hence is a no-op for the non-secure
     * implementation
     * @throws IOException
    */
    @Override
    public void handshake() throws IOException {}

    /**
    * Reads a sequence of bytes from this channel into the given buffer.
    *
    * @param dst The buffer into which bytes are to be transferred
    * @return The number of bytes read, possible zero or -1 if the channel has reached end-of-stream
    * @throws IOException if some other I/O error occurs
    */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        return socketChannel.read(dst);
    }

    /**
     * Reads a sequence of bytes from this channel into the given buffers.
     *
     * @param dsts - The buffers into which bytes are to be transferred.
     * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream.
     * @throws IOException if some other I/O error occurs
     */
    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return socketChannel.read(dsts);
    }

    /**
     * Reads a sequence of bytes from this channel into a subsequence of the given buffers.
     * @param dsts - The buffers into which bytes are to be transferred
     * @param offset - The offset within the buffer array of the first buffer into which bytes are to be transferred; must be non-negative and no larger than dsts.length.
     * @param length - The maximum number of buffers to be accessed; must be non-negative and no larger than dsts.length - offset
     * @returns The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream.
     * @throws IOException if some other I/O error occurs
     */
    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return socketChannel.read(dsts, offset, length);
    }

    /**
    * Writes a sequence of bytes to this channel from the given buffer.
    *
    * @param src The buffer from which bytes are to be retrieved
    * @returns The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream
    * @throws IOException If some other I/O error occurs
    */
    @Override
    public int write(ByteBuffer src) throws IOException {
        return socketChannel.write(src);
    }

    /**
    * Writes a sequence of bytes to this channel from the given buffer.
    *
    * @param srcs The buffer from which bytes are to be retrieved
    * @returns The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream
    * @throws IOException If some other I/O error occurs
    */
    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return socketChannel.write(srcs);
    }

    /**
    * Writes a sequence of bytes to this channel from the subsequence of the given buffers.
    *
    * @param srcs The buffers from which bytes are to be retrieved
    * @param offset The offset within the buffer array of the first buffer from which bytes are to be retrieved; must be non-negative and no larger than srcs.length.
    * @param length - The maximum number of buffers to be accessed; must be non-negative and no larger than srcs.length - offset.
    * @return returns no.of bytes written , possibly zero.
    * @throws IOException If some other I/O error occurs
    */
    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return socketChannel.write(srcs, offset, length);
    }

    /**
     * always returns false as there will be not be any
     * pending writes since we directly write to socketChannel.
     */
    @Override
    public boolean hasPendingWrites() {
        return false;
    }

    /**
     * Returns ANONYMOUS as Principal.
     */
    @Override
    public Principal peerPrincipal() throws IOException {
        return principal;
    }

    /**
     * Adds the interestOps to selectionKey.
     * @param ops
     */
    @Override
    public void addInterestOps(int ops) {
        key.interestOps(key.interestOps() | ops);

    }

    /**
     * Removes the interestOps from selectionKey.
     * @param ops
     */
    @Override
    public void removeInterestOps(int ops) {
        key.interestOps(key.interestOps() & ~ops);
    }

    @Override
    public boolean isMute() {
        return key.isValid() && (key.interestOps() & SelectionKey.OP_READ) == 0;
    }

    /**
     * 表示文件通道的字节会传入网络通道
     */
    @Override
    public long transferFrom(FileChannel fileChannel, long position, long count) throws IOException {
        return fileChannel.transferTo(position, count, socketChannel);
    }
}
