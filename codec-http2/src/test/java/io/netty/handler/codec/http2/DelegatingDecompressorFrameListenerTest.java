/*
 * Copyright 2025 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.netty.handler.codec.http2;

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.SnappyFrameEncoder;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class DelegatingDecompressorFrameListenerTest {

    private static final int STREAM_ID = 3;
    // SnappyFrameEncoder compresses its input in slices of Short.MAX_VALUE bytes, each one becoming a separate
    // chunk that SnappyFrameDecoder emits as its own decompressed buffer.
    private static final int CHUNK_SIZE = Short.MAX_VALUE;
    private static final int CHUNKS = 8;

    private TrackingAllocator allocator;
    private EmbeddedChannel channel;
    private ChannelHandlerContext ctx;
    private Http2Connection connection;
    private Http2LocalFlowController flowController;

    @BeforeEach
    public void setUp() throws Exception {
        allocator = new TrackingAllocator();
        channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        // The decompressor EmbeddedChannel shares the config (and so the allocator) of the parent channel.
        channel.config().setAllocator(allocator);
        ctx = channel.pipeline().firstContext();
        connection = new DefaultHttp2Connection(true);
        flowController = mock(Http2LocalFlowController.class);
        connection.local().flowController(flowController);
        connection.remote().createStream(STREAM_ID, false);
    }

    @AfterEach
    public void tearDown() {
        Http2Stream stream = connection.stream(STREAM_ID);
        if (stream != null) {
            stream.close();
        }
        channel.finishAndReleaseAll();
    }

    /**
     * A small, highly compressed DATA frame must not be fully inflated and buffered before it is handed to the
     * listener: each decompressed chunk has to be delivered (and released) before the next one is produced.
     */
    @ParameterizedTest
    @ValueSource(ints = { 0, 10 })
    public void decompressedDataIsStreamedToListenerInsteadOfBuffered(final int padding) throws Exception {
        final List<Integer> liveBuffersAtDelivery = new ArrayList<Integer>();
        final ByteBuf received = Unpooled.buffer();
        final boolean[] endOfStreamSeen = new boolean[1];
        DelegatingDecompressorFrameListener listener = new DelegatingDecompressorFrameListener(connection,
                new Http2FrameAdapter() {
                    @Override
                    public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding,
                                          boolean endOfStream) {
                        int processed = data.readableBytes() + padding;
                        if (data.isReadable()) {
                            liveBuffersAtDelivery.add(allocator.liveBuffers());
                            received.writeBytes(data);
                        }
                        if (endOfStream) {
                            endOfStreamSeen[0] = true;
                        }
                        return processed;
                    }
                });
        try {
            listener.onHeadersRead(ctx, STREAM_ID, snappyHeaders(), 0, false);

            ByteBuf compressed = snappyCompress(new byte[CHUNKS * CHUNK_SIZE]);
            final int compressedBytes = compressed.readableBytes();
            assertTrue(compressedBytes < CHUNK_SIZE, "payload must be highly compressed: " + compressedBytes);

            int returned;
            try {
                returned = listener.onDataRead(ctx, STREAM_ID, compressed, padding, true);
            } finally {
                compressed.release();
            }

            assertEquals(CHUNKS, liveBuffersAtDelivery.size());
            for (int live : liveBuffersAtDelivery) {
                // Only the chunk currently handed to the listener may be alive. Before the fix every chunk of the
                // frame was decompressed into the EmbeddedChannel's inbound queue before the first one was delivered.
                assertEquals(1, live, "decompressed buffers alive at delivery: " + liveBuffersAtDelivery);
            }
            assertEquals(0, allocator.liveBuffers());
            assertEquals(CHUNKS * CHUNK_SIZE, received.readableBytes());
            assertTrue(endOfStreamSeen[0]);

            // All compressed bytes (including padding) must be returned to the flow controller exactly once.
            ArgumentCaptor<Integer> consumed = ArgumentCaptor.forClass(Integer.class);
            verify(flowController, atLeastOnce()).consumeBytes(eq(connection.stream(STREAM_ID)), consumed.capture());
            int totalConsumed = returned;
            for (int bytes : consumed.getAllValues()) {
                totalConsumed += bytes;
            }
            assertEquals(compressedBytes + padding, totalConsumed);
        } finally {
            received.release();
        }
    }

    @Test
    public void decompressedBufferIsReleasedWhenListenerThrows() throws Exception {
        final Http2Exception failure = Http2Exception.streamError(STREAM_ID, Http2Error.INTERNAL_ERROR, "boom");
        final DelegatingDecompressorFrameListener listener = new DelegatingDecompressorFrameListener(connection,
                new Http2FrameAdapter() {
                    @Override
                    public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding,
                                          boolean endOfStream) throws Http2Exception {
                        if (data.isReadable()) {
                            throw failure;
                        }
                        return padding;
                    }
                });
        listener.onHeadersRead(ctx, STREAM_ID, snappyHeaders(), 0, false);

        final ByteBuf compressed = snappyCompress(new byte[CHUNKS * CHUNK_SIZE]);
        try {
            Http2Exception thrown = assertThrows(Http2Exception.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    listener.onDataRead(ctx, STREAM_ID, compressed, 0, false);
                }
            });
            assertSame(failure, thrown);
        } finally {
            compressed.release();
        }
        assertFalse(allocator.allocated.isEmpty());
        assertEquals(0, allocator.liveBuffers());
    }

    /**
     * Regression test for CVE-2026-56819. The per-stream decompressor {@link EmbeddedChannel} is closed as soon as
     * a {@code DATA} frame with {@code endOfStream} was processed, but it stays attached to the stream. Writing into
     * a closed {@link EmbeddedChannel} only fails <em>after</em> the passed in buffer was retained and the channel
     * will not release it in that case, so a remote peer was able to leak one reference of the received (direct)
     * buffer per {@code DATA} frame and so exhaust the memory of the JVM.
     */
    @Test
    public void dataFramesAfterEndOfStreamDoNotLeak() throws Exception {
        DelegatingDecompressorFrameListener listener =
                new DelegatingDecompressorFrameListener(connection, new Http2FrameAdapter());
        listener.onHeadersRead(ctx, STREAM_ID, gzipHeaders(), 0, false);

        // Finishes (closes) the decompressor while it is still attached to the stream.
        ByteBuf endOfStreamData = Unpooled.buffer();
        try {
            listener.onDataRead(ctx, STREAM_ID, endOfStreamData, 0, true);
        } finally {
            if (endOfStreamData.refCnt() > 0) {
                endOfStreamData.release();
            }
        }

        assertDataFramesForClosedDecompressorDoNotLeak(listener);
    }

    /**
     * Regression test for CVE-2026-56819. Same as {@link #dataFramesAfterEndOfStreamDoNotLeak()}, but the
     * decompressor is closed by the {@code onStreamRemoved -> Http2Decompressor.cleanup()} path (which calls
     * {@link EmbeddedChannel#finishAndReleaseAll()}) while {@code DATA} frames for the stream still reach it.
     */
    @Test
    public void dataFramesAfterDecompressorCleanupDoNotLeak() throws Exception {
        Http2Connection spiedConnection = spy(connection);
        DelegatingDecompressorFrameListener listener =
                new DelegatingDecompressorFrameListener(spiedConnection, new Http2FrameAdapter());
        ArgumentCaptor<Http2Connection.Listener> connectionListener =
                ArgumentCaptor.forClass(Http2Connection.Listener.class);
        verify(spiedConnection).addListener(connectionListener.capture());
        listener.onHeadersRead(ctx, STREAM_ID, gzipHeaders(), 0, false);

        // A first (empty) DATA frame binds the stream to the decompressor.
        ByteBuf firstData = Unpooled.buffer();
        try {
            assertEquals(0, listener.onDataRead(ctx, STREAM_ID, firstData, 0, false));
        } finally {
            if (firstData.refCnt() > 0) {
                firstData.release();
            }
        }

        // Closes the decompressor via cleanup() while it is still attached to the stream.
        connectionListener.getValue().onStreamRemoved(connection.stream(STREAM_ID));

        assertDataFramesForClosedDecompressorDoNotLeak(listener);
    }

    private void assertDataFramesForClosedDecompressorDoNotLeak(DelegatingDecompressorFrameListener listener)
            throws Exception {
        // Replay the same direct buffer as if it were received via a flood of DATA frames for the stream whose
        // decompressor is already closed. Each of them must fail the stream without taking ownership of the
        // buffer, so its reference count must not grow. Without the fix it climbs by one for every frame.
        ByteBuf data = Unpooled.directBuffer().writeZero(16);
        try {
            for (int i = 0; i < 8; i++) {
                try {
                    listener.onDataRead(ctx, STREAM_ID, data, 0, false);
                    fail("DATA frame " + i + " for a closed decompressor must fail the stream");
                } catch (Http2Exception.StreamException e) {
                    assertEquals(STREAM_ID, e.streamId());
                    assertEquals(Http2Error.INTERNAL_ERROR, e.error());
                    assertTrue(e.getCause() instanceof ClosedChannelException, String.valueOf(e.getCause()));
                }
                assertEquals(1, data.refCnt(), "buffer was retained by DATA frame " + i + " and never released");
            }
        } finally {
            data.release();
        }
    }

    private static Http2Headers gzipHeaders() {
        return new DefaultHttp2Headers().set(HttpHeaderNames.CONTENT_ENCODING, HttpHeaderValues.GZIP);
    }

    private static Http2Headers snappyHeaders() {
        return new DefaultHttp2Headers().set(HttpHeaderNames.CONTENT_ENCODING, HttpHeaderValues.SNAPPY);
    }

    private static ByteBuf snappyCompress(byte[] data) {
        EmbeddedChannel encoder = new EmbeddedChannel(new SnappyFrameEncoder());
        try {
            assertTrue(encoder.writeOutbound(Unpooled.wrappedBuffer(data)));
            ByteBuf compressed = Unpooled.buffer();
            for (;;) {
                ByteBuf buf = encoder.readOutbound();
                if (buf == null) {
                    break;
                }
                compressed.writeBytes(buf);
                buf.release();
            }
            return compressed;
        } finally {
            encoder.finishAndReleaseAll();
        }
    }

    private static final class TrackingAllocator extends AbstractByteBufAllocator {
        final List<ByteBuf> allocated = new ArrayList<ByteBuf>();

        TrackingAllocator() {
            super(false);
        }

        @Override
        protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
            ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.heapBuffer(initialCapacity, maxCapacity);
            allocated.add(buf);
            return buf;
        }

        @Override
        protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
            ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.directBuffer(initialCapacity, maxCapacity);
            allocated.add(buf);
            return buf;
        }

        @Override
        public boolean isDirectBufferPooled() {
            return false;
        }

        int liveBuffers() {
            int live = 0;
            for (ByteBuf buf : allocated) {
                if (buf.refCnt() > 0) {
                    live++;
                }
            }
            return live;
        }
    }
}
