/*
 * Copyright 2019 The Netty Project
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.DefaultMessageSizeEstimator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayDeque;
import java.util.Queue;

import static io.netty.handler.codec.http2.Http2CodecUtil.DEFAULT_MAX_FRAME_SIZE;
import static io.netty.handler.codec.http2.Http2Error.CANCEL;
import static io.netty.handler.codec.http2.Http2Error.ENHANCE_YOUR_CALM;
import static io.netty.handler.codec.http2.Http2Error.NO_ERROR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link Http2MaxRstFrameLimitEncoder}.
 */
public class Http2MaxRstFrameLimitEncoderTest {

    private Http2MaxRstFrameLimitEncoder encoder;

    @Mock
    private Http2FrameWriter writer;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private Channel channel;

    @Mock
    private Channel.Unsafe unsafe;

    @Mock
    private ChannelConfig config;

    @Mock
    private EventExecutor executor;

    private final Queue<ChannelPromise> goAwayPromises = new ArrayDeque<ChannelPromise>();

    /**
     * Init fields and do mocking.
     */
    @BeforeEach
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        Http2FrameWriter.Configuration configuration = mock(Http2FrameWriter.Configuration.class);
        Http2FrameSizePolicy frameSizePolicy = mock(Http2FrameSizePolicy.class);
        when(writer.configuration()).thenReturn(configuration);
        when(configuration.frameSizePolicy()).thenReturn(frameSizePolicy);
        when(frameSizePolicy.maxFrameSize()).thenReturn(DEFAULT_MAX_FRAME_SIZE);

        when(writer.writeRstStream(eq(ctx), anyInt(), anyLong(), any(ChannelPromise.class)))
                .thenAnswer(new Answer<ChannelFuture>() {
                    @Override
                    public ChannelFuture answer(InvocationOnMock invocationOnMock) {
                        return handlePromise(invocationOnMock, 3);
                    }
                });
        when(writer.writeGoAway(any(ChannelHandlerContext.class), anyInt(), anyLong(), any(ByteBuf.class),
                any(ChannelPromise.class))).thenAnswer(new Answer<ChannelFuture>() {
            @Override
            public ChannelFuture answer(InvocationOnMock invocationOnMock) {
                ReferenceCountUtil.release(invocationOnMock.getArgument(3));
                ChannelPromise promise = invocationOnMock.getArgument(4);
                goAwayPromises.offer(promise);
                return promise;
            }
        });
        Http2Connection connection = new DefaultHttp2Connection(false);
        connection.remote().flowController(new DefaultHttp2RemoteFlowController(connection));
        connection.local().flowController(new DefaultHttp2LocalFlowController(connection).frameWriter(writer));

        DefaultHttp2ConnectionEncoder defaultEncoder =
                new DefaultHttp2ConnectionEncoder(connection, writer);
        encoder = new Http2MaxRstFrameLimitEncoder(defaultEncoder, 2, 1);
        DefaultHttp2ConnectionDecoder decoder =
                new DefaultHttp2ConnectionDecoder(connection, encoder, mock(Http2FrameReader.class));
        Http2ConnectionHandler handler = new Http2ConnectionHandlerBuilder()
                .frameListener(mock(Http2FrameListener.class))
                .codec(decoder, encoder).build();

        // Set LifeCycleManager on encoder and decoder
        when(ctx.channel()).thenReturn(channel);
        when(ctx.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);
        when(channel.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);
        when(executor.inEventLoop()).thenReturn(true);
        doAnswer(new Answer<ChannelPromise>() {
            @Override
            public ChannelPromise answer(InvocationOnMock invocation) throws Throwable {
                return newPromise();
            }
        }).when(ctx).newPromise();
        when(ctx.executor()).thenReturn(executor);
        when(channel.isActive()).thenReturn(false);
        when(channel.config()).thenReturn(config);
        when(channel.isWritable()).thenReturn(true);
        when(channel.bytesBeforeUnwritable()).thenReturn(Long.MAX_VALUE);
        when(config.getWriteBufferHighWaterMark()).thenReturn(Integer.MAX_VALUE);
        when(config.getMessageSizeEstimator()).thenReturn(DefaultMessageSizeEstimator.DEFAULT);
        ChannelMetadata metadata = new ChannelMetadata(false, 16);
        when(channel.metadata()).thenReturn(metadata);
        when(channel.unsafe()).thenReturn(unsafe);
        handler.handlerAdded(ctx);
    }

    private ChannelPromise handlePromise(InvocationOnMock invocationOnMock, int promiseIdx) {
        ChannelPromise promise = invocationOnMock.getArgument(promiseIdx);
        return promise.setSuccess();
    }

    @AfterEach
    public void teardown() {
        // Close and release any buffered frames.
        encoder.close();

        // Notify all goAway ChannelPromise instances now as these will also release the retained ByteBuf for the
        // debugData.
        for (;;) {
            ChannelPromise promise = goAwayPromises.poll();
            if (promise == null) {
                break;
            }
            promise.setSuccess();
        }
    }

    @ParameterizedTest
    @EnumSource(Http2Error.class)
    public void testLimitRst(Http2Error error) {
        assertTrue(encoder.writeRstStream(ctx, 1, error.code(), newPromise()).isSuccess());
        assertTrue(encoder.writeRstStream(ctx, 1, error.code(), newPromise()).isSuccess());
        verifyFlushAndClose(0, false);
        assertTrue(encoder.writeRstStream(ctx, 1, error.code(), newPromise()).isSuccess());
        if (error == CANCEL || error == NO_ERROR) {
            // CANCEL and NO_ERROR are ignored as these will not be caused by a stream error.
            verifyFlushAndClose(0, false);
        } else {
            verifyFlushAndClose(1, true);
        }
    }

    @ParameterizedTest
    @EnumSource(Http2Error.class)
    public void testLimitRstReset(Http2Error error) throws Exception {
        assertTrue(encoder.writeRstStream(ctx, 1, error.code(), newPromise()).isSuccess());
        assertTrue(encoder.writeRstStream(ctx, 1, error.code(), newPromise()).isSuccess());
        verifyFlushAndClose(0, false);
        Thread.sleep(1000);
        assertTrue(encoder.writeRstStream(ctx, 1, error.code(), newPromise()).isSuccess());
        verifyFlushAndClose(0, false);
    }

    private void verifyFlushAndClose(int invocations, boolean failed) {
        verify(ctx, atLeast(invocations)).flush();
        verify(ctx, times(invocations)).close();
        if (failed) {
            verify(writer, times(1)).writeGoAway(eq(ctx), eq(Integer.MAX_VALUE), eq(ENHANCE_YOUR_CALM.code()),
                    any(ByteBuf.class), any(ChannelPromise.class));
        }
    }

    private ChannelPromise newPromise() {
        return new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
    }

    // Same value as AbstractHttp2ConnectionHandlerBuilder.DEFAULT_MAX_RST_FRAMES_PER_CONNECTION_FOR_SERVER.
    private static final int DEFAULT_MAX_RST_FRAMES_FOR_SERVER = 200;

    // Frame sequences a client can send to make the server generate RST_STREAM frames ("MadeYouReset").
    private static final int ZERO_WINDOW_UPDATE_ON_OPEN_STREAM = 0;
    private static final int DATA_ON_HALF_CLOSED_STREAM = 1;
    private static final int SELF_DEPENDENT_PRIORITY = 2;

    @Test
    public void testServerGeneratedRstFloodViaZeroWindowUpdateClosesConnection() {
        Http2ConnectionHandler handler = new Http2ConnectionHandlerBuilder()
                .server(true).frameListener(new Http2FrameAdapter()).build();
        assertServerClosesConnectionOnRstFlood(handler, ZERO_WINDOW_UPDATE_ON_OPEN_STREAM,
                DEFAULT_MAX_RST_FRAMES_FOR_SERVER);
    }

    @Test
    public void testServerGeneratedRstFloodViaDataOnHalfClosedStreamClosesConnection() {
        Http2ConnectionHandler handler = new Http2ConnectionHandlerBuilder()
                .server(true).frameListener(new Http2FrameAdapter()).build();
        assertServerClosesConnectionOnRstFlood(handler, DATA_ON_HALF_CLOSED_STREAM,
                DEFAULT_MAX_RST_FRAMES_FOR_SERVER);
    }

    @Test
    public void testServerGeneratedRstFloodViaSelfDependentPriorityClosesConnection() {
        Http2ConnectionHandler handler = new Http2ConnectionHandlerBuilder()
                .server(true).frameListener(new Http2FrameAdapter())
                .encoderEnforceMaxRstFramesPerWindow(10, 30).build();
        assertServerClosesConnectionOnRstFlood(handler, SELF_DEPENDENT_PRIORITY, 10);
    }

    @Test
    public void testFrameCodecServerGeneratedRstFloodClosesConnection() {
        Http2FrameCodec codec = Http2FrameCodecBuilder.forServer()
                .encoderEnforceMaxRstFramesPerWindow(10, 30).build();
        assertServerClosesConnectionOnRstFlood(codec, SELF_DEPENDENT_PRIORITY, 10);
    }

    @Test
    public void testServerGeneratedRstLimitCanBeDisabled() {
        Http2ConnectionHandler handler = new Http2ConnectionHandlerBuilder()
                .server(true).frameListener(new Http2FrameAdapter())
                .encoderEnforceMaxRstFramesPerWindow(0, 0).build();
        EmbeddedChannel ch = new EmbeddedChannel(handler);
        ByteBuf received = Unpooled.buffer();
        try {
            int attacks = DEFAULT_MAX_RST_FRAMES_FOR_SERVER + 50;
            writeClientPreface(ch);
            for (int i = 0; i < attacks; i++) {
                ch.writeInbound(newAttack(ZERO_WINDOW_UPDATE_ON_OPEN_STREAM, 2 * i + 1));
            }
            assertTrue(ch.isOpen());
            drainOutbound(ch, received);
            long[] result = countRstAndGoAway(received);
            assertEquals(attacks, result[0]);
            assertEquals(-1, result[1]);
        } finally {
            received.release();
            ch.finishAndReleaseAll();
        }
    }

    private static void assertServerClosesConnectionOnRstFlood(ChannelHandler handler, int attack, int maxRstFrames) {
        EmbeddedChannel ch = new EmbeddedChannel(handler);
        ByteBuf received = Unpooled.buffer();
        try {
            writeClientPreface(ch);
            // Each attack makes the server answer with exactly one RST_STREAM frame. Without the encoder side
            // limit the server would keep resetting streams forever and never close the connection.
            int attacks = 0;
            while (ch.isOpen() && attacks < maxRstFrames * 2) {
                ch.writeInbound(newAttack(attack, 2 * attacks + 1));
                attacks++;
            }
            drainOutbound(ch, received);
            long[] result = countRstAndGoAway(received);
            assertFalse(ch.isOpen(), "connection must be closed after " + attacks + " server generated RST frames");
            assertEquals(maxRstFrames + 1, result[0]);
            assertEquals(ENHANCE_YOUR_CALM.code(), result[1]);
        } finally {
            received.release();
            ch.finishAndReleaseAll();
        }
    }

    private static void writeClientPreface(EmbeddedChannel ch) {
        ch.writeInbound(Http2CodecUtil.connectionPrefaceBuf());
        ByteBuf settings = Unpooled.buffer();
        writeFrameHeader(settings, 0, Http2FrameTypes.SETTINGS, 0, 0);
        ch.writeInbound(settings);
    }

    private static ByteBuf newAttack(int attack, int streamId) {
        ByteBuf buf = Unpooled.buffer();
        switch (attack) {
            case ZERO_WINDOW_UPDATE_ON_OPEN_STREAM:
                // Open the stream, then send a WINDOW_UPDATE with an increment of 0 (stream error PROTOCOL_ERROR).
                writeRequestHeaders(buf, streamId, false);
                writeFrameHeader(buf, 4, Http2FrameTypes.WINDOW_UPDATE, 0, streamId);
                buf.writeInt(0);
                break;
            case DATA_ON_HALF_CLOSED_STREAM:
                // Half-close the stream, then send DATA on it (stream error STREAM_CLOSED).
                writeRequestHeaders(buf, streamId, true);
                writeFrameHeader(buf, 0, Http2FrameTypes.DATA, 0, streamId);
                break;
            case SELF_DEPENDENT_PRIORITY:
                // PRIORITY frame where the stream depends on itself (stream error PROTOCOL_ERROR).
                writeFrameHeader(buf, 5, Http2FrameTypes.PRIORITY, 0, streamId);
                buf.writeInt(streamId);
                buf.writeByte(15);
                break;
            default:
                throw new IllegalArgumentException("Unknown attack: " + attack);
        }
        return buf;
    }

    private static void writeRequestHeaders(ByteBuf buf, int streamId, boolean endStream) {
        // HPACK static table entries: :method GET, :scheme http, :path /
        int flags = Http2Flags.END_HEADERS | (endStream ? Http2Flags.END_STREAM : 0);
        writeFrameHeader(buf, 3, Http2FrameTypes.HEADERS, flags, streamId);
        buf.writeByte(0x82);
        buf.writeByte(0x86);
        buf.writeByte(0x84);
    }

    private static void writeFrameHeader(ByteBuf buf, int length, byte type, int flags, int streamId) {
        buf.writeMedium(length);
        buf.writeByte(type);
        buf.writeByte(flags);
        buf.writeInt(streamId);
    }

    private static void drainOutbound(EmbeddedChannel ch, ByteBuf received) {
        for (;;) {
            ByteBuf buf = ch.readOutbound();
            if (buf == null) {
                break;
            }
            received.writeBytes(buf);
            buf.release();
        }
    }

    /**
     * Returns the number of RST_STREAM frames and the error code of the last GOAWAY frame (or {@code -1}).
     */
    private static long[] countRstAndGoAway(ByteBuf received) {
        long rstFrames = 0;
        long goAwayErrorCode = -1;
        while (received.readableBytes() >= Http2CodecUtil.FRAME_HEADER_LENGTH) {
            int length = received.readUnsignedMedium();
            byte type = received.readByte();
            received.skipBytes(5); // flags + stream id
            ByteBuf payload = received.readSlice(length);
            if (type == Http2FrameTypes.RST_STREAM) {
                rstFrames++;
            } else if (type == Http2FrameTypes.GO_AWAY) {
                payload.skipBytes(4); // last stream id
                goAwayErrorCode = payload.readUnsignedInt();
            }
        }
        return new long[] { rstFrames, goAwayErrorCode };
    }
}
