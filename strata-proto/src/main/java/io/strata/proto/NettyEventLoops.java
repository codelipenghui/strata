package io.strata.proto;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

final class NettyEventLoops {
    static final NioEventLoopGroup CLIENT_GROUP =
            new NioEventLoopGroup(0, new DefaultThreadFactory("scp-client-io", true));
    static final NioEventLoopGroup SERVER_BOSS_GROUP =
            new NioEventLoopGroup(1, new DefaultThreadFactory("scp-server-boss", true));
    static final NioEventLoopGroup SERVER_WORKER_GROUP =
            new NioEventLoopGroup(0, new DefaultThreadFactory("scp-server-io", true));

    private NettyEventLoops() {}
}
