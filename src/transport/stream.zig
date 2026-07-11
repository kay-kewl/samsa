const std = @import("std");
const builtin = @import("builtin");

pub const Stream = struct {
    handle: std.posix.fd_t,

    pub fn close(self: Stream) void {
        closeFd(self.handle);
    }

    pub fn readAtLeast(self: Stream, buffer: []u8, len: usize) !usize {
        var total: usize = 0;
        while (total < len) {
            const n = try recvFd(self.handle, buffer[total..], 0);
            if (n == 0) {
                break;
            }
            total += n;
        }
        return total;
    }

    pub fn writeAll(self: Stream, bytes: []const u8) !void {
        var offset: usize = 0;
        while (offset < bytes.len) {
            const flags: u32 = if (builtin.os.tag == .linux) std.posix.MSG.NOSIGNAL else 0;
            const n = try sendFd(self.handle, bytes[offset..], flags);
            if (n == 0) {
                return error.ConnectionResetByPeer;
            }
            offset += n;
        }
    }
};

pub const ShutdownHow = enum(i32) {
    receive = 0,
    send = 1,
    both = 2,
};

fn errnoToSocketError(e: std.posix.E) anyerror {
    return switch (e) {
        .AGAIN => error.WouldBlock,
        .BADF => error.NotOpenForReading,
        .CONNRESET => error.ConnectionResetByPeer,
        .CONNREFUSED => error.ConnectionRefused,
        .INPROGRESS => error.ConnectionPending,
        .ALREADY => error.ConnectionPending,
        .NETUNREACH => error.NetworkUnreachable,
        .TIMEDOUT => error.ConnectionTimedOut,
        .PIPE => error.BrokenPipe,
        .INTR => error.Interrupted,
        else => error.Unexpected,
    };
}

pub fn closeFd(fd: std.posix.fd_t) void {
    _ = std.posix.system.close(fd);
}

pub fn socketTcp(family: std.posix.sa_family_t, nonblocking: bool) !std.posix.fd_t {
    const nonblocking_flag: u32 = if (nonblocking) std.posix.SOCK.NONBLOCK else 0;
    const socket_type: u32 = std.posix.SOCK.STREAM | nonblocking_flag;
    const rc = std.posix.system.socket(@intCast(family), socket_type, std.posix.IPPROTO.TCP);
    return switch (std.posix.errno(rc)) {
        .SUCCESS => @intCast(rc),
        else => |e| errnoToSocketError(e),
    };
}

pub fn connectFd(fd: std.posix.fd_t, address: *const std.posix.sockaddr, address_len: std.posix.socklen_t) !void {
    const rc = std.posix.system.connect(fd, address, address_len);
    return switch (std.posix.errno(rc)) {
        .SUCCESS => {},
        else => |e| errnoToSocketError(e),
    };
}

pub fn setSockOpt(fd: std.posix.fd_t, level: i32, optname: u32, value: []const u8) !void {
    const rc = std.posix.system.setsockopt(fd, level, optname, value.ptr, @intCast(value.len));
    return switch (std.posix.errno(rc)) {
        .SUCCESS => {},
        else => |e| errnoToSocketError(e),
    };
}

pub fn getSockOpt(fd: std.posix.fd_t, level: i32, optname: u32, value: []u8) !void {
    var len: std.posix.socklen_t = @intCast(value.len);
    const rc = std.posix.system.getsockopt(fd, level, optname, value.ptr, &len);
    return switch (std.posix.errno(rc)) {
        .SUCCESS => {},
        else => |e| errnoToSocketError(e),
    };
}

pub fn recvFd(fd: std.posix.fd_t, buffer: []u8, flags: u32) !usize {
    while (true) {
        const rc = std.posix.system.recvfrom(fd, buffer.ptr, buffer.len, flags, null, null);
        switch (std.posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            else => |e| return errnoToSocketError(e),
        }
    }
}

pub fn sendFd(fd: std.posix.fd_t, bytes: []const u8, flags: u32) !usize {
    while (true) {
        const rc = std.posix.system.sendto(fd, bytes.ptr, bytes.len, flags, null, 0);
        switch (std.posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            else => |e| return errnoToSocketError(e),
        }
    }
}

pub fn shutdownFd(fd: std.posix.fd_t, how: ShutdownHow) !void {
    const rc = std.posix.system.shutdown(fd, @intFromEnum(how));
    return switch (std.posix.errno(rc)) {
        .SUCCESS => {},
        else => |e| errnoToSocketError(e),
    };
}

pub fn parseIpAddress(host: []const u8, port: u16) !std.Io.net.IpAddress {
    var threaded = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer threaded.deinit();

    return resolveAddress(threaded.io(), host, port);
}

pub fn resolveAddress(io: std.Io, host: []const u8, port: u16) !std.Io.net.IpAddress {
    if (std.Io.net.IpAddress.resolve(io, host, port)) |address| {
        return address;
    } else |_| {
        const host_name = try std.Io.net.HostName.init(host);
        var result_buffer: [16]std.Io.net.HostName.LookupResult = undefined;
        var results: std.Io.Queue(std.Io.net.HostName.LookupResult) = .init(&result_buffer);

        try std.Io.net.HostName.lookup(host_name, io, &results, .{ .port = port });
        while (results.getOneUncancelable(io)) |result| {
            switch (result) {
                .address => |address| return address,
                .canonical_name => {},
            }
        } else |err| switch (err) {
            error.Closed => {},
        }
    }

    return error.UnknownHostName;
}

pub fn connectTcp(host: []const u8, port: u16) !Stream {
    var address = try parseIpAddress(host, port);
    var storage: std.Io.Threaded.PosixAddress = undefined;
    const sock_len = std.Io.Threaded.addressToPosix(&address, &storage);
    const sock = try socketTcp(storage.any.family, false);
    errdefer closeFd(sock);

    try connectFd(sock, &storage.any, sock_len);
    return .{ .handle = sock };
}
