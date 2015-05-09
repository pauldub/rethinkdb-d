module rethinkdb.session;

import std.stdio;
import std.bitmanip;
import std.json;
import vibe.d;
import proto = rethinkdb.protocol;
import rethinkdb.response;
import rethinkdb.rql;

alias Protocol = proto.Protocol;

/*
 * TODO(paul):
 *  - Ensure vibe.d resources are correctly cleaned up.
 */
class Session {
  enum State {
    CLOSED,
    OPEN,
    HANDSHAKE
  };

  State state;
	Protocol protocol;

  long queryToken; 

  string hostname;
  ushort port;
  TCPConnection conn;

  this(string hostname, ushort port) {
    this.state = State.CLOSED;
    this.hostname = hostname;
    this.port = port;
		this.protocol = Protocol.JSON;
  }

  void open() {
    this.conn = connectTCP(hostname, port);
    this.state = State.OPEN;
  }

  void close() {
    if(this.conn.connected) {
      this.conn.close();
    }

    this.state = State.CLOSED;
  }

  bool handshake() {
    if(this.state != State.OPEN) {
      return false;
    }

    auto protocolVersion = nativeToLittleEndian(proto.Version.V0_4);
    this.conn.write(protocolVersion);

    // Authentication
    this.conn.write(nativeToLittleEndian(0));

    // TODO(paul): Support protobuf protocol.
    // auto protocolType = nativeToLittleEndian(VersionDummy.Protocol.PROTOBUF);
    auto protocolType = nativeToLittleEndian(this.protocol);
    this.conn.write(protocolType);

    ubyte[8] buf;
    this.conn.read(buf);

    auto result = fromStringz(cast(char *)buf);
    if(result != "SUCCESS") {
      return false;
    }

    this.state = State.HANDSHAKE;

    return true;
  }

	private Response queryJson(T, U)(long token, RQL!(T, U) rql) {
		JSONValue q = [proto.QueryType.START];
    q.array ~= rql.json();
    q.array ~= parseJSON("{}");

    auto query = q.toString();

    long len(T)(T[] str) {
      return str.length * T.sizeof;
    }

    writefln("query: %s token: %d len: %d %d", query, token, query.length, len(query));
    writefln("%(%02x %)", nativeToLittleEndian(token));
    writefln("%(%02x %)", nativeToLittleEndian(cast(int)len(query)));
    writefln("%(%02x %)", cast(ubyte[])query);

    this.conn.write(nativeToLittleEndian(token));
    this.conn.write(nativeToLittleEndian(cast(int)len(query)));
    this.conn.write(query);

    // Read response query token
    ubyte[8] tokenBuf;
    this.conn.read(tokenBuf);
    auto resToken = littleEndianToNative!long(tokenBuf);

    // Read response len
    ubyte[4] lenBuf;
    this.conn.read(lenBuf);
    auto resLen = littleEndianToNative!int(lenBuf);

    // Read response content
    ubyte[] resBuf;
    resBuf.length = resLen;
    this.conn.read(resBuf);

    writefln("token: %d len: %d res: %s", resToken, resLen,
        fromStringz(cast(char *)resBuf));

    return Response.fromJSON(parseJSON(resBuf));
	}

	private Response queryProtobuf(T, U)(long token, RQL!(T, U) rql) {
		proto.Query q; 
		q.type = proto.QueryType.START;
		q.query = rql.term!T();
		q.token = token;

		auto query = q.serialize();
    writefln("query: %s token: %d len: %d", query, token, query.length);

    writefln("%(%02x %)", nativeToLittleEndian(token));
    writefln("%(%02x %)", nativeToLittleEndian(query.length));
    writefln("%(%02x %)", query);

		this.conn.write(nativeToLittleEndian(query.length));
		this.conn.write(query);

		writeln("Sent query");

    // Read response len
    ubyte[4] lenBuf;
    this.conn.read(lenBuf);
    auto resLen = littleEndianToNative!int(lenBuf);

    // Read response content
    ubyte[] resBuf;
    resBuf.length = resLen;
    this.conn.read(resBuf);

		auto res = proto.Response(resBuf);

		auto response = new Response();
		response.token = res.token;
		response.type = res.type;

    writefln("token: %d type: %d len: %d res: %s", response.token, response.type, resLen, res.serialize());

		// TODO(paul): add pp(Datum) for debugging.
		foreach(proto.Datum d; res.response) {
			writefln("d - type: %d ", cast(int)d.type);
			if(d.type == proto.DatumType.R_STR) {
					writefln("d - r_str: %s", d.r_str);
			}
		}

		return response;
	}

  Response query(T, U)(RQL!(T, U) rql) {
    if(this.state != State.HANDSHAKE) {
      throw new Exception("Session state is not HANDSHAKE");
    }

    auto token = ++this.queryToken;

		if(this.protocol == Protocol.JSON) {
			return queryJson(token, rql);
		} else if (this.protocol == Protocol.PROTOBUF) {
			return 	queryProtobuf(token, rql);
		} else {
			throw new Exception("Unsupported protocol.");
		}
  }
}

