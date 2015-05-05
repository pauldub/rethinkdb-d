import std.stdio;
import std.conv;
import std.bitmanip;
import std.encoding;
import std.json;
import vibe.d;
import dproto.dproto;

mixin ProtocolBuffer!"ql2.proto";

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

  long queryToken; 
  TCPConnection conn;

  string hostname;
  ushort port;

	this(string hostname, ushort port) {
		this.state = State.CLOSED;
		this.hostname = hostname;
		this.port = port;
	}

	void open() {
		this.conn = connectTCP(hostname, port);
		this.state = State.OPEN;
	}

	void close() {
		this.conn.close();
		this.state = State.CLOSED;
	}

	bool handshake() {
		if(this.state != State.OPEN) {
			return false;
		}

		auto protocolVersion = nativeToLittleEndian(VersionDummy.Version.V0_4);
		this.conn.write(protocolVersion);

		// Authentication
		this.conn.write(nativeToLittleEndian(0));

    // TODO(paul): Support protobuf protocol.
		auto protocolType = nativeToLittleEndian(VersionDummy.Protocol.JSON);
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

	void query() {
    if(this.state != State.HANDSHAKE) {
      throw new Exception("Session state is not HANDSHAKE");
    }

    JSONValue jj = ["foo": "bar"];
    AsciiString as;
    auto query = jj.toString();
    auto token = this.queryToken++;

    query.transcode(as);

    writefln("query: %s token: %d len: %d", query, token, as.length);

    this.conn.write(nativeToLittleEndian(token));
    this.conn.write(nativeToLittleEndian(query.length));
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


		return;
	}
}

void main()
{
	auto sess = new Session("localhost", 28015);
	scope(exit) sess.close();

	sess.open();

	if(sess.handshake()) {
		writeln("Handshake sucessful");

    sess.query();
	} else {
		writeln("Handeshake failed");
	}

	writeln("Edit source/app.d to start your project.");
}
