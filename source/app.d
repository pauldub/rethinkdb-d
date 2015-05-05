import std.stdio;
import std.conv;
import std.bitmanip;
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

		auto protocolType = nativeToLittleEndian(VersionDummy.Protocol.PROTOBUF);
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
	} else {
		writeln("Handeshake failed");
	}

	writeln("Edit source/app.d to start your project.");
}
