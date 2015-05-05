import std.stdio;
import vibe.d;
import dproto.dproto;

mixin ProtocolBuffer!"ql2.proto";

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

  int handshake() {
    if(this.state != State.OPEN) {
      return 1;
    }

    this.state = State.HANDSHAKE;
    this.conn.write(VersionDummy.Version.V0_4);

    return 0;
  }

  void query() {
    return;
  }
}

void main()
{
  auto sess = new Session("localhost", 28015);
  sess.open();

	writeln("Edit source/app.d to start your project.");
}
