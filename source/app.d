import std.stdio;
import std.conv;
import std.bitmanip;
import std.encoding;
import std.json;
import std.uni;
import vibe.d;
import dproto.dproto;

mixin ProtocolBuffer!"ql2.proto";

/*
   TODO(paul): Specify query encoding correctly.

   RQL encodes into a list:

   (Type, [Parent,] Args)

 */
class RQL {
  Term.TermType command;
  string[] arguments;
  int options;
  RQL parent;

  this(Term.TermType command) {
    this.command = command;
  }

  this(Term.TermType command, string[] arguments) {
    this.command = command;
    this.arguments = arguments;
  }

  RQL table(string table) {
    auto r = new RQL(Term.TermType.TABLE, [table]);
    r.parent = this;
    return r;
  }

  static RQL db(string db) {
    return new RQL(Term.TermType.DB, [db]);
  }

  JSONValue json() {
    JSONValue j = [this.command];
    if(this.parent) {
      j.array ~= [this.parent.json(), JSONValue(this.arguments)];
    } else {
      j.array ~= JSONValue(this.arguments);
    }

    return j;
  }

  Term term() {
    Term t;
    t.type = this.command;

    Datum d;
    d.type = Datum.DatumType.R_STR;
    d.r_str = this.arguments[0];

    t.datum = d;

    return t;
  }
}

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
    if(this.conn.connected) {
      this.conn.close();
    }

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
    // auto protocolType = nativeToLittleEndian(VersionDummy.Protocol.PROTOBUF);
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

    auto term = RQL.db("foobar").table("qux").json();
    auto token = ++this.queryToken;

    JSONValue q = [Query.QueryType.START];
    q.array ~= term;
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

    return;
  }
}

void main()
{
  auto sess = new Session("localhost", 28015);

  sess.open();

  if(sess.handshake()) {
    writeln("Handshake sucessful");

    sess.query();
  } else {
    writeln("Handeshake failed");
  }

  // R.db("foo").table("bar").run(sess);

  sess.close();

  writeln("Edit source/app.d to start your project.");
}
