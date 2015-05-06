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
class R {
  static RQL!T db(T = string[])(string db) {
    return new RQL!T(Term.TermType.DB, [db]);
  }
}

// T = Type of arguments for this expression
// P = Type of arguments for the parent expression
class RQL(T, P = string[]) {
  Term.TermType command;
  T arguments;
  int options;
  RQL!P parent;

  this(Term.TermType command) {
    this.command = command;
  }

  this(Term.TermType command, T arguments) {
    this.command = command;
    this.arguments = arguments;
  }

  RQL!(U, T) table(U = string[])(string table) {
    return chain!(U)(Term.TermType.TABLE, [table]);
  }
  
  RQL!(U, T) filter(U = string[string])(U args) {
    return chain!(U)(Term.TermType.FILTER, args);
  } 

  // U = Type of arguments for the chained expression
  RQL!(U, T) chain(U)(Term.TermType type, U args) {
    auto r = new RQL!(U, T)(type, args);
    r.parent = this;
    return r;
  }

  // TODO: Clean this mess.
  JSONValue parentJson(T : string[])() {
    auto j = JSONValue([this.parent.json()]);
    foreach(string arg; this.arguments) {
      j.array ~= JSONValue(arg);
    }
    return j;
  }

  JSONValue parentJson(T : string[string])() {
      auto j = JSONValue([this.parent.json()]);
      j.array ~= JSONValue(this.arguments);
      return j;
  }

  JSONValue json() {
    JSONValue j = [this.command];
    if(this.parent) {
      j.array ~= parentJson!T();
    } else {
      j.array ~= JSONValue(this.arguments);
    }

    return j;
  }

  /*
  Term term() {
    Term t;
    t.type = this.command;

    Datum d;
    d.type = Datum.DatumType.R_STR;
    d.r_str = this.arguments[0];

    t.datum = d;

    return t;
  }
  */

  RethinkResponse run(Session sess) {
    return sess.query!(T, P)(this);
  }
}

class RethinkResponse {
  Response.ResponseType type;
  JSONValue result;
  Response.ResponseNote[] notes;

  this(Response.ResponseType type, JSONValue result, Response.ResponseNote[] notes) {
    this.type = type;
    this.result = result;
    this.notes = notes;
  }

  static RethinkResponse fromJSON(JSONValue j) {
    auto type = cast(Response.ResponseType)j["t"].integer;
    auto result = j["r"];
    Response.ResponseNote[] notes;
    notes.length = j["n"].array.length;

    foreach(JSONValue jn; j["n"].array) {
      notes ~= cast(Response.ResponseNote)jn.integer;
    }

    return new RethinkResponse(type, result, notes);
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

  RethinkResponse query(T, U)(RQL!(T, U) term) {
    if(this.state != State.HANDSHAKE) {
      throw new Exception("Session state is not HANDSHAKE");
    }

    auto token = ++this.queryToken;

    JSONValue q = [Query.QueryType.START];
    q.array ~= term.json();
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

    return RethinkResponse.fromJSON(parseJSON(resBuf));
  }
}

void main()
{
  auto sess = new Session("localhost", 28015);

  sess.open();

  if(sess.handshake()) {
    writeln("Handshake sucessful");

    R.db("blog").table("users").filter(["name": "Michel"]).run(sess);
  } else {
    writeln("Handeshake failed");
  }

  sess.close();

  writeln("Edit source/app.d to start your project.");
}
