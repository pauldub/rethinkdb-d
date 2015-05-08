import std.stdio;
import std.conv;
import std.bitmanip;
import std.encoding;
import std.json;
import std.uni;
import vibe.d;
import dproto.dproto;

mixin ProtocolBuffer!"ql2.proto";

alias Term.TermType TermType;
alias Response.ResponseType ResponseType;
alias Response.ResponseNote ResponseNote;
alias VersionDummy.Version Version;
alias VersionDummy.Protocol Protocol;
alias Query.QueryType QueryType;
alias Datum.DatumType DatumType;

/*
 	TODO(paul): Use proper names for each class.
 */
class R {
  static RQL!T db(T = string[])(string db) {
    return new RQL!T(TermType.DB, [db]);
  }
}

// T = Type of arguments for this expression
// P = Type of arguments for the parent expression
class RQL(T, P = string[]) {
  TermType command;
  T arguments;
  int options;
  RQL!P parent;

  this(TermType command) {
    this.command = command;
  }

  this(TermType command, T arguments) {
    this.command = command;
    this.arguments = arguments;
  }

  RQL!(U, T) table(U = string[])(string table) {
    return chain!(U)(TermType.TABLE, [table]);
  }
  
  RQL!(U, T) filter(U = string[string])(U args) {
    return chain!(U)(TermType.FILTER, args);
  } 

  // U = Type of arguments for the chained expression
  RQL!(U, T) chain(U)(TermType type, U args) {
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

  Datum makeDatum(string s) {
    Datum d;
    d.type = DatumType.R_STR;
    d.r_str = s;
    return d;
  }

  Datum makeDatum(bool s) {
    Datum d;
    d.type = DatumType.R_BOOL;
    d.r_bool = s;
    return d;
  }

  Datum makeDatum(double s) {
    Datum d;
    d.type = DatumType.R_NUM;
    d.r_num = s;
    return d;
  }

  Datum makeDatum(T)(string[T] s) {
    Datum d;
    d.type = DatumType.R_OBJ;
    foreach(string k, T v; s) {
      Datum.AssocPair pair;
      pair.key = k;
      pair.val = datum(v);

      d.r_obj ~= pair;
    }
  }

  Datum datum(T)(T s) {
    return makeDatum(s);
  }

  Term term(T : T[])() {
    Term t;
    t.type = this.command;

    if(this.parent) {
      t.args ~= this.parent.term();
    }

    foreach(T a; this.arguments) {
      Term arg;
      arg.type = TermType.DATUM;
      arg.datum = datum(a);

      t.args ~= arg;
    }

    return t;
  }

  RethinkResponse run(Session sess) {
    return sess.query!(T, P)(this);
  }
}

class RethinkResponse {
  ResponseType type;
  JSONValue result;
  ResponseNote[] notes;

  this(ResponseType type, JSONValue result, ResponseNote[] notes) {
    this.type = type;
    this.result = result;
    this.notes = notes;
  }

  static RethinkResponse fromJSON(JSONValue j) {
    auto type = cast(ResponseType)j["t"].integer;
    auto result = j["r"];
    Response.ResponseNote[] notes;
    notes.length = j["n"].array.length;

    foreach(JSONValue jn; j["n"].array) {
      notes ~= cast(ResponseNote)jn.integer;
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

    auto protocolVersion = nativeToLittleEndian(Version.V0_4);
    this.conn.write(protocolVersion);

    // Authentication
    this.conn.write(nativeToLittleEndian(0));

    // TODO(paul): Support protobuf protocol.
    // auto protocolType = nativeToLittleEndian(VersionDummy.Protocol.PROTOBUF);
    auto protocolType = nativeToLittleEndian(Protocol.JSON);
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

    JSONValue q = [QueryType.START];
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
