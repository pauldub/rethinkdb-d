module rethinkdb.response;

import std.json;
import proto = rethinkdb.protocol;

class Response {
	long token;
  proto.ResponseType type;
  JSONValue result;
  proto.ResponseNote[] notes;
	proto.Datum[] data;

	this() {
	}

  this(proto.ResponseType type, JSONValue result, proto.ResponseNote[] notes) {
    this.type = type;
    this.result = result;
    this.notes = notes;
  }

  static Response fromJSON(JSONValue j) {
    auto type = cast(proto.ResponseType)j["t"].integer;
    auto result = j["r"];
    proto.ResponseNote[] notes;
    notes.length = j["n"].array.length;

    foreach(JSONValue jn; j["n"].array) {
      notes ~= cast(proto.ResponseNote)jn.integer;
    }

    return new Response(type, result, notes);
  }
}
