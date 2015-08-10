import std.stdio;

import rethinkdb;
import std.json;

void main()
{
  auto sess = new Session("localhost", 28015);
	// sess.protocol = Protocol.PROTOBUF;

  sess.open();

  if(sess.handshake()) {
    writeln("Handshake sucessful");

    auto res = R.db("blog").table("users").filter(["name": "Michel"]).run(sess);
    res = R.db("blog").table("users").filter(["name": "Michel"]).run(sess);

    foreach(JSONValue v; res.result.array) {
      writeln("id: ", v["id"].str, " name: ", v["name"].str);
    }
  } else {
    writeln("Handeshake failed");
  }

  sess.close();

  writeln("Edit source/app.d to start your project.");
}
