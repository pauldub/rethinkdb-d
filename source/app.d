import std.stdio;

import rethinkdb;

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
