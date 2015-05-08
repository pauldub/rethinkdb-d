module rethinkdb.rql;

import std.json;
import rethinkdb.protocol;
import rethinkdb.session;
import rethinkdb.response;

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

  rethinkdb.response.Response run(Session sess) {
    return sess.query!(T, P)(this);
  }
}

