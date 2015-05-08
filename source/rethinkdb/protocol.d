module rethinkdb.protocol; 

import dproto.dproto;

class Proto {
	mixin ProtocolBuffer!"ql2.proto";
}

alias Proto.Term Term;
alias Proto.Term.TermType TermType;

alias Proto.Response Response;
alias Proto.Response.ResponseType ResponseType;
alias Proto.Response.ResponseNote ResponseNote;

alias Proto.VersionDummy.Version Version;
alias Proto.VersionDummy.Protocol Protocol;

alias Proto.Query.QueryType QueryType;

alias Proto.Datum Datum;
alias Proto.Datum.DatumType DatumType;

