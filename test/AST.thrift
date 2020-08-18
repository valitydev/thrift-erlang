# A model of some hypothetical programming language grammar.
#
# This model is both quite complex and self-recursive in some of the submodels,
# which gives us helpful flexibility to generate large, deeply nested instances
# of the model, thus very useful in benchmarking affairs.

struct Module {
    1: required string name
    2: optional list<Form> forms
}

struct Form {
    1: required FormType type
    2: optional map<string, Anno> annos
    3: required Coord coord
    4: optional i64 hash
    5: optional list<string> aliases
    6: optional list<Coord> backrefs
    7: optional binary context
    8: required bool valid
    9: optional bool insrumented
}

union FormType {
    1: Attribute attribute
    2: Docstring docstring
    3: Function fn
}

struct Attribute {
    1: required string name
    2: required Term val
}

struct Docstring {
    1: required binary content
}

struct Function {
    1: required string name
    2: required i8 arity
    3: optional bool priv
    4: optional list<Clause> clauses
}

struct Clause {
    1: required list<Binding> head
    2: required list<Expression> body
    3: required Coord coord
}

union Binding {
    1: string variable
    2: Term pat
    3: Hole hole
}

union Term {
    1: string atom
    2: i64 int
    3: double flt
    4: binary bin
    5: list<Term> lst
    6: map<Term, Term> mp
}

struct Hole {}

struct Expression {
    1: required ExpressionType type
    2: optional map<string, Anno> annos
    3: required Coord coord
}

union ExpressionType {
    1: Match match
    2: Call call
}

struct Match {
    1: required Binding lhs
    2: required Expression rhs
}

struct Call {
    1: optional string mod
    2: required string name
    3: required list<Expression> arglist
}

struct Coord {
    1: required string mod
    2: required string file
    3: required i32 line
    4: required i32 column
}

struct Anno {
    1: required i64 tag
    2: required string val
}
