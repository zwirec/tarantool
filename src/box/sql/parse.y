/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains SQLite's grammar for SQL.  Process this file
** using the lemon parser generator to generate C code that runs
** the parser.  Lemon will also generate a header file containing
** numeric codes for all of the tokens.
*/

// All token codes are small integers with #defines that begin with "TK_"
%token_prefix TK_

// The type of the data attached to each token is Token.  This is also the
// default type for non-terminals.
//
%token_type {Token}
%default_type {Token}

// The generated parser function takes a 4th argument as follows:
%extra_argument {Parse *pParse}

// This code runs whenever there is a syntax error
//
%syntax_error {
  UNUSED_PARAMETER(yymajor);  /* Silence some compiler warnings */
  assert( TOKEN.z[0] );  /* The tokenizer always gives us a token */
  if (yypParser->is_fallback_failed && TOKEN.isReserved) {
    sqlite3ErrorMsg(pParse, "keyword \"%T\" is reserved", &TOKEN);
  } else {
    sqlite3ErrorMsg(pParse, "near \"%T\": syntax error", &TOKEN);
  }
}
%stack_overflow {
  sqlite3ErrorMsg(pParse, "parser stack overflow");
}

// The name of the generated procedure that implements the parser
// is as follows:
%name sqlite3Parser

// The following text is included near the beginning of the C source
// code file that implements the parser.
//
%include {
#include "sqliteInt.h"
#include "box/fkey.h"

/*
** Disable all error recovery processing in the parser push-down
** automaton.
*/
#define YYNOERRORRECOVERY 1

/*
** Make yytestcase() the same as testcase()
*/
#define yytestcase(X) testcase(X)

/*
** Indicate that sqlite3ParserFree() will never be called with a null
** pointer.
*/
#define YYPARSEFREENEVERNULL 1

/*
** Alternative datatype for the argument to the malloc() routine passed
** into sqlite3ParserAlloc().  The default is size_t.
*/
#define YYMALLOCARGTYPE  u64

/*
** An instance of this structure holds information about the
** LIMIT clause of a SELECT statement.
*/
struct LimitVal {
  Expr *pLimit;    /* The LIMIT expression.  NULL if there is no limit */
  Expr *pOffset;   /* The OFFSET expression.  NULL if there is none */
};

/*
** An instance of the following structure describes the event of a
** TRIGGER.  "a" is the event type, one of TK_UPDATE, TK_INSERT,
** TK_DELETE, or TK_INSTEAD.  If the event is of the form
**
**      UPDATE ON (a,b,c)
**
** Then the "b" IdList records the list "a,b,c".
*/
struct TrigEvent { int a; IdList * b; };

/*
** Disable lookaside memory allocation for objects that might be
** shared across database connections.
*/
static void disableLookaside(Parse *pParse){
  pParse->disableLookaside++;
  pParse->db->lookaside.bDisable++;
}

} // end %include

// Input is a single SQL command
input ::= ecmd.
ecmd ::= explain cmdx SEMI. {
	if (!pParse->parse_only)
		sql_finish_coding(pParse);
}
ecmd ::= SEMI. {
  sqlite3ErrorMsg(pParse, "syntax error: empty request");
}
explain ::= .
explain ::= EXPLAIN.              { pParse->explain = 1; }
explain ::= EXPLAIN QUERY PLAN.   { pParse->explain = 2; }
cmdx ::= cmd.

// Define operator precedence early so that this is the first occurrence
// of the operator tokens in the grammer.  Keeping the operators together
// causes them to be assigned integer values that are close together,
// which keeps parser tables smaller.
//
// The token values assigned to these symbols is determined by the order in
// which lemon first sees them.  It must be the case that NE/EQ, GT/LE, and
// GE/LT are separated by only a single value.  See the sqlite3ExprIfFalse()
// routine for additional information on this constraint.
//
%left OR.
%left AND.
%right NOT.
%left IS MATCH LIKE_KW BETWEEN IN NE EQ.
%left GT LE LT GE.
%right ESCAPE.
%left BITAND BITOR LSHIFT RSHIFT.
%left PLUS MINUS.
%left STAR SLASH REM.
%left CONCAT.
%left COLLATE.
%right BITNOT.


///////////////////// Begin and end transactions. ////////////////////////////
//

cmd ::= START TRANSACTION.  {sql_transaction_begin(pParse);}
cmd ::= COMMIT.      {sql_transaction_commit(pParse);}
cmd ::= ROLLBACK.    {sql_transaction_rollback(pParse);}

savepoint_opt ::= SAVEPOINT.
savepoint_opt ::= .
cmd ::= SAVEPOINT nm(X). {
  sqlite3Savepoint(pParse, SAVEPOINT_BEGIN, &X);
}
cmd ::= RELEASE savepoint_opt nm(X). {
  sqlite3Savepoint(pParse, SAVEPOINT_RELEASE, &X);
}
cmd ::= ROLLBACK TO savepoint_opt nm(X). {
  sqlite3Savepoint(pParse, SAVEPOINT_ROLLBACK, &X);
}

///////////////////// The CREATE TABLE statement ////////////////////////////
//
cmd ::= create_table create_table_args.
create_table ::= createkw TABLE ifnotexists(E) nm(Y). {
   sqlite3StartTable(pParse,&Y,E);
}
createkw(A) ::= CREATE(A).  {disableLookaside(pParse);}

%type ifnotexists {int}
ifnotexists(A) ::= .              {A = 0;}
ifnotexists(A) ::= IF NOT EXISTS. {A = 1;}

create_table_args ::= LP columnlist RP(E). {
  sqlite3EndTable(pParse,&E,0);
}
create_table_args ::= AS select(S). {
  sqlite3EndTable(pParse,0,S);
  sql_select_delete(pParse->db, S);
}
columnlist ::= columnlist COMMA tconsdef.
columnlist ::= columnlist COMMA columnname carglist.
columnlist ::= columnname carglist.
columnlist ::= tconsdef.
columnname(A) ::= nm(A) typedef(Y). {sqlite3AddColumn(pParse,&A,&Y);}

// An IDENTIFIER can be a generic identifier, or one of several
// keywords.  Any non-standard keyword can also be an identifier.
//
%token_class id  ID|INDEXED.

// The following directive causes tokens ABORT, AFTER, ASC, etc. to
// fallback to ID if they will not parse as their original value.
// This obviates the need for the "id" nonterminal.
//
// A keyword is checked for being a reserve one in `nm`, before
// processing of this %fallback directive. Reserved keywords included
// here to avoid the situation when a keyword has no usages within
// `parse.y` file (a keyword can have more or less usages depending on
// compiler defines). When a keyword has no usages it is excluded
// from autogenerated file `parse.h` that lead to compile-time error.
//
%fallback ID
  ABORT ACTION ADD AFTER AUTOINCREMENT BEFORE CASCADE
  CONFLICT DEFERRED END FAIL
  IGNORE INITIALLY INSTEAD NO MATCH PLAN
  QUERY KEY OFFSET RAISE RELEASE REPLACE RESTRICT
%ifdef SQLITE_OMIT_COMPOUND_SELECT
  INTERSECT 
%endif SQLITE_OMIT_COMPOUND_SELECT
  RENAME CTIME_KW IF
  .
%wildcard ANY.


// And "ids" is an identifer-or-string.
//
%token_class ids  ID|STRING.

// The name of a column or table can be any of the following:
//
%type nm {Token}
nm(A) ::= id(A). {
  if(A.isReserved) {
    sqlite3ErrorMsg(pParse, "keyword \"%T\" is reserved", &A);
  }
}

// "carglist" is a list of additional constraints that come after the
// column name and column type in a CREATE TABLE statement.
//
carglist ::= carglist cconsdef.
carglist ::= .
cconsdef ::= cconsname ccons.
cconsname ::= CONSTRAINT nm(X).           {pParse->constraintName = X;}
cconsname ::= .                           {pParse->constraintName.n = 0;}
ccons ::= DEFAULT term(X).            {sqlite3AddDefaultValue(pParse,&X);}
ccons ::= DEFAULT LP expr(X) RP.      {sqlite3AddDefaultValue(pParse,&X);}
ccons ::= DEFAULT PLUS term(X).       {sqlite3AddDefaultValue(pParse,&X);}
ccons ::= DEFAULT MINUS(A) term(X).      {
  ExprSpan v;
  v.pExpr = sqlite3PExpr(pParse, TK_UMINUS, X.pExpr, 0);
  v.zStart = A.z;
  v.zEnd = X.zEnd;
  sqlite3AddDefaultValue(pParse,&v);
}

// In addition to the type name, we also care about the primary key and
// UNIQUE constraints.
//
ccons ::= NULL onconf(R).        {
    sql_column_add_nullable_action(pParse, ON_CONFLICT_ACTION_NONE);
    /* Trigger nullability mismatch error if required. */
    if (R != ON_CONFLICT_ACTION_ABORT)
        sql_column_add_nullable_action(pParse, R);
}
ccons ::= NOT NULL onconf(R).    {sql_column_add_nullable_action(pParse, R);}
ccons ::= PRIMARY KEY sortorder(Z) autoinc(I).
                                 {sqlite3AddPrimaryKey(pParse,0,I,Z);}
ccons ::= UNIQUE.                {sql_create_index(pParse,0,0,0,0,
                                                   SORT_ORDER_ASC, false,
                                                   SQL_INDEX_TYPE_CONSTRAINT_UNIQUE);}
ccons ::= CHECK LP expr(X) RP.   {sql_add_check_constraint(pParse,&X);}
ccons ::= REFERENCES nm(T) eidlist_opt(TA) refargs(R).
                                 {sql_create_foreign_key(pParse, NULL, NULL, NULL, &T, TA, false, R);}
ccons ::= defer_subclause(D).    {fkey_change_defer_mode(pParse, D);}
ccons ::= COLLATE id(C).        {sqlite3AddCollateType(pParse, &C);}

// The optional AUTOINCREMENT keyword
%type autoinc {int}
autoinc(X) ::= .          {X = 0;}
autoinc(X) ::= AUTOINCR.  {X = 1;}

// The next group of rules parses the arguments to a REFERENCES clause
// that determine if the referential integrity checking is deferred or
// or immediate and which determine what action to take if a ref-integ
// check fails.
//
%type refargs {int}
refargs(A) ::= .                  { A = FKEY_NO_ACTION; }
refargs(A) ::= refargs(A) refarg(Y). { A = (A & ~Y.mask) | Y.value; }
%type refarg {struct {int value; int mask;}}
refarg(A) ::= MATCH matcharg(X).     { A.value = X<<16; A.mask = 0xff0000; }
refarg(A) ::= ON INSERT refact.      { A.value = 0;     A.mask = 0x000000; }
refarg(A) ::= ON DELETE refact(X).   { A.value = X;     A.mask = 0x0000ff; }
refarg(A) ::= ON UPDATE refact(X).   { A.value = X<<8;  A.mask = 0x00ff00; }
%type matcharg {int}
matcharg(A) ::= SIMPLE.  { A = FKEY_MATCH_SIMPLE; }
matcharg(A) ::= PARTIAL. { A = FKEY_MATCH_PARTIAL; }
matcharg(A) ::= FULL.    { A = FKEY_MATCH_FULL; }
%type refact {int}
refact(A) ::= SET NULL.              { A = FKEY_ACTION_SET_NULL; }
refact(A) ::= SET DEFAULT.           { A = FKEY_ACTION_SET_DEFAULT; }
refact(A) ::= CASCADE.               { A = FKEY_ACTION_CASCADE; }
refact(A) ::= RESTRICT.              { A = FKEY_ACTION_RESTRICT; }
refact(A) ::= NO ACTION.             { A = FKEY_NO_ACTION; }
%type defer_subclause {int}
defer_subclause(A) ::= NOT DEFERRABLE init_deferred_pred_opt.     {A = 0;}
defer_subclause(A) ::= DEFERRABLE init_deferred_pred_opt(X).      {A = X;}
%type init_deferred_pred_opt {int}
init_deferred_pred_opt(A) ::= .                       {A = 0;}
init_deferred_pred_opt(A) ::= INITIALLY DEFERRED.     {A = 1;}
init_deferred_pred_opt(A) ::= INITIALLY IMMEDIATE.    {A = 0;}

tconsdef ::= tconsname tcons.
tconsname ::= CONSTRAINT nm(X).      {pParse->constraintName = X;}
tconsname ::= .                      {pParse->constraintName.n = 0;}
tcons ::= PRIMARY KEY LP sortlist(X) autoinc(I) RP.
                                 {sqlite3AddPrimaryKey(pParse,X,I,0);}
tcons ::= UNIQUE LP sortlist(X) RP.
                                 {sql_create_index(pParse,0,0,X,0,
                                                   SORT_ORDER_ASC,false,
                                                   SQL_INDEX_TYPE_CONSTRAINT_UNIQUE);}
tcons ::= CHECK LP expr(E) RP onconf.
                                 {sql_add_check_constraint(pParse,&E);}
tcons ::= FOREIGN KEY LP eidlist(FA) RP
          REFERENCES nm(T) eidlist_opt(TA) refargs(R) defer_subclause_opt(D). {
    sql_create_foreign_key(pParse, NULL, NULL, FA, &T, TA, D, R);
}
%type defer_subclause_opt {int}
defer_subclause_opt(A) ::= .                    {A = 0;}
defer_subclause_opt(A) ::= defer_subclause(A).

// The following is a non-standard extension that allows us to declare the
// default behavior when there is a constraint conflict.
//
%type onconf {int}
%type index_onconf {int}
%type orconf {int}
%type resolvetype {int}
onconf(A) ::= .                              {A = ON_CONFLICT_ACTION_ABORT;}
onconf(A) ::= ON CONFLICT resolvetype(X).    {A = X;}
orconf(A) ::= .                              {A = ON_CONFLICT_ACTION_DEFAULT;}
orconf(A) ::= OR resolvetype(X).             {A = X;}
resolvetype(A) ::= raisetype(A).
resolvetype(A) ::= IGNORE.                   {A = ON_CONFLICT_ACTION_IGNORE;}
resolvetype(A) ::= REPLACE.                  {A = ON_CONFLICT_ACTION_REPLACE;}

////////////////////////// The DROP TABLE /////////////////////////////////////
//
cmd ::= DROP TABLE ifexists(E) fullname(X). {
  sql_drop_table(pParse, X, 0, E);
}
%type ifexists {int}
ifexists(A) ::= IF EXISTS.   {A = 1;}
ifexists(A) ::= .            {A = 0;}

///////////////////// The CREATE VIEW statement /////////////////////////////
//
cmd ::= createkw(X) VIEW ifnotexists(E) nm(Y) eidlist_opt(C)
          AS select(S). {
  if (!pParse->parse_only)
    sql_create_view(pParse, &X, &Y, C, S, E);
  else
    sql_store_select(pParse, S);
}
cmd ::= DROP VIEW ifexists(E) fullname(X). {
  sql_drop_table(pParse, X, 1, E);
}

//////////////////////// The SELECT statement /////////////////////////////////
//
cmd ::= select(X).  {
  SelectDest dest = {SRT_Output, 0, 0, 0, 0, 0, 0};
  if(!pParse->parse_only)
          sqlite3Select(pParse, X, &dest);
  else
          sql_expr_extract_select(pParse, X);
  sql_select_delete(pParse->db, X);
}

%type select {Select*}
%destructor select {sql_select_delete(pParse->db, $$);}
%type selectnowith {Select*}
%destructor selectnowith {sql_select_delete(pParse->db, $$);}
%type oneselect {Select*}
%destructor oneselect {sql_select_delete(pParse->db, $$);}

%include {
  /**
   * For a compound SELECT statement, make sure
   * p->pPrior->pNext==p for all elements in the list. And make
   * sure list length does not exceed SQL_LIMIT_COMPOUND_SELECT.
   */
  static void parserDoubleLinkSelect(Parse *pParse, Select *p){
    if( p->pPrior ){
      Select *pNext = 0, *pLoop;
      int mxSelect, cnt = 0;
      for(pLoop=p; pLoop; pNext=pLoop, pLoop=pLoop->pPrior, cnt++){
        pLoop->pNext = pNext;
        pLoop->selFlags |= SF_Compound;
      }
      if( (p->selFlags & SF_MultiValue)==0 && 
        (mxSelect = pParse->db->aLimit[SQL_LIMIT_COMPOUND_SELECT])>0 &&
        cnt>mxSelect
      ){
        sqlite3ErrorMsg(pParse, "Too many UNION or EXCEPT or INTERSECT "
                        "operations (limit %d is set)",
                        pParse->db->aLimit[SQL_LIMIT_COMPOUND_SELECT]);
      }
    }
  }
}

select(A) ::= with(W) selectnowith(X). {
  Select *p = X;
  if( p ){
    p->pWith = W;
    parserDoubleLinkSelect(pParse, p);
  }else{
    sqlite3WithDelete(pParse->db, W);
  }
  A = p; /*A-overwrites-W*/
}

selectnowith(A) ::= oneselect(A).
%ifndef SQLITE_OMIT_COMPOUND_SELECT
selectnowith(A) ::= selectnowith(A) multiselect_op(Y) oneselect(Z).  {
  Select *pRhs = Z;
  Select *pLhs = A;
  if( pRhs && pRhs->pPrior ){
    SrcList *pFrom;
    Token x;
    x.n = 0;
    parserDoubleLinkSelect(pParse, pRhs);
    pFrom = sqlite3SrcListAppendFromTerm(pParse,0,0,&x,pRhs,0,0);
    pRhs = sqlite3SelectNew(pParse,0,pFrom,0,0,0,0,0,0,0);
  }
  if( pRhs ){
    pRhs->op = (u8)Y;
    pRhs->pPrior = pLhs;
    if( ALWAYS(pLhs) ) pLhs->selFlags &= ~SF_MultiValue;
    pRhs->selFlags &= ~SF_MultiValue;
    if( Y!=TK_ALL ) pParse->hasCompound = 1;
  }else{
    sql_select_delete(pParse->db, pLhs);
  }
  A = pRhs;
}
%type multiselect_op {int}
multiselect_op(A) ::= UNION(OP).             {A = @OP; /*A-overwrites-OP*/}
multiselect_op(A) ::= UNION ALL.             {A = TK_ALL;}
multiselect_op(A) ::= EXCEPT|INTERSECT(OP).  {A = @OP; /*A-overwrites-OP*/}
%endif SQLITE_OMIT_COMPOUND_SELECT
oneselect(A) ::= SELECT(S) distinct(D) selcollist(W) from(X) where_opt(Y)
                 groupby_opt(P) having_opt(Q) orderby_opt(Z) limit_opt(L). {
#ifdef SELECTTRACE_ENABLED
  Token s = S; /*A-overwrites-S*/
#endif
  A = sqlite3SelectNew(pParse,W,X,Y,P,Q,Z,D,L.pLimit,L.pOffset);
#ifdef SELECTTRACE_ENABLED
  /* Populate the Select.zSelName[] string that is used to help with
  ** query planner debugging, to differentiate between multiple Select
  ** objects in a complex query.
  **
  ** If the SELECT keyword is immediately followed by a C-style comment
  ** then extract the first few alphanumeric characters from within that
  ** comment to be the zSelName value.  Otherwise, the label is #N where
  ** is an integer that is incremented with each SELECT statement seen.
  */
  if( A!=0 ){
    const char *z = s.z+6;
    int i;
    sqlite3_snprintf(sizeof(A->zSelName), A->zSelName, "#%d",
                     ++pParse->nSelect);
    while( z[0]==' ' ) z++;
    if( z[0]=='/' && z[1]=='*' ){
      z += 2;
      while( z[0]==' ' ) z++;
      for(i=0; sqlite3Isalnum(z[i]); i++){}
      sqlite3_snprintf(sizeof(A->zSelName), A->zSelName, "%.*s", i, z);
    }
  }
#endif /* SELECTRACE_ENABLED */
}
oneselect(A) ::= values(A).

%type values {Select*}
%destructor values {sql_select_delete(pParse->db, $$);}
values(A) ::= VALUES LP nexprlist(X) RP. {
  A = sqlite3SelectNew(pParse,X,0,0,0,0,0,SF_Values,0,0);
}
values(A) ::= values(A) COMMA LP exprlist(Y) RP. {
  Select *pRight, *pLeft = A;
  pRight = sqlite3SelectNew(pParse,Y,0,0,0,0,0,SF_Values|SF_MultiValue,0,0);
  if( ALWAYS(pLeft) ) pLeft->selFlags &= ~SF_MultiValue;
  if( pRight ){
    pRight->op = TK_ALL;
    pRight->pPrior = pLeft;
    A = pRight;
  }else{
    A = pLeft;
  }
}

// The "distinct" nonterminal is true (1) if the DISTINCT keyword is
// present and false (0) if it is not.
//
%type distinct {int}
distinct(A) ::= DISTINCT.   {A = SF_Distinct;}
distinct(A) ::= ALL.        {A = SF_All;}
distinct(A) ::= .           {A = 0;}

// selcollist is a list of expressions that are to become the return
// values of the SELECT statement.  The "*" in statements like
// "SELECT * FROM ..." is encoded as a special expression with an
// opcode of TK_ASTERISK.
//
%type selcollist {ExprList*}
%destructor selcollist {sql_expr_list_delete(pParse->db, $$);}
%type sclp {ExprList*}
%destructor sclp {sql_expr_list_delete(pParse->db, $$);}
sclp(A) ::= selcollist(A) COMMA.
sclp(A) ::= .                                {A = 0;}
selcollist(A) ::= sclp(A) expr(X) as(Y).     {
   A = sql_expr_list_append(pParse->db, A, X.pExpr);
   if( Y.n>0 ) sqlite3ExprListSetName(pParse, A, &Y, 1);
   sqlite3ExprListSetSpan(pParse,A,&X);
}
selcollist(A) ::= sclp(A) STAR. {
  Expr *p = sqlite3Expr(pParse->db, TK_ASTERISK, 0);
  A = sql_expr_list_append(pParse->db, A, p);
}
selcollist(A) ::= sclp(A) nm(X) DOT STAR. {
  Expr *pRight = sqlite3PExpr(pParse, TK_ASTERISK, 0, 0);
  Expr *pLeft = sqlite3ExprAlloc(pParse->db, TK_ID, &X, 1);
  Expr *pDot = sqlite3PExpr(pParse, TK_DOT, pLeft, pRight);
  A = sql_expr_list_append(pParse->db,A, pDot);
}

// An option "AS <id>" phrase that can follow one of the expressions that
// define the result set, or one of the tables in the FROM clause.
//
%type as {Token}
as(X) ::= AS nm(Y).    {X = Y;}
as(X) ::= ids(X).
as(X) ::= .            {X.n = 0; X.z = 0;}


%type seltablist {SrcList*}
%destructor seltablist {sqlite3SrcListDelete(pParse->db, $$);}
%type stl_prefix {SrcList*}
%destructor stl_prefix {sqlite3SrcListDelete(pParse->db, $$);}
%type from {SrcList*}
%destructor from {sqlite3SrcListDelete(pParse->db, $$);}

// A complete FROM clause.
//
from(A) ::= .                {A = sqlite3DbMallocZero(pParse->db, sizeof(*A));}
from(A) ::= FROM seltablist(X). {
  A = X;
  sqlite3SrcListShiftJoinType(A);
}

// "seltablist" is a "Select Table List" - the content of the FROM clause
// in a SELECT statement.  "stl_prefix" is a prefix of this list.
//
stl_prefix(A) ::= seltablist(A) joinop(Y).    {
   if( ALWAYS(A && A->nSrc>0) ) A->a[A->nSrc-1].fg.jointype = (u8)Y;
}
stl_prefix(A) ::= .                           {A = 0;}
seltablist(A) ::= stl_prefix(A) nm(Y) as(Z) indexed_opt(I)
                  on_opt(N) using_opt(U). {
  A = sqlite3SrcListAppendFromTerm(pParse,A,&Y,&Z,0,N,U);
  sqlite3SrcListIndexedBy(pParse, A, &I);
}
seltablist(A) ::= stl_prefix(A) nm(Y) LP exprlist(E) RP as(Z)
                  on_opt(N) using_opt(U). {
  A = sqlite3SrcListAppendFromTerm(pParse,A,&Y,&Z,0,N,U);
  sqlite3SrcListFuncArgs(pParse, A, E);
}
seltablist(A) ::= stl_prefix(A) LP select(S) RP
                  as(Z) on_opt(N) using_opt(U). {
  A = sqlite3SrcListAppendFromTerm(pParse,A,0,&Z,S,N,U);
}
seltablist(A) ::= stl_prefix(A) LP seltablist(F) RP
                  as(Z) on_opt(N) using_opt(U). {
  if( A==0 && Z.n==0 && N==0 && U==0 ){
    A = F;
  }else if( F->nSrc==1 ){
    A = sqlite3SrcListAppendFromTerm(pParse,A,0,&Z,0,N,U);
    if( A ){
      struct SrcList_item *pNew = &A->a[A->nSrc-1];
      struct SrcList_item *pOld = F->a;
      pNew->zName = pOld->zName;
      pNew->pSelect = pOld->pSelect;
      pOld->zName =  0;
      pOld->pSelect = 0;
    }
    sqlite3SrcListDelete(pParse->db, F);
  }else{
    Select *pSubquery;
    sqlite3SrcListShiftJoinType(F);
    pSubquery = sqlite3SelectNew(pParse,0,F,0,0,0,0,SF_NestedFrom,0,0);
    A = sqlite3SrcListAppendFromTerm(pParse,A,0,&Z,pSubquery,N,U);
  }
}

%type fullname {SrcList*}
%destructor fullname {sqlite3SrcListDelete(pParse->db, $$);}
fullname(A) ::= nm(X).  
   {A = sqlite3SrcListAppend(pParse->db,0,&X); /*A-overwrites-X*/}

%type joinop {int}
join_nm(A) ::= id(A).
join_nm(A) ::= JOIN_KW(A).

joinop(X) ::= COMMA|JOIN.              { X = JT_INNER; }
joinop(X) ::= JOIN_KW(A) JOIN.
                  {X = sqlite3JoinType(pParse,&A,0,0);  /*X-overwrites-A*/}
joinop(X) ::= JOIN_KW(A) join_nm(B) JOIN.
                  {X = sqlite3JoinType(pParse,&A,&B,0); /*X-overwrites-A*/}
joinop(X) ::= JOIN_KW(A) join_nm(B) join_nm(C) JOIN.
                  {X = sqlite3JoinType(pParse,&A,&B,&C);/*X-overwrites-A*/}

%type on_opt {Expr*}
%destructor on_opt {sql_expr_delete(pParse->db, $$, false);}
on_opt(N) ::= ON expr(E).   {N = E.pExpr;}
on_opt(N) ::= .             {N = 0;}

// Note that this block abuses the Token type just a little. If there is
// no "INDEXED BY" clause, the returned token is empty (z==0 && n==0). If
// there is an INDEXED BY clause, then the token is populated as per normal,
// with z pointing to the token data and n containing the number of bytes
// in the token.
//
// If there is a "NOT INDEXED" clause, then (z==0 && n==1), which is 
// normally illegal. The sqlite3SrcListIndexedBy() function 
// recognizes and interprets this as a special case.
//
%type indexed_opt {Token}
indexed_opt(A) ::= .                 {A.z=0; A.n=0;}
indexed_opt(A) ::= INDEXED BY nm(X). {A = X;}
indexed_opt(A) ::= NOT INDEXED.      {A.z=0; A.n=1;}

%type using_opt {IdList*}
%destructor using_opt {sqlite3IdListDelete(pParse->db, $$);}
using_opt(U) ::= USING LP idlist(L) RP.  {U = L;}
using_opt(U) ::= .                        {U = 0;}


%type orderby_opt {ExprList*}
%destructor orderby_opt {sql_expr_list_delete(pParse->db, $$);}

// the sortlist non-terminal stores a list of expression where each
// expression is optionally followed by ASC or DESC to indicate the
// sort order.
//
%type sortlist {ExprList*}
%destructor sortlist {sql_expr_list_delete(pParse->db, $$);}

orderby_opt(A) ::= .                          {A = 0;}
orderby_opt(A) ::= ORDER BY sortlist(X).      {A = X;}
sortlist(A) ::= sortlist(A) COMMA expr(Y) sortorder(Z). {
  A = sql_expr_list_append(pParse->db,A,Y.pExpr);
  sqlite3ExprListSetSortOrder(A,Z);
}
sortlist(A) ::= expr(Y) sortorder(Z). {
  /* A-overwrites-Y. */
  A = sql_expr_list_append(pParse->db,NULL,Y.pExpr);
  sqlite3ExprListSetSortOrder(A,Z);
}

%type sortorder {int}

sortorder(A) ::= ASC.           {A = SORT_ORDER_ASC;}
sortorder(A) ::= DESC.          {A = SORT_ORDER_DESC;}
sortorder(A) ::= .              {A = SORT_ORDER_UNDEF;}

%type groupby_opt {ExprList*}
%destructor groupby_opt {sql_expr_list_delete(pParse->db, $$);}
groupby_opt(A) ::= .                      {A = 0;}
groupby_opt(A) ::= GROUP BY nexprlist(X). {A = X;}

%type having_opt {Expr*}
%destructor having_opt {sql_expr_delete(pParse->db, $$, false);}
having_opt(A) ::= .                {A = 0;}
having_opt(A) ::= HAVING expr(X).  {A = X.pExpr;}

%type limit_opt {struct LimitVal}

// The destructor for limit_opt will never fire in the current grammar.
// The limit_opt non-terminal only occurs at the end of a single production
// rule for SELECT statements.  As soon as the rule that create the 
// limit_opt non-terminal reduces, the SELECT statement rule will also
// reduce.  So there is never a limit_opt non-terminal on the stack 
// except as a transient.  So there is never anything to destroy.
//
//%destructor limit_opt {
//  sqlite3ExprDelete(pParse->db, $$.pLimit);
//  sqlite3ExprDelete(pParse->db, $$.pOffset);
//}
limit_opt(A) ::= .                    {A.pLimit = 0; A.pOffset = 0;}
limit_opt(A) ::= LIMIT expr(X).       {A.pLimit = X.pExpr; A.pOffset = 0;}
limit_opt(A) ::= LIMIT expr(X) OFFSET expr(Y). 
                                      {A.pLimit = X.pExpr; A.pOffset = Y.pExpr;}
limit_opt(A) ::= LIMIT expr(X) COMMA expr(Y). 
                                      {A.pOffset = X.pExpr; A.pLimit = Y.pExpr;}

/////////////////////////// The DELETE statement /////////////////////////////
//
cmd ::= with(C) DELETE FROM fullname(X) indexed_opt(I) where_opt(W). {
  sqlite3WithPush(pParse, C, 1);
  sqlite3SrcListIndexedBy(pParse, X, &I);
  sqlSubProgramsRemaining = SQL_MAX_COMPILING_TRIGGERS;
  /* Instruct SQL to initate Tarantool's transaction.  */
  pParse->initiateTTrans = true;
  sql_table_delete_from(pParse,X,W);
}

/////////////////////////// The TRUNCATE statement /////////////////////////////
//
cmd ::= TRUNCATE TABLE fullname(X). {
  sql_table_truncate(pParse, X);
}

%type where_opt {Expr*}
%destructor where_opt {sql_expr_delete(pParse->db, $$, false);}

where_opt(A) ::= .                    {A = 0;}
where_opt(A) ::= WHERE expr(X).       {A = X.pExpr;}

////////////////////////// The UPDATE command ////////////////////////////////
//
cmd ::= with(C) UPDATE orconf(R) fullname(X) indexed_opt(I) SET setlist(Y)
        where_opt(W).  {
  sqlite3WithPush(pParse, C, 1);
  sqlite3SrcListIndexedBy(pParse, X, &I);
  sqlite3ExprListCheckLength(pParse,Y,"set list"); 
  sqlSubProgramsRemaining = SQL_MAX_COMPILING_TRIGGERS;
  /* Instruct SQL to initate Tarantool's transaction.  */
  pParse->initiateTTrans = true;
  sqlite3Update(pParse,X,Y,W,R);
}

%type setlist {ExprList*}
%destructor setlist {sql_expr_list_delete(pParse->db, $$);}

setlist(A) ::= setlist(A) COMMA nm(X) EQ expr(Y). {
  A = sql_expr_list_append(pParse->db, A, Y.pExpr);
  sqlite3ExprListSetName(pParse, A, &X, 1);
}
setlist(A) ::= setlist(A) COMMA LP idlist(X) RP EQ expr(Y). {
  A = sqlite3ExprListAppendVector(pParse, A, X, Y.pExpr);
}
setlist(A) ::= nm(X) EQ expr(Y). {
  A = sql_expr_list_append(pParse->db, NULL, Y.pExpr);
  sqlite3ExprListSetName(pParse, A, &X, 1);
}
setlist(A) ::= LP idlist(X) RP EQ expr(Y). {
  A = sqlite3ExprListAppendVector(pParse, 0, X, Y.pExpr);
}

////////////////////////// The INSERT command /////////////////////////////////
//
cmd ::= with(W) insert_cmd(R) INTO fullname(X) idlist_opt(F) select(S). {
  sqlite3WithPush(pParse, W, 1);
  sqlSubProgramsRemaining = SQL_MAX_COMPILING_TRIGGERS;
  /* Instruct SQL to initate Tarantool's transaction.  */
  pParse->initiateTTrans = true;
  sqlite3Insert(pParse, X, S, F, R);
}
cmd ::= with(W) insert_cmd(R) INTO fullname(X) idlist_opt(F) DEFAULT VALUES.
{
  sqlite3WithPush(pParse, W, 1);
  sqlSubProgramsRemaining = SQL_MAX_COMPILING_TRIGGERS;
  /* Instruct SQL to initate Tarantool's transaction.  */
  pParse->initiateTTrans = true;
  sqlite3Insert(pParse, X, 0, F, R);
}

%type insert_cmd {int}
insert_cmd(A) ::= INSERT orconf(R).   {A = R;}
insert_cmd(A) ::= REPLACE.            {A = ON_CONFLICT_ACTION_REPLACE;}

%type idlist_opt {IdList*}
%destructor idlist_opt {sqlite3IdListDelete(pParse->db, $$);}
%type idlist {IdList*}
%destructor idlist {sqlite3IdListDelete(pParse->db, $$);}

idlist_opt(A) ::= .                       {A = 0;}
idlist_opt(A) ::= LP idlist(X) RP.    {A = X;}
idlist(A) ::= idlist(A) COMMA nm(Y).
    {A = sqlite3IdListAppend(pParse->db,A,&Y);}
idlist(A) ::= nm(Y).
    {A = sqlite3IdListAppend(pParse->db,0,&Y); /*A-overwrites-Y*/}

/////////////////////////// Expression Processing /////////////////////////////
//

%type expr {ExprSpan}
%destructor expr {sql_expr_delete(pParse->db, $$.pExpr, false);}
%type term {ExprSpan}
%destructor term {sql_expr_delete(pParse->db, $$.pExpr, false);}

%include {
  /* This is a utility routine used to set the ExprSpan.zStart and
  ** ExprSpan.zEnd values of pOut so that the span covers the complete
  ** range of text beginning with pStart and going to the end of pEnd.
  */
  static void spanSet(ExprSpan *pOut, Token *pStart, Token *pEnd){
    pOut->zStart = pStart->z;
    pOut->zEnd = &pEnd->z[pEnd->n];
  }

  /* Construct a new Expr object from a single identifier.  Use the
  ** new Expr to populate pOut.  Set the span of pOut to be the identifier
  ** that created the expression.
  */
  static void spanExpr(ExprSpan *pOut, Parse *pParse, int op, Token t){
    Expr *p = sqlite3DbMallocRawNN(pParse->db, sizeof(Expr)+t.n+1);
    if( p ){
      memset(p, 0, sizeof(Expr));
      switch (op) {
      case TK_STRING:
        p->type = FIELD_TYPE_STRING;
        break;
      case TK_BLOB:
        p->type = FIELD_TYPE_SCALAR;
        break;
      case TK_INTEGER:
        p->type = FIELD_TYPE_INTEGER;
        break;
      case TK_FLOAT:
        p->type = FIELD_TYPE_NUMBER;
        break;
      }
      p->op = (u8)op;
      p->flags = EP_Leaf;
      p->iAgg = -1;
      p->u.zToken = (char*)&p[1];
      memcpy(p->u.zToken, t.z, t.n);
      p->u.zToken[t.n] = 0;
      if (op != TK_VARIABLE){
        sqlite3NormalizeName(p->u.zToken);
      }
#if SQLITE_MAX_EXPR_DEPTH>0
      p->nHeight = 1;
#endif  
    }
    pOut->pExpr = p;
    pOut->zStart = t.z;
    pOut->zEnd = &t.z[t.n];
  }
}

expr(A) ::= term(A).
expr(A) ::= LP(B) expr(X) RP(E).
            {spanSet(&A,&B,&E); /*A-overwrites-B*/  A.pExpr = X.pExpr;}
term(A) ::= NULL(X).        {spanExpr(&A,pParse,@X,X);/*A-overwrites-X*/}
expr(A) ::= id(X).          {spanExpr(&A,pParse,TK_ID,X); /*A-overwrites-X*/}
expr(A) ::= JOIN_KW(X).     {spanExpr(&A,pParse,TK_ID,X); /*A-overwrites-X*/}
expr(A) ::= nm(X) DOT nm(Y). {
  Expr *temp1 = sqlite3ExprAlloc(pParse->db, TK_ID, &X, 1);
  Expr *temp2 = sqlite3ExprAlloc(pParse->db, TK_ID, &Y, 1);
  spanSet(&A,&X,&Y); /*A-overwrites-X*/
  A.pExpr = sqlite3PExpr(pParse, TK_DOT, temp1, temp2);
}
term(A) ::= FLOAT|BLOB(X). {spanExpr(&A,pParse,@X,X);/*A-overwrites-X*/}
term(A) ::= STRING(X).     {spanExpr(&A,pParse,@X,X);/*A-overwrites-X*/}
term(A) ::= INTEGER(X). {
  A.pExpr = sqlite3ExprAlloc(pParse->db, TK_INTEGER, &X, 1);
  A.pExpr->type = FIELD_TYPE_INTEGER;
  A.zStart = X.z;
  A.zEnd = X.z + X.n;
  if( A.pExpr ) A.pExpr->flags |= EP_Leaf;
}
expr(A) ::= VARIABLE(X).     {
  Token t = X;
  if (pParse->parse_only) {
    spanSet(&A, &t, &t);
    sqlite3ErrorMsg(pParse, "bindings are not allowed in DDL");
    A.pExpr = NULL;
  } else if (!(X.z[0]=='#' && sqlite3Isdigit(X.z[1]))) {
    u32 n = X.n;
    spanExpr(&A, pParse, TK_VARIABLE, X);
    if (A.pExpr->u.zToken[0] == '?' && n > 1)
        sqlite3ErrorMsg(pParse, "near \"%T\": syntax error", &t);
    else
        sqlite3ExprAssignVarNumber(pParse, A.pExpr, n);
  }else{
    assert( t.n>=2 );
    spanSet(&A, &t, &t);
    sqlite3ErrorMsg(pParse, "near \"%T\": syntax error", &t);
    A.pExpr = NULL;
  }
}
expr(A) ::= expr(A) COLLATE id(C). {
  A.pExpr = sqlite3ExprAddCollateToken(pParse, A.pExpr, &C, 1);
  A.zEnd = &C.z[C.n];
}
%ifndef SQLITE_OMIT_CAST
expr(A) ::= CAST(X) LP expr(E) AS typedef(T) RP(Y). {
  spanSet(&A,&X,&Y); /*A-overwrites-X*/
  A.pExpr = sqlite3ExprAlloc(pParse->db, TK_CAST, 0, 1);
  A.pExpr->type = T.type;
  sqlite3ExprAttachSubtrees(pParse->db, A.pExpr, E.pExpr, 0);
}
%endif  SQLITE_OMIT_CAST
expr(A) ::= id(X) LP distinct(D) exprlist(Y) RP(E). {
  if( Y && Y->nExpr>pParse->db->aLimit[SQLITE_LIMIT_FUNCTION_ARG] ){
    sqlite3ErrorMsg(pParse, "too many arguments on function %T", &X);
  }
  A.pExpr = sqlite3ExprFunction(pParse, Y, &X);
  spanSet(&A,&X,&E);
  if( D==SF_Distinct && A.pExpr ){
    A.pExpr->flags |= EP_Distinct;
  }
}

type_func(A) ::= DATE(A) .
type_func(A) ::= DATETIME(A) .
type_func(A) ::= CHAR(A) .
expr(A) ::= type_func(X) LP distinct(D) exprlist(Y) RP(E). {
  if( Y && Y->nExpr>pParse->db->aLimit[SQLITE_LIMIT_FUNCTION_ARG] ){
    sqlite3ErrorMsg(pParse, "too many arguments on function %T", &X);
  }
  A.pExpr = sqlite3ExprFunction(pParse, Y, &X);
  spanSet(&A,&X,&E);
  if( D==SF_Distinct && A.pExpr ){
    A.pExpr->flags |= EP_Distinct;
  }
}

expr(A) ::= id(X) LP STAR RP(E). {
  A.pExpr = sqlite3ExprFunction(pParse, 0, &X);
  spanSet(&A,&X,&E);
}
term(A) ::= CTIME_KW(OP). {
  A.pExpr = sqlite3ExprFunction(pParse, 0, &OP);
  spanSet(&A, &OP, &OP);
}

%include {
  /* This routine constructs a binary expression node out of two ExprSpan
  ** objects and uses the result to populate a new ExprSpan object.
  */
  static void spanBinaryExpr(
    Parse *pParse,      /* The parsing context.  Errors accumulate here */
    int op,             /* The binary operation */
    ExprSpan *pLeft,    /* The left operand, and output */
    ExprSpan *pRight    /* The right operand */
  ){
    pLeft->pExpr = sqlite3PExpr(pParse, op, pLeft->pExpr, pRight->pExpr);
    pLeft->zEnd = pRight->zEnd;
  }

  /* If doNot is true, then add a TK_NOT Expr-node wrapper around the
  ** outside of *ppExpr.
  */
  static void exprNot(Parse *pParse, int doNot, ExprSpan *pSpan){
    if( doNot ){
      pSpan->pExpr = sqlite3PExpr(pParse, TK_NOT, pSpan->pExpr, 0);
    }
  }
}

expr(A) ::= LP(L) nexprlist(X) COMMA expr(Y) RP(R). {
  ExprList *pList = sql_expr_list_append(pParse->db, X, Y.pExpr);
  A.pExpr = sqlite3PExpr(pParse, TK_VECTOR, 0, 0);
  if( A.pExpr ){
    A.pExpr->x.pList = pList;
    spanSet(&A, &L, &R);
  }else{
    sql_expr_list_delete(pParse->db, pList);
  }
}

expr(A) ::= expr(A) AND(OP) expr(Y).    {spanBinaryExpr(pParse,@OP,&A,&Y);}
expr(A) ::= expr(A) OR(OP) expr(Y).     {spanBinaryExpr(pParse,@OP,&A,&Y);}
expr(A) ::= expr(A) LT|GT|GE|LE(OP) expr(Y).
                                        {spanBinaryExpr(pParse,@OP,&A,&Y);}
expr(A) ::= expr(A) EQ|NE(OP) expr(Y).  {spanBinaryExpr(pParse,@OP,&A,&Y);}
expr(A) ::= expr(A) BITAND|BITOR|LSHIFT|RSHIFT(OP) expr(Y).
                                        {spanBinaryExpr(pParse,@OP,&A,&Y);}
expr(A) ::= expr(A) PLUS|MINUS(OP) expr(Y).
                                        {spanBinaryExpr(pParse,@OP,&A,&Y);}
expr(A) ::= expr(A) STAR|SLASH|REM(OP) expr(Y).
                                        {spanBinaryExpr(pParse,@OP,&A,&Y);}
expr(A) ::= expr(A) CONCAT(OP) expr(Y). {spanBinaryExpr(pParse,@OP,&A,&Y);}
%type likeop {Token}
likeop(A) ::= LIKE_KW|MATCH(X).     {A=X;/*A-overwrites-X*/}
likeop(A) ::= NOT LIKE_KW|MATCH(X). {A=X; A.n|=0x80000000; /*A-overwrite-X*/}
expr(A) ::= expr(A) likeop(OP) expr(Y).  [LIKE_KW]  {
  ExprList *pList;
  int bNot = OP.n & 0x80000000;
  OP.n &= 0x7fffffff;
  pList = sql_expr_list_append(pParse->db,NULL, Y.pExpr);
  pList = sql_expr_list_append(pParse->db,pList, A.pExpr);
  A.pExpr = sqlite3ExprFunction(pParse, pList, &OP);
  exprNot(pParse, bNot, &A);
  A.zEnd = Y.zEnd;
  if( A.pExpr ) A.pExpr->flags |= EP_InfixFunc;
}
expr(A) ::= expr(A) likeop(OP) expr(Y) ESCAPE expr(E).  [LIKE_KW]  {
  ExprList *pList;
  int bNot = OP.n & 0x80000000;
  OP.n &= 0x7fffffff;
  pList = sql_expr_list_append(pParse->db,NULL, Y.pExpr);
  pList = sql_expr_list_append(pParse->db,pList, A.pExpr);
  pList = sql_expr_list_append(pParse->db,pList, E.pExpr);
  A.pExpr = sqlite3ExprFunction(pParse, pList, &OP);
  exprNot(pParse, bNot, &A);
  A.zEnd = E.zEnd;
  if( A.pExpr ) A.pExpr->flags |= EP_InfixFunc;
}

%include {
  /* Construct an expression node for a unary postfix operator
  */
  static void spanUnaryPostfix(
    Parse *pParse,         /* Parsing context to record errors */
    int op,                /* The operator */
    ExprSpan *pOperand,    /* The operand, and output */
    Token *pPostOp         /* The operand token for setting the span */
  ){
    pOperand->pExpr = sqlite3PExpr(pParse, op, pOperand->pExpr, 0);
    pOperand->zEnd = &pPostOp->z[pPostOp->n];
  }                           
}

// Tokens TK_ISNULL and TK_NOTNULL defined in extra tokens and are identifiers
// for operations IS NULL and IS NOT NULL.

expr(A) ::= expr(A) IS NULL(E).   {spanUnaryPostfix(pParse,TK_ISNULL,&A,&E);}
expr(A) ::= expr(A) IS NOT NULL(E).   {spanUnaryPostfix(pParse,TK_NOTNULL,&A,&E);}


%include {
  /* Construct an expression node for a unary prefix operator
  */
  static void spanUnaryPrefix(
    ExprSpan *pOut,        /* Write the new expression node here */
    Parse *pParse,         /* Parsing context to record errors */
    int op,                /* The operator */
    ExprSpan *pOperand,    /* The operand */
    Token *pPreOp         /* The operand token for setting the span */
  ){
    pOut->zStart = pPreOp->z;
    pOut->pExpr = sqlite3PExpr(pParse, op, pOperand->pExpr, 0);
    pOut->zEnd = pOperand->zEnd;
  }
}



expr(A) ::= NOT(B) expr(X).  
              {spanUnaryPrefix(&A,pParse,@B,&X,&B);/*A-overwrites-B*/}
expr(A) ::= BITNOT(B) expr(X).
              {spanUnaryPrefix(&A,pParse,@B,&X,&B);/*A-overwrites-B*/}
expr(A) ::= MINUS(B) expr(X). [BITNOT]
              {spanUnaryPrefix(&A,pParse,TK_UMINUS,&X,&B);/*A-overwrites-B*/}
expr(A) ::= PLUS(B) expr(X). [BITNOT]
              {spanUnaryPrefix(&A,pParse,TK_UPLUS,&X,&B);/*A-overwrites-B*/}

%type between_op {int}
between_op(A) ::= BETWEEN.     {A = 0;}
between_op(A) ::= NOT BETWEEN. {A = 1;}
expr(A) ::= expr(A) between_op(N) expr(X) AND expr(Y). [BETWEEN] {
  ExprList *pList = sql_expr_list_append(pParse->db,NULL, X.pExpr);
  pList = sql_expr_list_append(pParse->db,pList, Y.pExpr);
  A.pExpr = sqlite3PExpr(pParse, TK_BETWEEN, A.pExpr, 0);
  if( A.pExpr ){
    A.pExpr->x.pList = pList;
  }else{
    sql_expr_list_delete(pParse->db, pList);
  } 
  exprNot(pParse, N, &A);
  A.zEnd = Y.zEnd;
}
%type in_op {int}
in_op(A) ::= IN.      {A = 0;}
in_op(A) ::= NOT IN.  {A = 1;}
expr(A) ::= expr(A) in_op(N) LP exprlist(Y) RP(E). [IN] {
  if( Y==0 ){
    /* Expressions of the form
    **
    **      expr1 IN ()
    **      expr1 NOT IN ()
    **
    ** simplify to constants 0 (false) and 1 (true), respectively,
    ** regardless of the value of expr1.
    */
    sql_expr_delete(pParse->db, A.pExpr, false);
    A.pExpr = sqlite3ExprAlloc(pParse->db, TK_INTEGER,&sqlite3IntTokens[N],1);
  }else if( Y->nExpr==1 ){
    /* Expressions of the form:
    **
    **      expr1 IN (?1)
    **      expr1 NOT IN (?2)
    **
    ** with exactly one value on the RHS can be simplified to something
    ** like this:
    **
    **      expr1 == ?1
    **      expr1 <> ?2
    **
    ** But, the RHS of the == or <> is marked with the EP_Generic flag
    ** so that it may not contribute to the computation of comparison
    ** affinity or the collating sequence to use for comparison.  Otherwise,
    ** the semantics would be subtly different from IN or NOT IN.
    */
    Expr *pRHS = Y->a[0].pExpr;
    Y->a[0].pExpr = 0;
    sql_expr_list_delete(pParse->db, Y);
    /* pRHS cannot be NULL because a malloc error would have been detected
    ** before now and control would have never reached this point */
    if( ALWAYS(pRHS) ){
      pRHS->flags &= ~EP_Collate;
      pRHS->flags |= EP_Generic;
    }
    A.pExpr = sqlite3PExpr(pParse, N ? TK_NE : TK_EQ, A.pExpr, pRHS);
  }else{
    A.pExpr = sqlite3PExpr(pParse, TK_IN, A.pExpr, 0);
    if( A.pExpr ){
      A.pExpr->x.pList = Y;
      sqlite3ExprSetHeightAndFlags(pParse, A.pExpr);
    }else{
      sql_expr_list_delete(pParse->db, Y);
    }
    exprNot(pParse, N, &A);
  }
  A.zEnd = &E.z[E.n];
}
expr(A) ::= LP(B) select(X) RP(E). {
  spanSet(&A,&B,&E); /*A-overwrites-B*/
  A.pExpr = sqlite3PExpr(pParse, TK_SELECT, 0, 0);
  sqlite3PExprAddSelect(pParse, A.pExpr, X);
}
expr(A) ::= expr(A) in_op(N) LP select(Y) RP(E).  [IN] {
  A.pExpr = sqlite3PExpr(pParse, TK_IN, A.pExpr, 0);
  sqlite3PExprAddSelect(pParse, A.pExpr, Y);
  exprNot(pParse, N, &A);
  A.zEnd = &E.z[E.n];
}
expr(A) ::= expr(A) in_op(N) nm(Y) paren_exprlist(E). [IN] {
  SrcList *pSrc = sqlite3SrcListAppend(pParse->db, 0,&Y);
  Select *pSelect = sqlite3SelectNew(pParse, 0,pSrc,0,0,0,0,0,0,0);
  if( E )  sqlite3SrcListFuncArgs(pParse, pSelect ? pSrc : 0, E);
  A.pExpr = sqlite3PExpr(pParse, TK_IN, A.pExpr, 0);
  sqlite3PExprAddSelect(pParse, A.pExpr, pSelect);
  exprNot(pParse, N, &A);
  A.zEnd = &Y.z[Y.n];
}
expr(A) ::= EXISTS(B) LP select(Y) RP(E). {
  Expr *p;
  spanSet(&A,&B,&E); /*A-overwrites-B*/
  p = A.pExpr = sqlite3PExpr(pParse, TK_EXISTS, 0, 0);
  sqlite3PExprAddSelect(pParse, p, Y);
}

/* CASE expressions */
expr(A) ::= CASE(C) case_operand(X) case_exprlist(Y) case_else(Z) END(E). {
  spanSet(&A,&C,&E);  /*A-overwrites-C*/
  A.pExpr = sqlite3PExpr(pParse, TK_CASE, X, 0);
  if( A.pExpr ){
    A.pExpr->x.pList = Z ? sql_expr_list_append(pParse->db,Y,Z) : Y;
    sqlite3ExprSetHeightAndFlags(pParse, A.pExpr);
  }else{
    sql_expr_list_delete(pParse->db, Y);
    sql_expr_delete(pParse->db, Z, false);
  }
}
%type case_exprlist {ExprList*}
%destructor case_exprlist {sql_expr_list_delete(pParse->db, $$);}
case_exprlist(A) ::= case_exprlist(A) WHEN expr(Y) THEN expr(Z). {
  A = sql_expr_list_append(pParse->db,A, Y.pExpr);
  A = sql_expr_list_append(pParse->db,A, Z.pExpr);
}
case_exprlist(A) ::= WHEN expr(Y) THEN expr(Z). {
  A = sql_expr_list_append(pParse->db,NULL, Y.pExpr);
  A = sql_expr_list_append(pParse->db,A, Z.pExpr);
}
%type case_else {Expr*}
%destructor case_else {sql_expr_delete(pParse->db, $$, false);}
case_else(A) ::=  ELSE expr(X).         {A = X.pExpr;}
case_else(A) ::=  .                     {A = 0;} 
%type case_operand {Expr*}
%destructor case_operand {sql_expr_delete(pParse->db, $$, false);}
case_operand(A) ::= expr(X).            {A = X.pExpr; /*A-overwrites-X*/} 
case_operand(A) ::= .                   {A = 0;} 

%type exprlist {ExprList*}
%destructor exprlist {sql_expr_list_delete(pParse->db, $$);}
%type nexprlist {ExprList*}
%destructor nexprlist {sql_expr_list_delete(pParse->db, $$);}

exprlist(A) ::= nexprlist(A).
exprlist(A) ::= .                            {A = 0;}
nexprlist(A) ::= nexprlist(A) COMMA expr(Y).
    {A = sql_expr_list_append(pParse->db,A,Y.pExpr);}
nexprlist(A) ::= expr(Y).
    {A = sql_expr_list_append(pParse->db,NULL,Y.pExpr); /*A-overwrites-Y*/}

/* A paren_exprlist is an optional expression list contained inside
** of parenthesis */
%type paren_exprlist {ExprList*}
%destructor paren_exprlist {sql_expr_list_delete(pParse->db, $$);}
paren_exprlist(A) ::= .   {A = 0;}
paren_exprlist(A) ::= LP exprlist(X) RP.  {A = X;}


///////////////////////////// The CREATE INDEX command ///////////////////////
//
cmd ::= createkw(S) uniqueflag(U) INDEX ifnotexists(NE) nm(X)
        ON nm(Y) LP sortlist(Z) RP. {
  sql_create_index(pParse, &X, sqlite3SrcListAppend(pParse->db,0,&Y), Z, &S,
                   SORT_ORDER_ASC, NE, U);
}

%type uniqueflag {int}
uniqueflag(A) ::= UNIQUE.  {A = SQL_INDEX_TYPE_UNIQUE;}
uniqueflag(A) ::= .        {A = SQL_INDEX_TYPE_NON_UNIQUE;}


// The eidlist non-terminal (Expression Id List) generates an ExprList
// from a list of identifiers.  The identifier names are in ExprList.a[].zName.
// This list is stored in an ExprList rather than an IdList so that it
// can be easily sent to sqlite3ColumnsExprList().
//
// eidlist is grouped with CREATE INDEX because it used to be the non-terminal
// used for the arguments to an index.  That is just an historical accident.
//
%type eidlist {ExprList*}
%destructor eidlist {sql_expr_list_delete(pParse->db, $$);}
%type eidlist_opt {ExprList*}
%destructor eidlist_opt {sql_expr_list_delete(pParse->db, $$);}

%include {
  /* Add a single new term to an ExprList that is used to store a
  ** list of identifiers.  Report an error if the ID list contains
  ** a COLLATE clause or an ASC or DESC keyword, except ignore the
  ** error while parsing a legacy schema.
  */
  static ExprList *parserAddExprIdListTerm(
    Parse *pParse,
    ExprList *pPrior,
    Token *pIdToken,
    int hasCollate,
    int sortOrder
  ){
    ExprList *p = sql_expr_list_append(pParse->db, pPrior, NULL);
    if( (hasCollate || sortOrder != SORT_ORDER_UNDEF)
        && pParse->db->init.busy==0
    ){
      sqlite3ErrorMsg(pParse, "syntax error after column name \"%.*s\"",
                         pIdToken->n, pIdToken->z);
    }
    sqlite3ExprListSetName(pParse, p, pIdToken, 1);
    return p;
  }
} // end %include

eidlist_opt(A) ::= .                         {A = 0;}
eidlist_opt(A) ::= LP eidlist(X) RP.         {A = X;}
eidlist(A) ::= eidlist(A) COMMA nm(Y) collate(C) sortorder(Z).  {
  A = parserAddExprIdListTerm(pParse, A, &Y, C, Z);
}
eidlist(A) ::= nm(Y) collate(C) sortorder(Z). {
  A = parserAddExprIdListTerm(pParse, 0, &Y, C, Z); /*A-overwrites-Y*/
}

%type collate {int}
collate(C) ::= .              {C = 0;}
collate(C) ::= COLLATE id.   {C = 1;}


///////////////////////////// The DROP INDEX command /////////////////////////
//
cmd ::= DROP INDEX ifexists(E) fullname(X) ON nm(Y).   {
    sql_drop_index(pParse, X, &Y, E);
}

///////////////////////////// The PRAGMA command /////////////////////////////
//
cmd ::= PRAGMA nm(X).                        {
    sqlite3Pragma(pParse,&X,0,0,0);
}
cmd ::= PRAGMA nm(X) EQ nmnum(Y).  {
    sqlite3Pragma(pParse,&X,&Y,0,0);
}
cmd ::= PRAGMA nm(X) LP nmnum(Y) RP.         {
    sqlite3Pragma(pParse,&X,&Y,0,0);
}
cmd ::= PRAGMA nm(X) EQ minus_num(Y).        {
    sqlite3Pragma(pParse,&X,&Y,0,1);
}
cmd ::= PRAGMA nm(X) LP minus_num(Y) RP.     {
    sqlite3Pragma(pParse,&X,&Y,0,1);
}
cmd ::= PRAGMA nm(X) LP nm(Z) DOT nm(Y) RP.  {
    sqlite3Pragma(pParse,&X,&Y,&Z,0);
}
cmd ::= PRAGMA .                            {
    sqlite3Pragma(pParse, 0,0,0,0);
}

nmnum(A) ::= plus_num(A).
nmnum(A) ::= STRING(A).
nmnum(A) ::= nm(A).
nmnum(A) ::= ON(A).
nmnum(A) ::= DELETE(A).
nmnum(A) ::= DEFAULT(A).

%token_class number INTEGER|FLOAT.
plus_num(A) ::= PLUS number(X).       {A = X;}
plus_num(A) ::= number(A).
minus_num(A) ::= MINUS number(X).     {A = X;}
//////////////////////////// The CREATE TRIGGER command /////////////////////

cmd ::= createkw trigger_decl(A) BEGIN trigger_cmd_list(S) END(Z). {
  Token all;
  all.z = A.z;
  all.n = (int)(Z.z - A.z) + Z.n;
  pParse->initiateTTrans = false;
  sql_trigger_finish(pParse, S, &all);
}

trigger_decl(A) ::= TRIGGER ifnotexists(NOERR) nm(B)
                    trigger_time(C) trigger_event(D)
                    ON fullname(E) foreach_clause when_clause(G). {
  sql_trigger_begin(pParse, &B, C, D.a, D.b, E, G, NOERR);
  A = B; /*A-overwrites-T*/
}

%type trigger_time {int}
trigger_time(A) ::= BEFORE.      { A = TK_BEFORE; }
trigger_time(A) ::= AFTER.       { A = TK_AFTER;  }
trigger_time(A) ::= INSTEAD OF.  { A = TK_INSTEAD;}
trigger_time(A) ::= .            { A = TK_BEFORE; }

%type trigger_event {struct TrigEvent}
%destructor trigger_event {sqlite3IdListDelete(pParse->db, $$.b);}
trigger_event(A) ::= DELETE|INSERT(X).   {A.a = @X; /*A-overwrites-X*/ A.b = 0;}
trigger_event(A) ::= UPDATE(X).          {A.a = @X; /*A-overwrites-X*/ A.b = 0;}
trigger_event(A) ::= UPDATE OF idlist(X).{A.a = TK_UPDATE; A.b = X;}

foreach_clause ::= .
foreach_clause ::= FOR EACH ROW.

%type when_clause {Expr*}
%destructor when_clause {sql_expr_delete(pParse->db, $$, false);}
when_clause(A) ::= .             { A = 0; }
when_clause(A) ::= WHEN expr(X). { A = X.pExpr; }

%type trigger_cmd_list {TriggerStep*}
%destructor trigger_cmd_list {sqlite3DeleteTriggerStep(pParse->db, $$);}
trigger_cmd_list(A) ::= trigger_cmd_list(A) trigger_cmd(X) SEMI. {
  assert( A!=0 );
  A->pLast->pNext = X;
  A->pLast = X;
}
trigger_cmd_list(A) ::= trigger_cmd(A) SEMI. { 
  assert( A!=0 );
  A->pLast = A;
}

// Disallow qualified table names on INSERT, UPDATE, and DELETE statements
// within a trigger.  The table to INSERT, UPDATE, or DELETE is always in 
// the same database as the table that the trigger fires on.
//
%type trnm {Token}
trnm(A) ::= nm(A).
trnm(A) ::= nm DOT nm(X). {
  A = X;
  sqlite3ErrorMsg(pParse, 
        "qualified table names are not allowed on INSERT, UPDATE, and DELETE "
        "statements within triggers");
}

// Disallow the INDEX BY and NOT INDEXED clauses on UPDATE and DELETE
// statements within triggers.  We make a specific error message for this
// since it is an exception to the default grammar rules.
//
tridxby ::= .
tridxby ::= INDEXED BY nm. {
  sqlite3ErrorMsg(pParse,
        "the INDEXED BY clause is not allowed on UPDATE or DELETE statements "
        "within triggers");
}
tridxby ::= NOT INDEXED. {
  sqlite3ErrorMsg(pParse,
        "the NOT INDEXED clause is not allowed on UPDATE or DELETE statements "
        "within triggers");
}



%type trigger_cmd {TriggerStep*}
%destructor trigger_cmd {sqlite3DeleteTriggerStep(pParse->db, $$);}
// UPDATE 
trigger_cmd(A) ::=
   UPDATE orconf(R) trnm(X) tridxby SET setlist(Y) where_opt(Z).  
   {A = sqlite3TriggerUpdateStep(pParse->db, &X, Y, Z, R);}

// INSERT
trigger_cmd(A) ::= insert_cmd(R) INTO trnm(X) idlist_opt(F) select(S).
   {A = sqlite3TriggerInsertStep(pParse->db, &X, F, S, R);/*A-overwrites-R*/}

// DELETE
trigger_cmd(A) ::= DELETE FROM trnm(X) tridxby where_opt(Y).
   {A = sqlite3TriggerDeleteStep(pParse->db, &X, Y);}

// SELECT
trigger_cmd(A) ::= select(X).
   {A = sqlite3TriggerSelectStep(pParse->db, X); /*A-overwrites-X*/}

// The special RAISE expression that may occur in trigger programs
expr(A) ::= RAISE(X) LP IGNORE RP(Y).  {
  spanSet(&A,&X,&Y);  /*A-overwrites-X*/
  A.pExpr = sqlite3PExpr(pParse, TK_RAISE, 0, 0); 
  if( A.pExpr ){
    A.pExpr->on_conflict_action = ON_CONFLICT_ACTION_IGNORE;
  }
}
expr(A) ::= RAISE(X) LP raisetype(T) COMMA STRING(Z) RP(Y).  {
  spanSet(&A,&X,&Y);  /*A-overwrites-X*/
  A.pExpr = sqlite3ExprAlloc(pParse->db, TK_RAISE, &Z, 1);
  if( A.pExpr ) {
    A.pExpr->on_conflict_action = (enum on_conflict_action) T;
  }
}

%type raisetype {int}
raisetype(A) ::= ROLLBACK.  {A = ON_CONFLICT_ACTION_ROLLBACK;}
raisetype(A) ::= ABORT.     {A = ON_CONFLICT_ACTION_ABORT;}
raisetype(A) ::= FAIL.      {A = ON_CONFLICT_ACTION_FAIL;}


////////////////////////  DROP TRIGGER statement //////////////////////////////
cmd ::= DROP TRIGGER ifexists(NOERR) fullname(X). {
  sql_drop_trigger(pParse,X,NOERR);
}

/////////////////////////////////// ANALYZE ///////////////////////////////////
cmd ::= ANALYZE.                {sqlite3Analyze(pParse, 0);}
cmd ::= ANALYZE nm(X).          {sqlite3Analyze(pParse, &X);}

//////////////////////// ALTER TABLE table ... ////////////////////////////////
cmd ::= ALTER TABLE fullname(X) RENAME TO nm(Z). {
  sql_alter_table_rename(pParse,X,&Z);
}

cmd ::= ALTER TABLE fullname(X) ADD CONSTRAINT nm(Z) FOREIGN KEY
        LP eidlist(FA) RP REFERENCES nm(T) eidlist_opt(TA) refargs(R)
        defer_subclause_opt(D). {
    sql_create_foreign_key(pParse, X, &Z, FA, &T, TA, D, R);
}

cmd ::= ALTER TABLE fullname(X) DROP CONSTRAINT nm(Z). {
    sql_drop_foreign_key(pParse, X, &Z);
}

//////////////////////// COMMON TABLE EXPRESSIONS ////////////////////////////
%type with {With*}
%type wqlist {With*}
%destructor with {sqlite3WithDelete(pParse->db, $$);}
%destructor wqlist {sqlite3WithDelete(pParse->db, $$);}

with(A) ::= . {A = 0;}
%ifndef SQLITE_OMIT_CTE
with(A) ::= WITH wqlist(W).              { A = W; }
with(A) ::= WITH RECURSIVE wqlist(W).    { A = W; }

wqlist(A) ::= nm(X) eidlist_opt(Y) AS LP select(Z) RP. {
  A = sqlite3WithAdd(pParse, 0, &X, Y, Z); /*A-overwrites-X*/
}
wqlist(A) ::= wqlist(A) COMMA nm(X) eidlist_opt(Y) AS LP select(Z) RP. {
  A = sqlite3WithAdd(pParse, A, &X, Y, Z);
}
%endif  SQLITE_OMIT_CTE

////////////////////////////// TYPE DECLARATION ///////////////////////////////
%type typedef {struct type_def}
typedef(A) ::= TEXT . { A.type = FIELD_TYPE_STRING; }
typedef(A) ::= BLOB . { A.type = FIELD_TYPE_SCALAR; }
typedef(A) ::= DATE . { A.type = FIELD_TYPE_NUMBER; }
typedef(A) ::= TIME . { A.type = FIELD_TYPE_NUMBER; }
typedef(A) ::= DATETIME . { A.type = FIELD_TYPE_NUMBER; }

%type char_len {int}
typedef(A) ::= CHAR . {
  A.type = FIELD_TYPE_STRING;
}

char_len(A) ::= LP INTEGER(B) RP . {
  (void) A;
  (void) B;
}

typedef(A) ::= CHAR char_len(B) . {
  A.type = FIELD_TYPE_STRING;
  (void) B;
}

typedef(A) ::= VARCHAR char_len(B) . {
  A.type = FIELD_TYPE_STRING;
  (void) B;
}

%type number_typedef {struct type_def}
typedef(A) ::= number_typedef(A) .
number_typedef(A) ::= FLOAT|REAL|DOUBLE . { A.type = FIELD_TYPE_NUMBER; }
number_typedef(A) ::= INT|INTEGER . { A.type = FIELD_TYPE_INTEGER; }

%type number_len_typedef {struct type_def}
number_typedef(A) ::= DECIMAL|NUMERIC|NUM number_len_typedef(B) . {
  A.type = FIELD_TYPE_NUMBER;
  (void) B;
}

number_len_typedef(A) ::= . { (void) A; }
number_len_typedef(A) ::= LP INTEGER(B) RP . {
  (void) A;
  (void) B;
}

number_len_typedef(A) ::= LP INTEGER(B) COMMA INTEGER(C) RP . {
  (void) A;
  (void) B;
  (void) C;
}
