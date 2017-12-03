/***** This file contains automatically generated code ******
**
** The code in this file has been automatically generated by
**
**   sqlite/tool/mkkeywordhash.c
**
** The code in this file implements a function that determines whether
** or not a given identifier is really an SQL keyword.  The same thing
** might be implemented more directly using a hand-written hash table.
** But by using this automatically generated code, the size of the code
** is substantially reduced.  This is important for embedded applications
** on platforms with limited memory.
*/
/* Hash score: 278 */
static int keywordCode(const char *z, int n, int *pType, bool *pFlag){
  /* zText[] encodes 1206 bytes of keywords in 783 bytes */
  /*   REINDEXEDECIMALTEREGEXPLAINOUTERELEASENSITIVEACHARACTERAISE        */
  /*   LECTABLEAVELSEIFOREIGNOREFERENCESCAPENDECLARESIGNALIKEYBEFORE      */
  /*   VOKEXCEPTHENATURALIMITERATEXISTSAVEPOINTEGERANGETRANSACTION        */
  /*   OTNULLEFTRIGGEREADSMALLINTERSECTVARCHARECURSIVECASECONSTRAINTO     */
  /*   FFSETUNIQUERYWITHOUTBEGINSTEADDEFERRABLEBETWEENCASCADESCRIBE       */
  /*   CASTARTCOMMITCURSORDERENAMEDOUBLEFETCHECKGROUPDATEJOINNEREPEAT     */
  /*   MATCHAVINGLOBINARYPLANALYZEPRAGMABORTPROCEDUREPLACEVALUES          */
  /*   PECIFICALLOCALTIMESTAMPARTITIONWHERESTRICTWHILEAFTERETURNAND       */
  /*   EFAULTAUTOINCREMENTCOLLATECOLUMNCONDITIONCONFLICTCONNECTCREATE     */
  /*   CROSSQLOOPRECISIONCURRENT_DATECURRENT_TIMESTAMPRIMARY              */
  /*   CURRENT_USERIGHTDEFERREDELETEDENSE_RANKDETERMINISTICDISTINCT       */
  /*   DROPFAILFLOATFROMFUNCTIONGRANTIMMEDIATEINSENSITIVEINSERTISNULL     */
  /*   OVEROLLBACKROWSYSTEMROW_NUMBERUNIONUSINGVIEWHENEVERANYBY           */
  /*   INITIALLY                                                          */
  static const char zText[782] = {
    'R','E','I','N','D','E','X','E','D','E','C','I','M','A','L','T','E','R',
    'E','G','E','X','P','L','A','I','N','O','U','T','E','R','E','L','E','A',
    'S','E','N','S','I','T','I','V','E','A','C','H','A','R','A','C','T','E',
    'R','A','I','S','E','L','E','C','T','A','B','L','E','A','V','E','L','S',
    'E','I','F','O','R','E','I','G','N','O','R','E','F','E','R','E','N','C',
    'E','S','C','A','P','E','N','D','E','C','L','A','R','E','S','I','G','N',
    'A','L','I','K','E','Y','B','E','F','O','R','E','V','O','K','E','X','C',
    'E','P','T','H','E','N','A','T','U','R','A','L','I','M','I','T','E','R',
    'A','T','E','X','I','S','T','S','A','V','E','P','O','I','N','T','E','G',
    'E','R','A','N','G','E','T','R','A','N','S','A','C','T','I','O','N','O',
    'T','N','U','L','L','E','F','T','R','I','G','G','E','R','E','A','D','S',
    'M','A','L','L','I','N','T','E','R','S','E','C','T','V','A','R','C','H',
    'A','R','E','C','U','R','S','I','V','E','C','A','S','E','C','O','N','S',
    'T','R','A','I','N','T','O','F','F','S','E','T','U','N','I','Q','U','E',
    'R','Y','W','I','T','H','O','U','T','B','E','G','I','N','S','T','E','A',
    'D','D','E','F','E','R','R','A','B','L','E','B','E','T','W','E','E','N',
    'C','A','S','C','A','D','E','S','C','R','I','B','E','C','A','S','T','A',
    'R','T','C','O','M','M','I','T','C','U','R','S','O','R','D','E','R','E',
    'N','A','M','E','D','O','U','B','L','E','F','E','T','C','H','E','C','K',
    'G','R','O','U','P','D','A','T','E','J','O','I','N','N','E','R','E','P',
    'E','A','T','M','A','T','C','H','A','V','I','N','G','L','O','B','I','N',
    'A','R','Y','P','L','A','N','A','L','Y','Z','E','P','R','A','G','M','A',
    'B','O','R','T','P','R','O','C','E','D','U','R','E','P','L','A','C','E',
    'V','A','L','U','E','S','P','E','C','I','F','I','C','A','L','L','O','C',
    'A','L','T','I','M','E','S','T','A','M','P','A','R','T','I','T','I','O',
    'N','W','H','E','R','E','S','T','R','I','C','T','W','H','I','L','E','A',
    'F','T','E','R','E','T','U','R','N','A','N','D','E','F','A','U','L','T',
    'A','U','T','O','I','N','C','R','E','M','E','N','T','C','O','L','L','A',
    'T','E','C','O','L','U','M','N','C','O','N','D','I','T','I','O','N','C',
    'O','N','F','L','I','C','T','C','O','N','N','E','C','T','C','R','E','A',
    'T','E','C','R','O','S','S','Q','L','O','O','P','R','E','C','I','S','I',
    'O','N','C','U','R','R','E','N','T','_','D','A','T','E','C','U','R','R',
    'E','N','T','_','T','I','M','E','S','T','A','M','P','R','I','M','A','R',
    'Y','C','U','R','R','E','N','T','_','U','S','E','R','I','G','H','T','D',
    'E','F','E','R','R','E','D','E','L','E','T','E','D','E','N','S','E','_',
    'R','A','N','K','D','E','T','E','R','M','I','N','I','S','T','I','C','D',
    'I','S','T','I','N','C','T','D','R','O','P','F','A','I','L','F','L','O',
    'A','T','F','R','O','M','F','U','N','C','T','I','O','N','G','R','A','N',
    'T','I','M','M','E','D','I','A','T','E','I','N','S','E','N','S','I','T',
    'I','V','E','I','N','S','E','R','T','I','S','N','U','L','L','O','V','E',
    'R','O','L','L','B','A','C','K','R','O','W','S','Y','S','T','E','M','R',
    'O','W','_','N','U','M','B','E','R','U','N','I','O','N','U','S','I','N',
    'G','V','I','E','W','H','E','N','E','V','E','R','A','N','Y','B','Y','I',
    'N','I','T','I','A','L','L','Y',
  };
  static const unsigned short aHash[128] = {
     154, 159, 167, 129, 115,   0, 137, 104,   0,  94,   0, 127,  64,
     161,  98,  72,   0, 138, 168,  95, 162, 160, 109,   0,  56,  42,
      19, 164, 112,   0,  37, 119,  20,  34,   0,   0, 120, 125,   0,
      60,  13,   0,  67, 116, 135,   0, 163, 134,   0,   0,   0,   0,
     143,  74,   0,  78,  29, 166,   0, 131,   0,  79,  96,  21, 121,
     149,   0, 172, 147, 152, 171,  71,  88,  89,   0, 117, 107, 133,
      36, 122, 126,   0,   0,  14,  80, 118, 136, 132, 148,   7, 151,
     150,  83, 101,  15,  12, 103, 170,  44,  70, 165, 100, 157,  50,
      53,  39, 155,   0, 169, 105, 158,  84,   0,  57,   0,   0,  48,
      43, 113,  92,  52,   0,  18,  49,   0,  90, 156, 145,
  };
  static const unsigned short aNext[172] = {
       0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
       4,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
       0,   0,   0,   0,   0,  26,   0,  11,   0,   0,   6,   0,   0,
       0,   0,   0,   0,  16,   0,   0,   0,   0,  38,   0,   0,   0,
      24,   0,   0,   0,   0,   0,   0,   1,   0,   0,   0,  40,   0,
       8,   0,   0,  10,   0,   0,   2,   0,   0,   0,   0,   0,   0,
      27,   0,   0,   0,   0,   9,  35,   0,   0,  47,   3,  66,  63,
      51,   0,  58,  65,   0,   0,  31,   0,  97,   0,  91,  69,  68,
      62,  55,   0,  59,  17,   0,   0,   0, 111,   5,  30,  73,  41,
      22,   0,  76,   0,   0,   0, 114,  87,   0,   0,   0, 110,   0,
      28, 124,  23, 130,  99,  82,   0,  46,   0,  86,   0,  25, 139,
       0,   0,  33, 146, 123,   0, 141,  61,  75,   0,   0,   0,   0,
     102, 108,  54,  81, 142, 106,  77,  32, 128, 140, 153,  93, 144,
      85,   0,  45,
  };
  static const unsigned char aLen[172] = {
       7,   7,   5,   7,   5,   6,   7,   5,   5,   7,  10,   2,   4,
       9,   4,   5,   6,   5,   5,   6,   4,   7,   3,   2,   6,  10,
       6,   3,   7,   8,   6,   4,   3,   6,   6,   6,   4,   7,   5,
       7,   6,   9,   7,   5,   3,  11,   6,   2,   7,   3,   2,   4,
       4,   7,   5,   8,   9,   7,   9,   4,  10,   4,   6,   2,   3,
       6,   5,   7,   4,   3,   5,   7,   3,  10,   7,   7,   3,   8,
       4,   4,   5,   6,   6,   5,   6,   6,   5,   5,   5,   6,   4,
       5,   6,   5,   6,   4,   6,   4,   7,   6,   5,   9,   7,   6,
       8,   2,   4,  14,   9,   9,   5,   8,   5,   5,   6,   3,   7,
      13,   2,   7,   6,   9,   8,   7,   6,   5,   3,   4,   9,  12,
       4,   7,  17,  12,   7,  12,   4,   5,   8,   6,  10,   4,  13,
       2,   2,   8,   4,   4,   5,   4,   8,   5,   9,  11,   9,   6,
       6,   4,   8,   4,   6,  10,   3,   5,   5,   4,   8,   4,   3,
       2,   9,   3,
  };
  static const unsigned short int aOffset[172] = {
       0,   2,   2,   8,  13,  17,  20,  25,  27,  31,  35,  35,  44,
      46,  46,  54,  57,  62,  65,  69,  69,  74,  74,  75,  78,  82,
      90,  95,  97, 102, 104, 109, 111, 114, 118, 123, 128, 131, 137,
     140, 146, 151, 157, 163, 166, 168, 173, 177, 178, 178, 178, 181,
     184, 187, 193, 197, 202, 211, 217, 226, 230, 237, 240, 240, 243,
     246, 249, 254, 254, 258, 261, 264, 269, 271, 281, 288, 289, 293,
     293, 301, 303, 308, 314, 318, 322, 328, 334, 337, 342, 345, 351,
     353, 357, 363, 367, 372, 375, 381, 383, 390, 395, 400, 407, 414,
     419, 423, 426, 429, 429, 442, 451, 454, 462, 467, 471, 477, 479,
     486, 488, 499, 506, 512, 521, 529, 536, 542, 546, 548, 551, 560,
     568, 572, 572, 572, 588, 595, 603, 606, 611, 618, 624, 630, 634,
     640, 642, 647, 655, 659, 663, 668, 672, 680, 685, 694, 696, 705,
     711, 717, 720, 728, 731, 737, 737, 747, 752, 757, 760, 760, 768,
     771, 773, 778,
  };
  static const unsigned char aCode[172] = {
    TK_REINDEX,    TK_INDEXED,    TK_INDEX,      TK_ID,         TK_ALTER,      
    TK_LIKE_KW,    TK_EXPLAIN,    TK_STANDARD,   TK_JOIN_KW,    TK_RELEASE,    
    TK_STANDARD,   TK_AS,         TK_EACH,       TK_ID,         TK_ID,         
    TK_RAISE,      TK_SELECT,     TK_TABLE,      TK_STANDARD,   TK_STANDARD,   
    TK_ELSE,       TK_FOREIGN,    TK_FOR,        TK_OR,         TK_IGNORE,     
    TK_REFERENCES, TK_ESCAPE,     TK_END,        TK_STANDARD,   TK_STANDARD,   
    TK_STANDARD,   TK_LIKE_KW,    TK_KEY,        TK_BEFORE,     TK_STANDARD,   
    TK_EXCEPT,     TK_THEN,       TK_JOIN_KW,    TK_LIMIT,      TK_STANDARD,   
    TK_EXISTS,     TK_SAVEPOINT,  TK_ID,         TK_STANDARD,   TK_STANDARD,   
    TK_TRANSACTION,TK_ACTION,     TK_ON,         TK_NOTNULL,    TK_NOT,        
    TK_NO,         TK_NULL,       TK_JOIN_KW,    TK_TRIGGER,    TK_STANDARD,   
    TK_ID,         TK_INTERSECT,  TK_ID,         TK_RECURSIVE,  TK_CASE,       
    TK_CONSTRAINT, TK_INTO,       TK_OFFSET,     TK_OF,         TK_SET,        
    TK_UNIQUE,     TK_QUERY,      TK_WITHOUT,    TK_WITH,       TK_STANDARD,   
    TK_BEGIN,      TK_INSTEAD,    TK_ADD,        TK_DEFERRABLE, TK_BETWEEN,    
    TK_CASCADE,    TK_ASC,        TK_STANDARD,   TK_DESC,       TK_CAST,       
    TK_STANDARD,   TK_COMMIT,     TK_STANDARD,   TK_ORDER,      TK_RENAME,     
    TK_ID,         TK_STANDARD,   TK_CHECK,      TK_GROUP,      TK_UPDATE,     
    TK_JOIN,       TK_JOIN_KW,    TK_STANDARD,   TK_MATCH,      TK_HAVING,     
    TK_LIKE_KW,    TK_ID,         TK_PLAN,       TK_ANALYZE,    TK_PRAGMA,     
    TK_ABORT,      TK_STANDARD,   TK_REPLACE,    TK_VALUES,     TK_STANDARD,   
    TK_IF,         TK_STANDARD,   TK_STANDARD,   TK_STANDARD,   TK_STANDARD,   
    TK_WHERE,      TK_RESTRICT,   TK_STANDARD,   TK_AFTER,      TK_STANDARD,   
    TK_AND,        TK_DEFAULT,    TK_AUTOINCR,   TK_TO,         TK_COLLATE,    
    TK_COLUMNKW,   TK_STANDARD,   TK_CONFLICT,   TK_STANDARD,   TK_CREATE,     
    TK_JOIN_KW,    TK_STANDARD,   TK_STANDARD,   TK_STANDARD,   TK_CTIME_KW,   
    TK_ID,         TK_STANDARD,   TK_CTIME_KW,   TK_CTIME_KW,   TK_PRIMARY,    
    TK_STANDARD,   TK_STANDARD,   TK_JOIN_KW,    TK_DEFERRED,   TK_DELETE,     
    TK_STANDARD,   TK_STANDARD,   TK_STANDARD,   TK_IN,         TK_IS,         
    TK_DISTINCT,   TK_DROP,       TK_FAIL,       TK_ID,         TK_FROM,       
    TK_STANDARD,   TK_STANDARD,   TK_IMMEDIATE,  TK_STANDARD,   TK_STANDARD,   
    TK_INSERT,     TK_ISNULL,     TK_STANDARD,   TK_ROLLBACK,   TK_STANDARD,   
    TK_STANDARD,   TK_STANDARD,   TK_ROW,        TK_UNION,      TK_USING,      
    TK_VIEW,       TK_STANDARD,   TK_WHEN,       TK_STANDARD,   TK_BY,         
    TK_INITIALLY,  TK_ALL,        
  };
  static const bool aFlag[172] = {
    true,          false,         true,          true,          true,          
    false,         true,          true,          true,          true,          
    true,          true,          true,          true,          true,          
    false,         true,          true,          true,          true,          
    true,          true,          true,          true,          false,         
    true,          true,          true,          true,          true,          
    true,          true,          false,         false,         true,          
    true,          true,          true,          false,         true,          
    true,          true,          true,          true,          true,          
    true,          false,         true,          false,         true,          
    false,         true,          true,          true,          true,          
    true,          true,          true,          true,          true,          
    true,          true,          false,         true,          true,          
    true,          false,         false,         true,          true,          
    true,          false,         false,         false,         true,          
    false,         true,          true,          true,          false,         
    true,          true,          true,          true,          true,          
    true,          true,          true,          true,          true,          
    true,          true,          true,          true,          true,          
    false,         true,          false,         true,          true,          
    false,         true,          true,          true,          true,          
    true,          true,          true,          true,          true,          
    true,          false,         true,          false,         true,          
    true,          true,          false,         true,          true,          
    true,          true,          false,         true,          true,          
    true,          true,          true,          true,          true,          
    true,          true,          true,          true,          true,          
    true,          true,          true,          false,         true,          
    true,          true,          true,          true,          true,          
    true,          true,          false,         true,          true,          
    true,          true,          true,          true,          true,          
    true,          false,         true,          true,          true,          
    true,          true,          true,          true,          true,          
    true,          true,          true,          true,          true,          
    false,         true,          
  };
  int i, j;
  const char *zKW;
  if( n>=2 ){
    i = ((charMap(z[0])*4) ^ (charMap(z[n-1])*3) ^ n) % 128;
    for(i=((int)aHash[i])-1; i>=0; i=((int)aNext[i])-1){
      if( aLen[i]!=n ) continue;
      j = 0;
      zKW = &zText[aOffset[i]];
#ifdef SQLITE_ASCII
      while( j<n && (z[j]&~0x20)==zKW[j] ){ j++; }
#endif
#ifdef SQLITE_EBCDIC
      while( j<n && toupper(z[j])==zKW[j] ){ j++; }
#endif
      if( j<n ) continue;
      testcase( i==0 ); /* REINDEX */
      testcase( i==1 ); /* INDEXED */
      testcase( i==2 ); /* INDEX */
      testcase( i==3 ); /* DECIMAL */
      testcase( i==4 ); /* ALTER */
      testcase( i==5 ); /* REGEXP */
      testcase( i==6 ); /* EXPLAIN */
      testcase( i==7 ); /* INOUT */
      testcase( i==8 ); /* OUTER */
      testcase( i==9 ); /* RELEASE */
      testcase( i==10 ); /* ASENSITIVE */
      testcase( i==11 ); /* AS */
      testcase( i==12 ); /* EACH */
      testcase( i==13 ); /* CHARACTER */
      testcase( i==14 ); /* CHAR */
      testcase( i==15 ); /* RAISE */
      testcase( i==16 ); /* SELECT */
      testcase( i==17 ); /* TABLE */
      testcase( i==18 ); /* LEAVE */
      testcase( i==19 ); /* ELSEIF */
      testcase( i==20 ); /* ELSE */
      testcase( i==21 ); /* FOREIGN */
      testcase( i==22 ); /* FOR */
      testcase( i==23 ); /* OR */
      testcase( i==24 ); /* IGNORE */
      testcase( i==25 ); /* REFERENCES */
      testcase( i==26 ); /* ESCAPE */
      testcase( i==27 ); /* END */
      testcase( i==28 ); /* DECLARE */
      testcase( i==29 ); /* RESIGNAL */
      testcase( i==30 ); /* SIGNAL */
      testcase( i==31 ); /* LIKE */
      testcase( i==32 ); /* KEY */
      testcase( i==33 ); /* BEFORE */
      testcase( i==34 ); /* REVOKE */
      testcase( i==35 ); /* EXCEPT */
      testcase( i==36 ); /* THEN */
      testcase( i==37 ); /* NATURAL */
      testcase( i==38 ); /* LIMIT */
      testcase( i==39 ); /* ITERATE */
      testcase( i==40 ); /* EXISTS */
      testcase( i==41 ); /* SAVEPOINT */
      testcase( i==42 ); /* INTEGER */
      testcase( i==43 ); /* RANGE */
      testcase( i==44 ); /* GET */
      testcase( i==45 ); /* TRANSACTION */
      testcase( i==46 ); /* ACTION */
      testcase( i==47 ); /* ON */
      testcase( i==48 ); /* NOTNULL */
      testcase( i==49 ); /* NOT */
      testcase( i==50 ); /* NO */
      testcase( i==51 ); /* NULL */
      testcase( i==52 ); /* LEFT */
      testcase( i==53 ); /* TRIGGER */
      testcase( i==54 ); /* READS */
      testcase( i==55 ); /* SMALLINT */
      testcase( i==56 ); /* INTERSECT */
      testcase( i==57 ); /* VARCHAR */
      testcase( i==58 ); /* RECURSIVE */
      testcase( i==59 ); /* CASE */
      testcase( i==60 ); /* CONSTRAINT */
      testcase( i==61 ); /* INTO */
      testcase( i==62 ); /* OFFSET */
      testcase( i==63 ); /* OF */
      testcase( i==64 ); /* SET */
      testcase( i==65 ); /* UNIQUE */
      testcase( i==66 ); /* QUERY */
      testcase( i==67 ); /* WITHOUT */
      testcase( i==68 ); /* WITH */
      testcase( i==69 ); /* OUT */
      testcase( i==70 ); /* BEGIN */
      testcase( i==71 ); /* INSTEAD */
      testcase( i==72 ); /* ADD */
      testcase( i==73 ); /* DEFERRABLE */
      testcase( i==74 ); /* BETWEEN */
      testcase( i==75 ); /* CASCADE */
      testcase( i==76 ); /* ASC */
      testcase( i==77 ); /* DESCRIBE */
      testcase( i==78 ); /* DESC */
      testcase( i==79 ); /* CAST */
      testcase( i==80 ); /* START */
      testcase( i==81 ); /* COMMIT */
      testcase( i==82 ); /* CURSOR */
      testcase( i==83 ); /* ORDER */
      testcase( i==84 ); /* RENAME */
      testcase( i==85 ); /* DOUBLE */
      testcase( i==86 ); /* FETCH */
      testcase( i==87 ); /* CHECK */
      testcase( i==88 ); /* GROUP */
      testcase( i==89 ); /* UPDATE */
      testcase( i==90 ); /* JOIN */
      testcase( i==91 ); /* INNER */
      testcase( i==92 ); /* REPEAT */
      testcase( i==93 ); /* MATCH */
      testcase( i==94 ); /* HAVING */
      testcase( i==95 ); /* GLOB */
      testcase( i==96 ); /* BINARY */
      testcase( i==97 ); /* PLAN */
      testcase( i==98 ); /* ANALYZE */
      testcase( i==99 ); /* PRAGMA */
      testcase( i==100 ); /* ABORT */
      testcase( i==101 ); /* PROCEDURE */
      testcase( i==102 ); /* REPLACE */
      testcase( i==103 ); /* VALUES */
      testcase( i==104 ); /* SPECIFIC */
      testcase( i==105 ); /* IF */
      testcase( i==106 ); /* CALL */
      testcase( i==107 ); /* LOCALTIMESTAMP */
      testcase( i==108 ); /* LOCALTIME */
      testcase( i==109 ); /* PARTITION */
      testcase( i==110 ); /* WHERE */
      testcase( i==111 ); /* RESTRICT */
      testcase( i==112 ); /* WHILE */
      testcase( i==113 ); /* AFTER */
      testcase( i==114 ); /* RETURN */
      testcase( i==115 ); /* AND */
      testcase( i==116 ); /* DEFAULT */
      testcase( i==117 ); /* AUTOINCREMENT */
      testcase( i==118 ); /* TO */
      testcase( i==119 ); /* COLLATE */
      testcase( i==120 ); /* COLUMN */
      testcase( i==121 ); /* CONDITION */
      testcase( i==122 ); /* CONFLICT */
      testcase( i==123 ); /* CONNECT */
      testcase( i==124 ); /* CREATE */
      testcase( i==125 ); /* CROSS */
      testcase( i==126 ); /* SQL */
      testcase( i==127 ); /* LOOP */
      testcase( i==128 ); /* PRECISION */
      testcase( i==129 ); /* CURRENT_DATE */
      testcase( i==130 ); /* DATE */
      testcase( i==131 ); /* CURRENT */
      testcase( i==132 ); /* CURRENT_TIMESTAMP */
      testcase( i==133 ); /* CURRENT_TIME */
      testcase( i==134 ); /* PRIMARY */
      testcase( i==135 ); /* CURRENT_USER */
      testcase( i==136 ); /* USER */
      testcase( i==137 ); /* RIGHT */
      testcase( i==138 ); /* DEFERRED */
      testcase( i==139 ); /* DELETE */
      testcase( i==140 ); /* DENSE_RANK */
      testcase( i==141 ); /* RANK */
      testcase( i==142 ); /* DETERMINISTIC */
      testcase( i==143 ); /* IN */
      testcase( i==144 ); /* IS */
      testcase( i==145 ); /* DISTINCT */
      testcase( i==146 ); /* DROP */
      testcase( i==147 ); /* FAIL */
      testcase( i==148 ); /* FLOAT */
      testcase( i==149 ); /* FROM */
      testcase( i==150 ); /* FUNCTION */
      testcase( i==151 ); /* GRANT */
      testcase( i==152 ); /* IMMEDIATE */
      testcase( i==153 ); /* INSENSITIVE */
      testcase( i==154 ); /* SENSITIVE */
      testcase( i==155 ); /* INSERT */
      testcase( i==156 ); /* ISNULL */
      testcase( i==157 ); /* OVER */
      testcase( i==158 ); /* ROLLBACK */
      testcase( i==159 ); /* ROWS */
      testcase( i==160 ); /* SYSTEM */
      testcase( i==161 ); /* ROW_NUMBER */
      testcase( i==162 ); /* ROW */
      testcase( i==163 ); /* UNION */
      testcase( i==164 ); /* USING */
      testcase( i==165 ); /* VIEW */
      testcase( i==166 ); /* WHENEVER */
      testcase( i==167 ); /* WHEN */
      testcase( i==168 ); /* ANY */
      testcase( i==169 ); /* BY */
      testcase( i==170 ); /* INITIALLY */
      testcase( i==171 ); /* ALL */
      *pType = aCode[i];
      if (pFlag) {
        *pFlag = aFlag[i];
      }
      break;
    }
  }
  return n;
}
int sqlite3KeywordCode(const unsigned char *z, int n){
  int id = TK_ID;
  keywordCode((char*)z, n, &id, NULL);
  return id;
}
#define SQLITE_N_KEYWORD 172