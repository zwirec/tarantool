/*
 * Copyright 2010-2017, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/*
 * This file contains the C-language implementations for many of the SQL
 * functions of SQLite.  (Some function, and in particular the date and
 * time functions, are implemented separately.)
 */
#include "sqliteInt.h"
#include "vdbeInt.h"
#include "version.h"
#include "coll.h"
#include <unicode/ustring.h>
#include <unicode/ucasemap.h>
#include <unicode/ucnv.h>
#include <unicode/uchar.h>
#include <unicode/ucol.h>

static UConverter* pUtf8conv;

/*
 * Return the collating function associated with a function.
 */
static struct coll *
sqlite3GetFuncCollSeq(sqlite3_context * context)
{
	VdbeOp *pOp;
	assert(context->pVdbe != 0);
	pOp = &context->pVdbe->aOp[context->iOp - 1];
	assert(pOp->opcode == OP_CollSeq);
	assert(pOp->p4type == P4_COLLSEQ || pOp->p4.pColl == NULL);
	return pOp->p4.pColl;
}

/*
 * Indicate that the accumulator load should be skipped on this
 * iteration of the aggregate loop.
 */
static void
sqlite3SkipAccumulatorLoad(sqlite3_context * context)
{
	context->skipFlag = 1;
}

/*
 * Implementation of the non-aggregate min() and max() functions
 */
static void
minmaxFunc(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	int i;
	int mask;		/* 0 for min() or 0xffffffff for max() */
	int iBest;
	struct coll *pColl;

	assert(argc > 1);
	mask = sqlite3_user_data(context) == 0 ? 0 : -1;
	pColl = sqlite3GetFuncCollSeq(context);
	assert(mask == -1 || mask == 0);
	iBest = 0;
	if (sqlite3_value_type(argv[0]) == SQLITE_NULL)
		return;
	for (i = 1; i < argc; i++) {
		if (sqlite3_value_type(argv[i]) == SQLITE_NULL)
			return;
		if ((sqlite3MemCompare(argv[iBest], argv[i], pColl) ^ mask) >=
		    0) {
			testcase(mask == 0);
			iBest = i;
		}
	}
	sqlite3_result_value(context, argv[iBest]);
}

/*
 * Return the type of the argument.
 */
static void
typeofFunc(sqlite3_context * context, int NotUsed, sqlite3_value ** argv)
{
	const char *z = 0;
	UNUSED_PARAMETER(NotUsed);
	switch (sqlite3_value_type(argv[0])) {
	case SQLITE_INTEGER:
		z = "integer";
		break;
	case SQLITE_TEXT:
		z = "text";
		break;
	case SQLITE_FLOAT:
		z = "real";
		break;
	case SQLITE_BLOB:
		z = "blob";
		break;
	default:
		z = "null";
		break;
	}
	sqlite3_result_text(context, z, -1, SQLITE_STATIC);
}

/*
 * Implementation of the length() function
 */
static void
lengthFunc(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	int len;

	assert(argc == 1);
	UNUSED_PARAMETER(argc);
	switch (sqlite3_value_type(argv[0])) {
	case SQLITE_BLOB:
	case SQLITE_INTEGER:
	case SQLITE_FLOAT:{
			sqlite3_result_int(context,
					   sqlite3_value_bytes(argv[0]));
			break;
		}
	case SQLITE_TEXT:{
			const unsigned char *z = sqlite3_value_text(argv[0]);
			if (z == 0)
				return;
			len = 0;
			while (*z) {
				len++;
				SQLITE_SKIP_UTF8(z);
			}
			sqlite3_result_int(context, len);
			break;
		}
	default:{
			sqlite3_result_null(context);
			break;
		}
	}
}

/*
 * Implementation of the abs() function.
 *
 * IMP: R-23979-26855 The abs(X) function returns the absolute value of
 * the numeric argument X.
 */
static void
absFunc(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	assert(argc == 1);
	UNUSED_PARAMETER(argc);
	switch (sqlite3_value_type(argv[0])) {
	case SQLITE_INTEGER:{
			i64 iVal = sqlite3_value_int64(argv[0]);
			if (iVal < 0) {
				if (iVal == SMALLEST_INT64) {
					/* IMP: R-31676-45509 If X is the integer -9223372036854775808
					 * then abs(X) throws an integer overflow error since there is no
					 * equivalent positive 64-bit two complement value.
					 */
					sqlite3_result_error(context,
							     "integer overflow",
							     -1);
					return;
				}
				iVal = -iVal;
			}
			sqlite3_result_int64(context, iVal);
			break;
		}
	case SQLITE_NULL:{
			/* IMP: R-37434-19929 Abs(X) returns NULL if X is NULL. */
			sqlite3_result_null(context);
			break;
		}
	default:{
			/* Because sqlite3_value_double() returns 0.0 if the argument is not
			 * something that can be converted into a number, we have:
			 * IMP: R-01992-00519 Abs(X) returns 0.0 if X is a string or blob
			 * that cannot be converted to a numeric value.
			 */
			double rVal = sqlite3_value_double(argv[0]);
			if (rVal < 0)
				rVal = -rVal;
			sqlite3_result_double(context, rVal);
			break;
		}
	}
}

/*
 * Implementation of the instr() function.
 *
 * instr(haystack,needle) finds the first occurrence of needle
 * in haystack and returns the number of previous characters plus 1,
 * or 0 if needle does not occur within haystack.
 *
 * If both haystack and needle are BLOBs, then the result is one more than
 * the number of bytes in haystack prior to the first occurrence of needle,
 * or 0 if needle never occurs in haystack.
 */
static void
instrFunc(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	const unsigned char *zHaystack;
	const unsigned char *zNeedle;
	int nHaystack;
	int nNeedle;
	int typeHaystack, typeNeedle;
	int N = 1;
	int isText;

	UNUSED_PARAMETER(argc);
	typeHaystack = sqlite3_value_type(argv[0]);
	typeNeedle = sqlite3_value_type(argv[1]);
	if (typeHaystack == SQLITE_NULL || typeNeedle == SQLITE_NULL)
		return;
	nHaystack = sqlite3_value_bytes(argv[0]);
	nNeedle = sqlite3_value_bytes(argv[1]);
	if (nNeedle > 0) {
		if (typeHaystack == SQLITE_BLOB && typeNeedle == SQLITE_BLOB) {
			zHaystack = sqlite3_value_blob(argv[0]);
			zNeedle = sqlite3_value_blob(argv[1]);
			assert(zNeedle != 0);
			assert(zHaystack != 0 || nHaystack == 0);
			isText = 0;
		} else {
			zHaystack = sqlite3_value_text(argv[0]);
			zNeedle = sqlite3_value_text(argv[1]);
			isText = 1;
			if (zHaystack == 0 || zNeedle == 0)
				return;
		}
		while (nNeedle <= nHaystack
		       && memcmp(zHaystack, zNeedle, nNeedle) != 0) {
			N++;
			do {
				nHaystack--;
				zHaystack++;
			} while (isText && (zHaystack[0] & 0xc0) == 0x80);
		}
		if (nNeedle > nHaystack)
			N = 0;
	}
	sqlite3_result_int(context, N);
}

/*
 * Implementation of the printf() function.
 */
static void
printfFunc(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	PrintfArguments x;
	StrAccum str;
	const char *zFormat;
	int n;
	sqlite3 *db = sqlite3_context_db_handle(context);

	if (argc >= 1
	    && (zFormat = (const char *)sqlite3_value_text(argv[0])) != 0) {
		x.nArg = argc - 1;
		x.nUsed = 0;
		x.apArg = argv + 1;
		sqlite3StrAccumInit(&str, db, 0, 0,
				    db->aLimit[SQLITE_LIMIT_LENGTH]);
		str.printfFlags = SQLITE_PRINTF_SQLFUNC;
		sqlite3XPrintf(&str, zFormat, &x);
		n = str.nChar;
		sqlite3_result_text(context, sqlite3StrAccumFinish(&str), n,
				    SQLITE_DYNAMIC);
	}
}

/*
 * Implementation of the substr() function.
 *
 * substr(x,p1,p2)  returns p2 characters of x[] beginning with p1.
 * p1 is 1-indexed.  So substr(x,1,1) returns the first character
 * of x.  If x is text, then we actually count UTF-8 characters.
 * If x is a blob, then we count bytes.
 *
 * If p1 is negative, then we begin abs(p1) from the end of x[].
 *
 * If p2 is negative, return the p2 characters preceding p1.
 */
static void
substrFunc(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	const unsigned char *z;
	const unsigned char *z2;
	int len;
	int p0type;
	i64 p1, p2;
	int negP2 = 0;

	assert(argc == 3 || argc == 2);
	if (sqlite3_value_type(argv[1]) == SQLITE_NULL
	    || (argc == 3 && sqlite3_value_type(argv[2]) == SQLITE_NULL)
	    ) {
		return;
	}
	p0type = sqlite3_value_type(argv[0]);
	p1 = sqlite3_value_int(argv[1]);
	if (p0type == SQLITE_BLOB) {
		len = sqlite3_value_bytes(argv[0]);
		z = sqlite3_value_blob(argv[0]);
		if (z == 0)
			return;
		assert(len == sqlite3_value_bytes(argv[0]));
	} else {
		z = sqlite3_value_text(argv[0]);
		if (z == 0)
			return;
		len = 0;
		if (p1 < 0) {
			for (z2 = z; *z2; len++) {
				SQLITE_SKIP_UTF8(z2);
			}
		}
	}
#ifdef SQLITE_SUBSTR_COMPATIBILITY
	/* If SUBSTR_COMPATIBILITY is defined then substr(X,0,N) work the same as
	 * as substr(X,1,N) - it returns the first N characters of X.  This
	 * is essentially a back-out of the bug-fix in check-in [5fc125d362df4b8]
	 * from 2009-02-02 for compatibility of applications that exploited the
	 * old buggy behavior.
	 */
	if (p1 == 0)
		p1 = 1;		/* <rdar://problem/6778339> */
#endif
	if (argc == 3) {
		p2 = sqlite3_value_int(argv[2]);
		if (p2 < 0) {
			p2 = -p2;
			negP2 = 1;
		}
	} else {
		p2 = sqlite3_context_db_handle(context)->
		    aLimit[SQLITE_LIMIT_LENGTH];
	}
	if (p1 < 0) {
		p1 += len;
		if (p1 < 0) {
			p2 += p1;
			if (p2 < 0)
				p2 = 0;
			p1 = 0;
		}
	} else if (p1 > 0) {
		p1--;
	} else if (p2 > 0) {
		p2--;
	}
	if (negP2) {
		p1 -= p2;
		if (p1 < 0) {
			p2 += p1;
			p1 = 0;
		}
	}
	assert(p1 >= 0 && p2 >= 0);
	if (p0type != SQLITE_BLOB) {
		while (*z && p1) {
			SQLITE_SKIP_UTF8(z);
			p1--;
		}
		for (z2 = z; *z2 && p2; p2--) {
			SQLITE_SKIP_UTF8(z2);
		}
		sqlite3_result_text64(context, (char *)z, z2 - z,
				      SQLITE_TRANSIENT);
	} else {
		if (p1 + p2 > len) {
			p2 = len - p1;
			if (p2 < 0)
				p2 = 0;
		}
		sqlite3_result_blob64(context, (char *)&z[p1], (u64) p2,
				      SQLITE_TRANSIENT);
	}
}

/*
 * Implementation of the round() function
 */
#ifndef SQLITE_OMIT_FLOATING_POINT
static void
roundFunc(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	int n = 0;
	double r;
	char *zBuf;
	assert(argc == 1 || argc == 2);
	if (argc == 2) {
		if (SQLITE_NULL == sqlite3_value_type(argv[1]))
			return;
		n = sqlite3_value_int(argv[1]);
		if (n < 0)
			n = 0;
	}
	if (sqlite3_value_type(argv[0]) == SQLITE_NULL)
		return;
	r = sqlite3_value_double(argv[0]);
	/* If Y==0 and X will fit in a 64-bit int,
	 * handle the rounding directly,
	 * otherwise use printf.
	 */
	if (n == 0 && r >= 0 && r < LARGEST_INT64 - 1) {
		r = (double)((sqlite_int64) (r + 0.5));
	} else if (n == 0 && r < 0 && (-r) < LARGEST_INT64 - 1) {
		r = -(double)((sqlite_int64) ((-r) + 0.5));
	} else {
		zBuf = sqlite3_mprintf("%.*f", n, r);
		if (zBuf == 0) {
			sqlite3_result_error_nomem(context);
			return;
		}
		sqlite3AtoF(zBuf, &r, sqlite3Strlen30(zBuf));
		sqlite3_free(zBuf);
	}
	sqlite3_result_double(context, r);
}
#endif

/*
 * Allocate nByte bytes of space using sqlite3Malloc(). If the
 * allocation fails, call sqlite3_result_error_nomem() to notify
 * the database handle that malloc() has failed and return NULL.
 * If nByte is larger than the maximum string or blob length, then
 * raise an SQLITE_TOOBIG exception and return NULL.
 */
static void *
contextMalloc(sqlite3_context * context, i64 nByte)
{
	char *z;
	sqlite3 *db = sqlite3_context_db_handle(context);
	assert(nByte > 0);
	testcase(nByte == db->aLimit[SQLITE_LIMIT_LENGTH]);
	testcase(nByte == db->aLimit[SQLITE_LIMIT_LENGTH] + 1);
	if (nByte > db->aLimit[SQLITE_LIMIT_LENGTH]) {
		sqlite3_result_error_toobig(context);
		z = 0;
	} else {
		z = sqlite3Malloc(nByte);
		if (!z) {
			sqlite3_result_error_nomem(context);
		}
	}
	return z;
}

/*
 * Implementation of the upper() and lower() SQL functions.
 */

#define ICU_CASE_CONVERT(case_type)                                            \
static void                                                                    \
case_type##ICUFunc(sqlite3_context *context, int argc, sqlite3_value **argv)   \
{                                                                              \
	char *z1;                                                              \
	const char *z2;                                                        \
	int n;                                                                 \
	UNUSED_PARAMETER(argc);                                                \
	z2 = (char *)sqlite3_value_text(argv[0]);                              \
	n = sqlite3_value_bytes(argv[0]);                                      \
	/*                                                                     \
	 * Verify that the call to _bytes()                                    \
	 * does not invalidate the _text() pointer.                            \
	 */                                                                    \
	assert(z2 == (char *)sqlite3_value_text(argv[0]));                     \
	if (!z2)                                                               \
		return;                                                        \
	z1 = contextMalloc(context, ((i64) n) + 1);                            \
	if (!z1) {                                                             \
		sqlite3_result_error_nomem(context);                           \
		return;                                                        \
	}                                                                      \
	UErrorCode status = U_ZERO_ERROR;                                      \
	struct coll *coll = sqlite3GetFuncCollSeq(context);                    \
	const char *locale = NULL;                                             \
	if (coll != NULL && coll->type == COLL_TYPE_ICU) {                     \
		locale = ucol_getLocaleByType(coll->collator,                  \
					      ULOC_VALID_LOCALE, &status);     \
	}                                                                      \
	UCaseMap *case_map = ucasemap_open(locale, 0, &status);                \
	assert(case_map != NULL);                                              \
	int len = ucasemap_utf8To##case_type(case_map, z1, n, z2, n, &status); \
	if (len > n) {                                                         \
		status = U_ZERO_ERROR;                                         \
		sqlite3_free(z1);                                              \
		z1 = contextMalloc(context, ((i64) len) + 1);                  \
		if (!z1) {                                                     \
			sqlite3_result_error_nomem(context);                   \
			return;                                                \
		}                                                              \
		ucasemap_utf8To##case_type(case_map, z1, len, z2, n, &status); \
	}                                                                      \
	ucasemap_close(case_map);                                              \
	sqlite3_result_text(context, z1, len, sqlite3_free);                   \
}                                                                              \

ICU_CASE_CONVERT(Lower);
ICU_CASE_CONVERT(Upper);


/*
 * Some functions like COALESCE() and IFNULL() and UNLIKELY() are implemented
 * as VDBE code so that unused argument values do not have to be computed.
 * However, we still need some kind of function implementation for this
 * routines in the function table.  The noopFunc macro provides this.
 * noopFunc will never be called so it doesn't matter what the implementation
 * is.  We might as well use the "version()" function as a substitute.
 */
#define noopFunc sql_func_version /* Substitute function - never called */

/*
 * Implementation of random().  Return a random integer.
 */
static void
randomFunc(sqlite3_context * context, int NotUsed, sqlite3_value ** NotUsed2)
{
	sqlite_int64 r;
	UNUSED_PARAMETER2(NotUsed, NotUsed2);
	sqlite3_randomness(sizeof(r), &r);
	if (r < 0) {
		/* We need to prevent a random number of 0x8000000000000000
		 * (or -9223372036854775808) since when you do abs() of that
		 * number of you get the same value back again.  To do this
		 * in a way that is testable, mask the sign bit off of negative
		 * values, resulting in a positive value.  Then take the
		 * 2s complement of that positive value.  The end result can
		 * therefore be no less than -9223372036854775807.
		 */
		r = -(r & LARGEST_INT64);
	}
	sqlite3_result_int64(context, r);
}

/*
 * Implementation of randomblob(N).  Return a random blob
 * that is N bytes long.
 */
static void
randomBlob(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	int n;
	unsigned char *p;
	assert(argc == 1);
	UNUSED_PARAMETER(argc);
	n = sqlite3_value_int(argv[0]);
	if (n < 1)
		return;
	p = contextMalloc(context, n);
	if (p) {
		sqlite3_randomness(n, p);
		sqlite3_result_blob(context, (char *)p, n, sqlite3_free);
	}
}

#define Utf8Read(s, e) ucnv_getNextUChar(pUtf8conv, &(s), (e), &status)

#define SQL_END_OF_STRING        0xffff
#define SQL_INVALID_UTF8_SYMBOL  0xfffd

/**
 * Returns codes from sql_utf8_pattern_compare().
 */
enum pattern_match_status {
	MATCH = 0,
	NO_MATCH = 1,
	/** No match in spite of having * or % wildcards. */
	NO_WILDCARD_MATCH = 2,
	/** Pattern contains invalid UTF-8 symbol. */
	INVALID_PATTERN = 3
};

/**
 * Compare two UTF-8 strings for equality where the first string
 * is a LIKE expression.
 *
 * Like matching rules:
 *
 *      '%'       Matches any sequence of zero or more
 *                characters.
 *
 *      '_'       Matches any one character.
 *
 *      Ec        Where E is the "esc" character and c is any
 *                other character, including '%', '_', and esc,
 *                match exactly c.
 *
 * This routine is usually quick, but can be N**2 in the worst
 * case.
 *
 * @param pattern String containing comparison pattern.
 * @param string String being compared.
 * @param is_like_ci true if LIKE is case insensitive.
 * @param match_other The escape char for LIKE.
 *
 * @retval One of pattern_match_status values.
 */
static int
sql_utf8_pattern_compare(const char *pattern,
			 const char *string,
			 const int is_like_ci,
			 UChar32 match_other)
{
	/* Next pattern and input string chars. */
	UChar32 c, c2;
	/* One past the last escaped input char. */
	const char *zEscaped = 0;
	const char *pattern_end = pattern + strlen(pattern);
	const char *string_end = string + strlen(string);
	UErrorCode status = U_ZERO_ERROR;

	while (pattern < pattern_end) {
		c = Utf8Read(pattern, pattern_end);
		if (c == SQL_INVALID_UTF8_SYMBOL)
			return INVALID_PATTERN;
		if (c == MATCH_ALL_WILDCARD) {
			/*
			 * Skip over multiple "%" characters in
			 * the pattern. If there are also "_"
			 * characters, skip those as well, but
			 * consume a single character of the
			 * input string for each "_" skipped.
			 */
			while ((c = Utf8Read(pattern, pattern_end)) !=
			       SQL_END_OF_STRING) {
				if (c == SQL_INVALID_UTF8_SYMBOL)
					return INVALID_PATTERN;
				if (c != MATCH_ALL_WILDCARD &&
				    c != MATCH_ONE_WILDCARD)
					break;
				if (c == MATCH_ONE_WILDCARD &&
				    (c2 = Utf8Read(string, string_end)) ==
				    SQL_END_OF_STRING)
					return NO_WILDCARD_MATCH;
				if (c2 == SQL_INVALID_UTF8_SYMBOL)
					return NO_MATCH;
			}
			/*
			 * "%" at the end of the pattern matches.
			 */
			if (c == SQL_END_OF_STRING) {
				return MATCH;
			}
			if (c == match_other) {
				c = Utf8Read(pattern, pattern_end);
				if (c == SQL_INVALID_UTF8_SYMBOL)
					return INVALID_PATTERN;
				if (c == SQL_END_OF_STRING)
					return NO_WILDCARD_MATCH;
			}

			/*
			 * At this point variable c contains the
			 * first character of the pattern string
			 * past the "%". Search in the input
			 * string for the first matching
			 * character and recursively continue the
			 * match from that point.
			 *
			 * For a case-insensitive search, set
			 * variable cx to be the same as c but in
			 * the other case and search the input
			 * string for either c or cx.
			 */

			int bMatch;
			if (is_like_ci)
				c = u_tolower(c);
			while (string < string_end){
				/*
				 * This loop could have been
				 * implemented without if
				 * converting c2 to lower case
				 * by holding c_upper and
				 * c_lower,however it is
				 * implemented this way because
				 * lower works better with German
				 * and Turkish languages.
				 */
				c2 = Utf8Read(string, string_end);
				if (c2 == SQL_INVALID_UTF8_SYMBOL)
					return NO_MATCH;
				if (!is_like_ci) {
					if (c2 != c)
						continue;
				} else {
					if (c2 != c && u_tolower(c2) != c)
						continue;
				}
				bMatch = sql_utf8_pattern_compare(pattern,
								  string,
								  is_like_ci,
								  match_other);
				if (bMatch != NO_MATCH)
					return bMatch;
			}
			return NO_WILDCARD_MATCH;
		}
		if (c == match_other) {
			c = Utf8Read(pattern, pattern_end);
			if (c == SQL_INVALID_UTF8_SYMBOL)
				return INVALID_PATTERN;
			if (c == SQL_END_OF_STRING)
				return NO_MATCH;
			zEscaped = pattern;
		}
		c2 = Utf8Read(string, string_end);
		if (c2 == SQL_INVALID_UTF8_SYMBOL)
			return NO_MATCH;
		if (c == c2)
			continue;
		if (is_like_ci) {
			/*
			 * Small optimization. Reduce number of
			 * calls to u_tolower function. SQL
			 * standards suggest use to_upper for
			 * symbol normalisation. However, using
			 * to_lower allows to respect Turkish 'İ'
			 * in default locale.
			 */
			if (u_tolower(c) == c2 || c == u_tolower(c2))
				continue;
		}
		if (c == MATCH_ONE_WILDCARD && pattern != zEscaped &&
		    c2 != SQL_END_OF_STRING)
			continue;
		return NO_MATCH;
	}
	return string == string_end ? MATCH : NO_MATCH;
}

/**
 * Compare two UTF-8 strings for equality using case sensitive
 * sql_utf8_pattern_compare.
 */
int
sql_strlike_cs(const char *zPattern, const char *zStr, unsigned int esc)
{
	return sql_utf8_pattern_compare(zPattern, zStr, 0, esc);
}

/**
 * Compare two UTF-8 strings for equality using case insensitive
 * sql_utf8_pattern_compare.
 */
int
sql_strlike_ci(const char *zPattern, const char *zStr, unsigned int esc)
{
	return sql_utf8_pattern_compare(zPattern, zStr, 1, esc);
}

/**
 * Count the number of times that the LIKE operator gets called.
 * This is used for testing only.
 */
#ifdef SQLITE_TEST
int sqlite3_like_count = 0;
#endif

/**
 * Implementation of the like() SQL function. This function
 * implements the built-in LIKE operator. The first argument to
 * the function is the pattern and the second argument is the
 * string. So, the SQL statements of the following type:
 *
 *       A LIKE B
 *
 * are implemented as like(B,A).
 */
static void
likeFunc(sqlite3_context *context, int argc, sqlite3_value **argv)
{
	const char *zA, *zB;
	u32 escape = SQL_END_OF_STRING;
	int nPat;
	sqlite3 *db = sqlite3_context_db_handle(context);
	int is_like_ci = SQLITE_PTR_TO_INT(sqlite3_user_data(context));

#ifdef SQLITE_LIKE_DOESNT_MATCH_BLOBS
	if (sqlite3_value_type(argv[0]) == SQLITE_BLOB
	    || sqlite3_value_type(argv[1]) == SQLITE_BLOB) {
#ifdef SQLITE_TEST
		sqlite3_like_count++;
#endif
		sqlite3_result_int(context, 0);
		return;
	}
#endif
	zB = (const char *) sqlite3_value_text(argv[0]);
	zA = (const char *) sqlite3_value_text(argv[1]);

	/*
	 * Limit the length of the LIKE pattern to avoid problems
	 * of deep recursion and N*N behavior in
	 * sql_utf8_pattern_compare().
	 */
	nPat = sqlite3_value_bytes(argv[0]);
	testcase(nPat == db->aLimit[SQLITE_LIMIT_LIKE_PATTERN_LENGTH]);
	testcase(nPat == db->aLimit[SQLITE_LIMIT_LIKE_PATTERN_LENGTH] + 1);
	if (nPat > db->aLimit[SQLITE_LIMIT_LIKE_PATTERN_LENGTH]) {
		sqlite3_result_error(context,
				     "LIKE pattern is too complex", -1);
		return;
	}
	/* Encoding did not change */
	assert(zB == (const char *) sqlite3_value_text(argv[0]));

	if (argc == 3) {
		/*
		 * The escape character string must consist of a
		 * single UTF-8 character. Otherwise, return an
		 * error.
		 */
		const unsigned char *zEsc = sqlite3_value_text(argv[2]);
		if (zEsc == 0)
			return;
		const char *const err_msg =
			"ESCAPE expression must be a single character";
		if (sqlite3Utf8CharLen((char *)zEsc, -1) != 1) {
			sqlite3_result_error(context, err_msg, -1);
			return;
		}
		escape = sqlite3Utf8Read(&zEsc);
	}
	if (!zA || !zB)
		return;
#ifdef SQLITE_TEST
	sqlite3_like_count++;
#endif
	int res;
	res = sql_utf8_pattern_compare(zB, zA, is_like_ci, escape);
	if (res == INVALID_PATTERN) {
		const char *const err_msg =
			"LIKE pattern can only contain UTF-8 characters";
		sqlite3_result_error(context, err_msg, -1);
		return;
	}
	sqlite3_result_int(context, res == MATCH);
}

/*
 * Implementation of the NULLIF(x,y) function.  The result is the first
 * argument if the arguments are different.  The result is NULL if the
 * arguments are equal to each other.
 */
static void
nullifFunc(sqlite3_context * context, int NotUsed, sqlite3_value ** argv)
{
	struct coll *pColl = sqlite3GetFuncCollSeq(context);
	UNUSED_PARAMETER(NotUsed);
	if (sqlite3MemCompare(argv[0], argv[1], pColl) != 0) {
		sqlite3_result_value(context, argv[0]);
	}
}

/**
 * Implementation of the version() function.  The result is the
 * version of the Tarantool that is running.
 *
 * @param context Context being used.
 * @param unused1 Unused.
 * @param unused2 Unused.
 */
static void
sql_func_version(struct sqlite3_context *context,
		 MAYBE_UNUSED int unused1,
		 MAYBE_UNUSED sqlite3_value **unused2)
{
	sqlite3_result_text(context, tarantool_version(), -1, SQLITE_STATIC);
}

/* Array for converting from half-bytes (nybbles) into ASCII hex
 * digits.
 */
static const char hexdigits[] = {
	'0', '1', '2', '3', '4', '5', '6', '7',
	'8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
};

/*
 * Implementation of the QUOTE() function.  This function takes a single
 * argument.  If the argument is numeric, the return value is the same as
 * the argument.  If the argument is NULL, the return value is the string
 * "NULL".  Otherwise, the argument is enclosed in single quotes with
 * single-quote escapes.
 */
static void
quoteFunc(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	assert(argc == 1);
	UNUSED_PARAMETER(argc);
	switch (sqlite3_value_type(argv[0])) {
	case SQLITE_FLOAT:{
			double r1, r2;
			char zBuf[50];
			r1 = sqlite3_value_double(argv[0]);
			sqlite3_snprintf(sizeof(zBuf), zBuf, "%!.15g", r1);
			sqlite3AtoF(zBuf, &r2, 20);
			if (r1 != r2) {
				sqlite3_snprintf(sizeof(zBuf), zBuf, "%!.20e",
						 r1);
			}
			sqlite3_result_text(context, zBuf, -1,
					    SQLITE_TRANSIENT);
			break;
		}
	case SQLITE_INTEGER:{
			sqlite3_result_value(context, argv[0]);
			break;
		}
	case SQLITE_BLOB:{
			char *zText = 0;
			char const *zBlob = sqlite3_value_blob(argv[0]);
			int nBlob = sqlite3_value_bytes(argv[0]);
			assert(zBlob == sqlite3_value_blob(argv[0]));	/* No encoding change */
			zText =
			    (char *)contextMalloc(context,
						  (2 * (i64) nBlob) + 4);
			if (zText) {
				int i;
				for (i = 0; i < nBlob; i++) {
					zText[(i * 2) + 2] =
					    hexdigits[(zBlob[i] >> 4) & 0x0F];
					zText[(i * 2) + 3] =
					    hexdigits[(zBlob[i]) & 0x0F];
				}
				zText[(nBlob * 2) + 2] = '\'';
				zText[(nBlob * 2) + 3] = '\0';
				zText[0] = 'X';
				zText[1] = '\'';
				sqlite3_result_text(context, zText, -1,
						    SQLITE_TRANSIENT);
				sqlite3_free(zText);
			}
			break;
		}
	case SQLITE_TEXT:{
			int i, j;
			u64 n;
			const unsigned char *zArg = sqlite3_value_text(argv[0]);
			char *z;

			if (zArg == 0)
				return;
			for (i = 0, n = 0; zArg[i]; i++) {
				if (zArg[i] == '\'')
					n++;
			}
			z = contextMalloc(context, ((i64) i) + ((i64) n) + 3);
			if (z) {
				z[0] = '\'';
				for (i = 0, j = 1; zArg[i]; i++) {
					z[j++] = zArg[i];
					if (zArg[i] == '\'') {
						z[j++] = '\'';
					}
				}
				z[j++] = '\'';
				z[j] = 0;
				sqlite3_result_text(context, z, j,
						    sqlite3_free);
			}
			break;
		}
	default:{
			assert(sqlite3_value_type(argv[0]) == SQLITE_NULL);
			sqlite3_result_text(context, "NULL", 4, SQLITE_STATIC);
			break;
		}
	}
}

/*
 * The unicode() function.  Return the integer unicode code-point value
 * for the first character of the input string.
 */
static void
unicodeFunc(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	const unsigned char *z = sqlite3_value_text(argv[0]);
	(void)argc;
	if (z && z[0])
		sqlite3_result_int(context, sqlite3Utf8Read(&z));
}

/*
 * The char() function takes zero or more arguments, each of which is
 * an integer.  It constructs a string where each character of the string
 * is the unicode character for the corresponding integer argument.
 */
static void
charFunc(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	unsigned char *z, *zOut;
	int i;
	zOut = z = sqlite3_malloc64(argc * 4 + 1);
	if (z == 0) {
		sqlite3_result_error_nomem(context);
		return;
	}
	for (i = 0; i < argc; i++) {
		sqlite3_int64 x;
		unsigned c;
		x = sqlite3_value_int64(argv[i]);
		if (x < 0 || x > 0x10ffff)
			x = 0xfffd;
		c = (unsigned)(x & 0x1fffff);
		if (c < 0x00080) {
			*zOut++ = (u8) (c & 0xFF);
		} else if (c < 0x00800) {
			*zOut++ = 0xC0 + (u8) ((c >> 6) & 0x1F);
			*zOut++ = 0x80 + (u8) (c & 0x3F);
		} else if (c < 0x10000) {
			*zOut++ = 0xE0 + (u8) ((c >> 12) & 0x0F);
			*zOut++ = 0x80 + (u8) ((c >> 6) & 0x3F);
			*zOut++ = 0x80 + (u8) (c & 0x3F);
		} else {
			*zOut++ = 0xF0 + (u8) ((c >> 18) & 0x07);
			*zOut++ = 0x80 + (u8) ((c >> 12) & 0x3F);
			*zOut++ = 0x80 + (u8) ((c >> 6) & 0x3F);
			*zOut++ = 0x80 + (u8) (c & 0x3F);
		}
	}
	sqlite3_result_text64(context, (char *)z, zOut - z, sqlite3_free);
}

/*
 * The hex() function.  Interpret the argument as a blob.  Return
 * a hexadecimal rendering as text.
 */
static void
hexFunc(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	int i, n;
	const unsigned char *pBlob;
	char *zHex, *z;
	assert(argc == 1);
	UNUSED_PARAMETER(argc);
	pBlob = sqlite3_value_blob(argv[0]);
	n = sqlite3_value_bytes(argv[0]);
	assert(pBlob == sqlite3_value_blob(argv[0]));	/* No encoding change */
	z = zHex = contextMalloc(context, ((i64) n) * 2 + 1);
	if (zHex) {
		for (i = 0; i < n; i++, pBlob++) {
			unsigned char c = *pBlob;
			*(z++) = hexdigits[(c >> 4) & 0xf];
			*(z++) = hexdigits[c & 0xf];
		}
		*z = 0;
		sqlite3_result_text(context, zHex, n * 2, sqlite3_free);
	}
}

/*
 * The zeroblob(N) function returns a zero-filled blob of size N bytes.
 */
static void
zeroblobFunc(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	i64 n;
	int rc;
	assert(argc == 1);
	UNUSED_PARAMETER(argc);
	n = sqlite3_value_int64(argv[0]);
	if (n < 0)
		n = 0;
	rc = sqlite3_result_zeroblob64(context, n);	/* IMP: R-00293-64994 */
	if (rc) {
		sqlite3_result_error_code(context, rc);
	}
}

/*
 * The replace() function.  Three arguments are all strings: call
 * them A, B, and C. The result is also a string which is derived
 * from A by replacing every occurrence of B with C.  The match
 * must be exact.  Collating sequences are not used.
 */
static void
replaceFunc(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	const unsigned char *zStr;	/* The input string A */
	const unsigned char *zPattern;	/* The pattern string B */
	const unsigned char *zRep;	/* The replacement string C */
	unsigned char *zOut;	/* The output */
	int nStr;		/* Size of zStr */
	int nPattern;		/* Size of zPattern */
	int nRep;		/* Size of zRep */
	i64 nOut;		/* Maximum size of zOut */
	int loopLimit;		/* Last zStr[] that might match zPattern[] */
	int i, j;		/* Loop counters */

	assert(argc == 3);
	UNUSED_PARAMETER(argc);
	zStr = sqlite3_value_text(argv[0]);
	if (zStr == 0)
		return;
	nStr = sqlite3_value_bytes(argv[0]);
	assert(zStr == sqlite3_value_text(argv[0]));	/* No encoding change */
	zPattern = sqlite3_value_text(argv[1]);
	if (zPattern == 0) {
		assert(sqlite3_value_type(argv[1]) == SQLITE_NULL
		       || sqlite3_context_db_handle(context)->mallocFailed);
		return;
	}
	if (zPattern[0] == 0) {
		assert(sqlite3_value_type(argv[1]) != SQLITE_NULL);
		sqlite3_result_value(context, argv[0]);
		return;
	}
	nPattern = sqlite3_value_bytes(argv[1]);
	assert(zPattern == sqlite3_value_text(argv[1]));	/* No encoding change */
	zRep = sqlite3_value_text(argv[2]);
	if (zRep == 0)
		return;
	nRep = sqlite3_value_bytes(argv[2]);
	assert(zRep == sqlite3_value_text(argv[2]));
	nOut = nStr + 1;
	assert(nOut < SQLITE_MAX_LENGTH);
	zOut = contextMalloc(context, (i64) nOut);
	if (zOut == 0) {
		return;
	}
	loopLimit = nStr - nPattern;
	for (i = j = 0; i <= loopLimit; i++) {
		if (zStr[i] != zPattern[0]
		    || memcmp(&zStr[i], zPattern, nPattern)) {
			zOut[j++] = zStr[i];
		} else {
			u8 *zOld;
			sqlite3 *db = sqlite3_context_db_handle(context);
			nOut += nRep - nPattern;
			testcase(nOut - 1 == db->aLimit[SQLITE_LIMIT_LENGTH]);
			testcase(nOut - 2 == db->aLimit[SQLITE_LIMIT_LENGTH]);
			if (nOut - 1 > db->aLimit[SQLITE_LIMIT_LENGTH]) {
				sqlite3_result_error_toobig(context);
				sqlite3_free(zOut);
				return;
			}
			zOld = zOut;
			zOut = sqlite3_realloc64(zOut, (int)nOut);
			if (zOut == 0) {
				sqlite3_result_error_nomem(context);
				sqlite3_free(zOld);
				return;
			}
			memcpy(&zOut[j], zRep, nRep);
			j += nRep;
			i += nPattern - 1;
		}
	}
	assert(j + nStr - i + 1 == nOut);
	memcpy(&zOut[j], &zStr[i], nStr - i);
	j += nStr - i;
	assert(j <= nOut);
	zOut[j] = 0;
	sqlite3_result_text(context, (char *)zOut, j, sqlite3_free);
}

/*
 * Implementation of the TRIM(), LTRIM(), and RTRIM() functions.
 * The userdata is 0x1 for left trim, 0x2 for right trim, 0x3 for both.
 */
static void
trimFunc(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	const unsigned char *zIn;	/* Input string */
	const unsigned char *zCharSet;	/* Set of characters to trim */
	int nIn;		/* Number of bytes in input */
	int flags;		/* 1: trimleft  2: trimright  3: trim */
	int i;			/* Loop counter */
	unsigned char *aLen = 0;	/* Length of each character in zCharSet */
	unsigned char **azChar = 0;	/* Individual characters in zCharSet */
	int nChar;		/* Number of characters in zCharSet */

	if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
		return;
	}
	zIn = sqlite3_value_text(argv[0]);
	if (zIn == 0)
		return;
	nIn = sqlite3_value_bytes(argv[0]);
	assert(zIn == sqlite3_value_text(argv[0]));
	if (argc == 1) {
		static const unsigned char lenOne[] = { 1 };
		static unsigned char *const azOne[] = { (u8 *) " " };
		nChar = 1;
		aLen = (u8 *) lenOne;
		azChar = (unsigned char **)azOne;
		zCharSet = 0;
	} else if ((zCharSet = sqlite3_value_text(argv[1])) == 0) {
		return;
	} else {
		const unsigned char *z;
		for (z = zCharSet, nChar = 0; *z; nChar++) {
			SQLITE_SKIP_UTF8(z);
		}
		if (nChar > 0) {
			azChar =
			    contextMalloc(context,
					  ((i64) nChar) * (sizeof(char *) + 1));
			if (azChar == 0) {
				return;
			}
			aLen = (unsigned char *)&azChar[nChar];
			for (z = zCharSet, nChar = 0; *z; nChar++) {
				azChar[nChar] = (unsigned char *)z;
				SQLITE_SKIP_UTF8(z);
				aLen[nChar] = (u8) (z - azChar[nChar]);
			}
		}
	}
	if (nChar > 0) {
		flags = SQLITE_PTR_TO_INT(sqlite3_user_data(context));
		if (flags & 1) {
			while (nIn > 0) {
				int len = 0;
				for (i = 0; i < nChar; i++) {
					len = aLen[i];
					if (len <= nIn
					    && memcmp(zIn, azChar[i], len) == 0)
						break;
				}
				if (i >= nChar)
					break;
				zIn += len;
				nIn -= len;
			}
		}
		if (flags & 2) {
			while (nIn > 0) {
				int len = 0;
				for (i = 0; i < nChar; i++) {
					len = aLen[i];
					if (len <= nIn
					    && memcmp(&zIn[nIn - len],
						      azChar[i], len) == 0)
						break;
				}
				if (i >= nChar)
					break;
				nIn -= len;
			}
		}
		if (zCharSet) {
			sqlite3_free(azChar);
		}
	}
	sqlite3_result_text(context, (char *)zIn, nIn, SQLITE_TRANSIENT);
}

#ifdef SQLITE_ENABLE_UNKNOWN_SQL_FUNCTION
/*
 * The "unknown" function is automatically substituted in place of
 * any unrecognized function name when doing an EXPLAIN or EXPLAIN QUERY PLAN
 * when the SQLITE_ENABLE_UNKNOWN_FUNCTION compile-time option is used.
 * When the "sqlite3" command-line shell is built using this functionality,
 * that allows an EXPLAIN or EXPLAIN QUERY PLAN for complex queries
 * involving application-defined functions to be examined in a generic
 * sqlite3 shell.
 */
static void
unknownFunc(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	/* no-op */
}
#endif				/*SQLITE_ENABLE_UNKNOWN_SQL_FUNCTION */

/* IMP: R-25361-16150 This function is omitted from SQLite by default. It
 * is only available if the SQLITE_SOUNDEX compile-time option is used
 * when SQLite is built.
 */
#ifdef SQLITE_SOUNDEX
/*
 * Compute the soundex encoding of a word.
 *
 * IMP: R-59782-00072 The soundex(X) function returns a string that is the
 * soundex encoding of the string X.
 */
static void
soundexFunc(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	char zResult[8];
	const u8 *zIn;
	int i, j;
	static const unsigned char iCode[] = {
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 1, 2, 3, 0, 1, 2, 0, 0, 2, 2, 4, 5, 5, 0,
		1, 2, 6, 2, 3, 0, 1, 0, 2, 0, 2, 0, 0, 0, 0, 0,
		0, 0, 1, 2, 3, 0, 1, 2, 0, 0, 2, 2, 4, 5, 5, 0,
		1, 2, 6, 2, 3, 0, 1, 0, 2, 0, 2, 0, 0, 0, 0, 0,
	};
	assert(argc == 1);
	zIn = (u8 *) sqlite3_value_text(argv[0]);
	if (zIn == 0)
		zIn = (u8 *) "";
	for (i = 0; zIn[i] && !sqlite3Isalpha(zIn[i]); i++) {
	}
	if (zIn[i]) {
		u8 prevcode = iCode[zIn[i] & 0x7f];
		zResult[0] = sqlite3Toupper(zIn[i]);
		for (j = 1; j < 4 && zIn[i]; i++) {
			int code = iCode[zIn[i] & 0x7f];
			if (code > 0) {
				if (code != prevcode) {
					prevcode = code;
					zResult[j++] = code + '0';
				}
			} else {
				prevcode = 0;
			}
		}
		while (j < 4) {
			zResult[j++] = '0';
		}
		zResult[j] = 0;
		sqlite3_result_text(context, zResult, 4, SQLITE_TRANSIENT);
	} else {
		/* IMP: R-64894-50321 The string "?000" is returned if the argument
		 * is NULL or contains no ASCII alphabetic characters.
		 */
		sqlite3_result_text(context, "?000", 4, SQLITE_STATIC);
	}
}
#endif				/* SQLITE_SOUNDEX */

/*
 * An instance of the following structure holds the context of a
 * sum() or avg() aggregate computation.
 */
typedef struct SumCtx SumCtx;
struct SumCtx {
	double rSum;		/* Floating point sum */
	i64 iSum;		/* Integer sum */
	i64 cnt;		/* Number of elements summed */
	u8 overflow;		/* True if integer overflow seen */
	u8 approx;		/* True if non-integer value was input to the sum */
};

/*
 * Routines used to compute the sum, average, and total.
 *
 * The SUM() function follows the (broken) SQL standard which means
 * that it returns NULL if it sums over no inputs.  TOTAL returns
 * 0.0 in that case.  In addition, TOTAL always returns a float where
 * SUM might return an integer if it never encounters a floating point
 * value.  TOTAL never fails, but SUM might through an exception if
 * it overflows an integer.
 */
static void
sumStep(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	SumCtx *p;
	int type;
	assert(argc == 1);
	UNUSED_PARAMETER(argc);
	p = sqlite3_aggregate_context(context, sizeof(*p));
	type = sqlite3_value_numeric_type(argv[0]);
	if (p && type != SQLITE_NULL) {
		p->cnt++;
		if (type == SQLITE_INTEGER) {
			i64 v = sqlite3_value_int64(argv[0]);
			p->rSum += v;
			if ((p->approx | p->overflow) == 0
			    && sqlite3AddInt64(&p->iSum, v)) {
				p->overflow = 1;
			}
		} else {
			p->rSum += sqlite3_value_double(argv[0]);
			p->approx = 1;
		}
	}
}

static void
sumFinalize(sqlite3_context * context)
{
	SumCtx *p;
	p = sqlite3_aggregate_context(context, 0);
	if (p && p->cnt > 0) {
		if (p->overflow) {
			sqlite3_result_error(context, "integer overflow", -1);
		} else if (p->approx) {
			sqlite3_result_double(context, p->rSum);
		} else {
			sqlite3_result_int64(context, p->iSum);
		}
	}
}

static void
avgFinalize(sqlite3_context * context)
{
	SumCtx *p;
	p = sqlite3_aggregate_context(context, 0);
	if (p && p->cnt > 0) {
		sqlite3_result_double(context, p->rSum / (double)p->cnt);
	}
}

static void
totalFinalize(sqlite3_context * context)
{
	SumCtx *p;
	p = sqlite3_aggregate_context(context, 0);
	/* (double)0 In case of SQLITE_OMIT_FLOATING_POINT... */
	sqlite3_result_double(context, p ? p->rSum : (double)0);
}

/*
 * The following structure keeps track of state information for the
 * count() aggregate function.
 */
typedef struct CountCtx CountCtx;
struct CountCtx {
	i64 n;
};

/*
 * Routines to implement the count() aggregate function.
 */
static void
countStep(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	CountCtx *p;
	p = sqlite3_aggregate_context(context, sizeof(*p));
	if ((argc == 0 || SQLITE_NULL != sqlite3_value_type(argv[0])) && p) {
		p->n++;
	}
}

static void
countFinalize(sqlite3_context * context)
{
	CountCtx *p;
	p = sqlite3_aggregate_context(context, 0);
	sqlite3_result_int64(context, p ? p->n : 0);
}

/*
 * Routines to implement min() and max() aggregate functions.
 */
static void
minmaxStep(sqlite3_context * context, int NotUsed, sqlite3_value ** argv)
{
	Mem *pArg = (Mem *) argv[0];
	Mem *pBest;
	UNUSED_PARAMETER(NotUsed);

	pBest = (Mem *) sqlite3_aggregate_context(context, sizeof(*pBest));
	if (!pBest)
		return;

	if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
		if (pBest->flags)
			sqlite3SkipAccumulatorLoad(context);
	} else if (pBest->flags) {
		int max;
		int cmp;
		struct coll *pColl = sqlite3GetFuncCollSeq(context);
		/* This step function is used for both the min() and max() aggregates,
		 * the only difference between the two being that the sense of the
		 * comparison is inverted. For the max() aggregate, the
		 * sqlite3_user_data() function returns (void *)-1. For min() it
		 * returns (void *)db, where db is the sqlite3* database pointer.
		 * Therefore the next statement sets variable 'max' to 1 for the max()
		 * aggregate, or 0 for min().
		 */
		max = sqlite3_user_data(context) != 0;
		cmp = sqlite3MemCompare(pBest, pArg, pColl);
		if ((max && cmp < 0) || (!max && cmp > 0)) {
			sqlite3VdbeMemCopy(pBest, pArg);
		} else {
			sqlite3SkipAccumulatorLoad(context);
		}
	} else {
		pBest->db = sqlite3_context_db_handle(context);
		sqlite3VdbeMemCopy(pBest, pArg);
	}
}

static void
minMaxFinalize(sqlite3_context * context)
{
	sqlite3_value *pRes;
	pRes = (sqlite3_value *) sqlite3_aggregate_context(context, 0);
	if (pRes) {
		if (pRes->flags) {
			sqlite3_result_value(context, pRes);
		}
		sqlite3VdbeMemRelease(pRes);
	}
}

/*
 * group_concat(EXPR, ?SEPARATOR?)
 */
static void
groupConcatStep(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	const char *zVal;
	StrAccum *pAccum;
	const char *zSep;
	int nVal, nSep;
	assert(argc == 1 || argc == 2);
	if (sqlite3_value_type(argv[0]) == SQLITE_NULL)
		return;
	pAccum =
	    (StrAccum *) sqlite3_aggregate_context(context, sizeof(*pAccum));

	if (pAccum) {
		sqlite3 *db = sqlite3_context_db_handle(context);
		int firstTerm = pAccum->mxAlloc == 0;
		pAccum->mxAlloc = db->aLimit[SQLITE_LIMIT_LENGTH];
		if (!firstTerm) {
			if (argc == 2) {
				zSep = (char *)sqlite3_value_text(argv[1]);
				nSep = sqlite3_value_bytes(argv[1]);
			} else {
				zSep = ",";
				nSep = 1;
			}
			if (zSep)
				sqlite3StrAccumAppend(pAccum, zSep, nSep);
		}
		zVal = (char *)sqlite3_value_text(argv[0]);
		nVal = sqlite3_value_bytes(argv[0]);
		if (zVal)
			sqlite3StrAccumAppend(pAccum, zVal, nVal);
	}
}

static void
groupConcatFinalize(sqlite3_context * context)
{
	StrAccum *pAccum;
	pAccum = sqlite3_aggregate_context(context, 0);
	if (pAccum) {
		if (pAccum->accError == STRACCUM_TOOBIG) {
			sqlite3_result_error_toobig(context);
		} else if (pAccum->accError == STRACCUM_NOMEM) {
			sqlite3_result_error_nomem(context);
		} else {
			sqlite3_result_text(context,
					    sqlite3StrAccumFinish(pAccum), -1,
					    sqlite3_free);
		}
	}
}

/*
 * If the function already exists as a regular global function, then
 * this routine is a no-op.  If the function does not exist, then create
 * a new one that always throws a run-time error.
 */
static inline int
sqlite3_overload_function(sqlite3 * db, const char *zName,
			  enum field_type type, int nArg)
{
	int rc = SQLITE_OK;

#ifdef SQLITE_ENABLE_API_ARMOR
	if (!sqlite3SafetyCheckOk(db) || zName == 0 || nArg < -2) {
		return SQLITE_MISUSE_BKPT;
	}
#endif
	if (sqlite3FindFunction(db, zName, nArg, 0) == 0) {
		rc = sqlite3CreateFunc(db, zName, type, nArg, 0, 0,
				       sqlite3InvalidFunction, 0, 0, 0);
	}
	rc = sqlite3ApiExit(db, rc);
	return rc;
}

/*
 * This routine does per-connection function registration.  Most
 * of the built-in functions above are part of the global function set.
 * This routine only deals with those that are not global.
 */
void
sqlite3RegisterPerConnectionBuiltinFunctions(sqlite3 * db)
{
	int rc = sqlite3_overload_function(db, "MATCH", FIELD_TYPE_ANY, 2);
	assert(rc == SQLITE_NOMEM || rc == SQLITE_OK);
	if (rc == SQLITE_NOMEM) {
		sqlite3OomFault(db);
	}
}

/*
 * Set the LIKEOPT flag on the 2-argument function with the given name.
 */
static void
setLikeOptFlag(sqlite3 * db, const char *zName, u8 flagVal)
{
	FuncDef *pDef;
	pDef = sqlite3FindFunction(db, zName, 2, 0);
	if (ALWAYS(pDef)) {
		pDef->funcFlags |= flagVal;
	}
}

/**
 * Register the built-in LIKE function.
 */
void
sqlite3RegisterLikeFunctions(sqlite3 *db, int is_case_insensitive)
{
	/*
	 * FIXME: after introducing type <BOOLEAN> LIKE must
	 * return that type: TRUE if the string matches the
	 * supplied pattern and FALSE otherwise.
	 */
	int *is_like_ci = SQLITE_INT_TO_PTR(is_case_insensitive);
	sqlite3CreateFunc(db, "LIKE", FIELD_TYPE_INTEGER, 2, 0,
			  is_like_ci, likeFunc, 0, 0, 0);
	sqlite3CreateFunc(db, "LIKE", FIELD_TYPE_INTEGER, 3, 0,
			  is_like_ci, likeFunc, 0, 0, 0);
	setLikeOptFlag(db, "LIKE",
		       !(is_case_insensitive) ? (SQLITE_FUNC_LIKE |
		       SQLITE_FUNC_CASE) : SQLITE_FUNC_LIKE);
}

int
sql_is_like_func(struct sqlite3 *db, struct Expr *expr, int *is_like_ci)
{
	if (expr->op != TK_FUNCTION || !expr->x.pList ||
	    expr->x.pList->nExpr != 2)
		return 0;
	assert(!ExprHasProperty(expr, EP_xIsSelect));
	struct FuncDef *func = sqlite3FindFunction(db, expr->u.zToken, 2, 0);
	assert(func != NULL);
	if ((func->funcFlags & SQLITE_FUNC_LIKE) == 0)
		return 0;
	*is_like_ci = (func->funcFlags & SQLITE_FUNC_CASE) == 0;
	return 1;
}

/*
 * All of the FuncDef structures in the aBuiltinFunc[] array above
 * to the global function hash table.  This occurs at start-time (as
 * a consequence of calling sqlite3_initialize()).
 *
 * After this routine runs
 */
void
sqlite3RegisterBuiltinFunctions(void)
{
	/*
	 * Initialize default case map for UPPER/LOWER functions
	 * This structure is not freed at db exit, but that is ok.
	 */
	UErrorCode status = U_ZERO_ERROR;

	pUtf8conv = ucnv_open("utf8", &status);
	assert(pUtf8conv);
	/*
	 * The following array holds FuncDef structures for all of the functions
	 * defined in this file.
	 *
	 * The array cannot be constant since changes are made to the
	 * FuncDef.pHash elements at start-time.  The elements of this array
	 * are read-only after initialization is complete.
	 *
	 * For peak efficiency, put the most frequently used function last.
	 */
	static FuncDef aBuiltinFunc[] = {
#ifdef SQLITE_SOUNDEX
		FUNCTION(soundex, 1, 0, 0, soundexFunc),
#endif
		FUNCTION2(unlikely, 1, 0, 0, noopFunc, SQLITE_FUNC_UNLIKELY,
			  FIELD_TYPE_INTEGER),
		FUNCTION2(likelihood, 2, 0, 0, noopFunc, SQLITE_FUNC_UNLIKELY,
			  FIELD_TYPE_INTEGER),
		FUNCTION2(likely, 1, 0, 0, noopFunc, SQLITE_FUNC_UNLIKELY,
			  FIELD_TYPE_INTEGER),
		FUNCTION(ltrim, 1, 1, 0, trimFunc, FIELD_TYPE_STRING),
		FUNCTION(ltrim, 2, 1, 0, trimFunc, FIELD_TYPE_STRING),
		FUNCTION(rtrim, 1, 2, 0, trimFunc, FIELD_TYPE_STRING),
		FUNCTION(rtrim, 2, 2, 0, trimFunc, FIELD_TYPE_STRING),
		FUNCTION(trim, 1, 3, 0, trimFunc, FIELD_TYPE_STRING),
		FUNCTION(trim, 2, 3, 0, trimFunc, FIELD_TYPE_STRING),
		FUNCTION(min, -1, 0, 1, minmaxFunc, 0),
		FUNCTION(min, 0, 0, 1, 0, 0),
		AGGREGATE2(min, 1, 0, 1, minmaxStep, minMaxFinalize,
			   SQLITE_FUNC_MINMAX, 0),
		FUNCTION(max, -1, 1, 1, minmaxFunc, 0),
		FUNCTION(max, 0, 1, 1, 0, 0),
		AGGREGATE2(max, 1, 1, 1, minmaxStep, minMaxFinalize,
			   SQLITE_FUNC_MINMAX, 0),
		FUNCTION2(typeof, 1, 0, 0, typeofFunc, SQLITE_FUNC_TYPEOF,
			  FIELD_TYPE_STRING),
		FUNCTION2(length, 1, 0, 0, lengthFunc, SQLITE_FUNC_LENGTH,
			  FIELD_TYPE_INTEGER),
		FUNCTION(instr, 2, 0, 0, instrFunc, FIELD_TYPE_INTEGER),
		FUNCTION(printf, -1, 0, 0, printfFunc, FIELD_TYPE_STRING),
		FUNCTION(unicode, 1, 0, 0, unicodeFunc, FIELD_TYPE_STRING),
		FUNCTION(char, -1, 0, 0, charFunc, FIELD_TYPE_STRING),
		FUNCTION(abs, 1, 0, 0, absFunc, FIELD_TYPE_NUMBER),
#ifndef SQLITE_OMIT_FLOATING_POINT
		FUNCTION(round, 1, 0, 0, roundFunc, FIELD_TYPE_INTEGER),
		FUNCTION(round, 2, 0, 0, roundFunc, FIELD_TYPE_INTEGER),
#endif
		FUNCTION(upper, 1, 0, 1, UpperICUFunc, FIELD_TYPE_STRING),
		FUNCTION(lower, 1, 0, 1, LowerICUFunc, FIELD_TYPE_STRING),
		FUNCTION(hex, 1, 0, 0, hexFunc, FIELD_TYPE_STRING),
		FUNCTION2(ifnull, 2, 0, 0, noopFunc, SQLITE_FUNC_COALESCE,
			  FIELD_TYPE_INTEGER),
		VFUNCTION(random, 0, 0, 0, randomFunc, FIELD_TYPE_NUMBER),
		VFUNCTION(randomblob, 1, 0, 0, randomBlob, FIELD_TYPE_SCALAR),
		FUNCTION(nullif, 2, 0, 1, nullifFunc, 0),
		FUNCTION(version, 0, 0, 0, sql_func_version, FIELD_TYPE_STRING),
		FUNCTION(quote, 1, 0, 0, quoteFunc, FIELD_TYPE_STRING),
		VFUNCTION(row_count, 0, 0, 0, sql_row_count, FIELD_TYPE_INTEGER),
		FUNCTION(replace, 3, 0, 0, replaceFunc, FIELD_TYPE_STRING),
		FUNCTION(zeroblob, 1, 0, 0, zeroblobFunc, FIELD_TYPE_SCALAR),
		FUNCTION(substr, 2, 0, 0, substrFunc, FIELD_TYPE_STRING),
		FUNCTION(substr, 3, 0, 0, substrFunc, FIELD_TYPE_STRING),
		AGGREGATE(sum, 1, 0, 0, sumStep, sumFinalize, 0),
		AGGREGATE(total, 1, 0, 0, sumStep, totalFinalize, 0),
		AGGREGATE(avg, 1, 0, 0, sumStep, avgFinalize, 0),
		AGGREGATE2(count, 0, 0, 0, countStep, countFinalize,
			   SQLITE_FUNC_COUNT, FIELD_TYPE_INTEGER),
		AGGREGATE(count, 1, 0, 0, countStep, countFinalize,
			  FIELD_TYPE_INTEGER),
		AGGREGATE(group_concat, 1, 0, 0, groupConcatStep,
			  groupConcatFinalize, FIELD_TYPE_STRING),
		AGGREGATE(group_concat, 2, 0, 0, groupConcatStep,
			  groupConcatFinalize, FIELD_TYPE_STRING),

		LIKEFUNC(like, 2, 1, SQLITE_FUNC_LIKE,
			 FIELD_TYPE_INTEGER),
		LIKEFUNC(like, 3, 1, SQLITE_FUNC_LIKE,
			 FIELD_TYPE_INTEGER),
#ifdef SQLITE_ENABLE_UNKNOWN_SQL_FUNCTION
		FUNCTION(unknown, -1, 0, 0, unknownFunc, 0),
#endif
		FUNCTION(coalesce, 1, 0, 0, 0, 0),
		FUNCTION(coalesce, 0, 0, 0, 0, 0),
		FUNCTION2(coalesce, -1, 0, 0, noopFunc, SQLITE_FUNC_COALESCE, 0),
	};
	sqlite3AnalyzeFunctions();
	sqlite3RegisterDateTimeFunctions();
	sqlite3InsertBuiltinFuncs(aBuiltinFunc, ArraySize(aBuiltinFunc));

#if 0				/* Enable to print out how the built-in functions are hashed */
	{
		int i;
		FuncDef *p;
		for (i = 0; i < SQLITE_FUNC_HASH_SZ; i++) {
			printf("FUNC-HASH %02d:", i);
			for (p = sqlite3BuiltinFunctions.a[i]; p;
			     p = p->u.pHash) {
				int n = sqlite3Strlen30(p->zName);
				int h = p->zName[0] + n;
				printf(" %s(%d)", p->zName, h);
			}
			printf("\n");
		}
	}
#endif
}
