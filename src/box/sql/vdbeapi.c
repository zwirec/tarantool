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
 *
 * This file contains code use to implement APIs that are part of the
 * VDBE.
 */
#include "sqliteInt.h"
#include "vdbeInt.h"

/*
 * Check on a Vdbe to make sure it has not been finalized.  Log
 * an error and return true if it has been finalized (or is otherwise
 * invalid).  Return false if it is ok.
 */
static int
vdbeSafety(Vdbe * p)
{
	if (p->db == 0) {
		sqlite3_log(SQLITE_MISUSE,
			    "API called with finalized prepared statement");
		return 1;
	} else {
		return 0;
	}
}

static int
vdbeSafetyNotNull(Vdbe * p)
{
	if (p == 0) {
		sqlite3_log(SQLITE_MISUSE,
			    "API called with NULL prepared statement");
		return 1;
	} else {
		return vdbeSafety(p);
	}
}

#ifndef SQLITE_OMIT_TRACE
/*
 * Invoke the profile callback.  This routine is only called if we already
 * know that the profile callback is defined and needs to be invoked.
 */
static SQLITE_NOINLINE void
invokeProfileCallback(sqlite3 * db, Vdbe * p)
{
	sqlite3_int64 iNow;
	sqlite3_int64 iElapse;
	assert(p->startTime > 0);
	assert(db->xProfile != 0 || (db->mTrace & SQLITE_TRACE_PROFILE) != 0);
	assert(db->init.busy == 0);
	assert(p->zSql != 0);
	sqlite3OsCurrentTimeInt64(db->pVfs, &iNow);
	iElapse = (iNow - p->startTime) * 1000000;
	if (db->xProfile) {
		db->xProfile(db->pProfileArg, p->zSql, iElapse);
	}
	if (db->mTrace & SQLITE_TRACE_PROFILE) {
		db->xTrace(SQLITE_TRACE_PROFILE, db->pTraceArg, p,
			   (void *)&iElapse);
	}
	p->startTime = 0;
}

/*
 * The checkProfileCallback(DB,P) macro checks to see if a profile callback
 * is needed, and it invokes the callback if it is needed.
 */
#define checkProfileCallback(DB,P) \
   if( ((P)->startTime)>0 ){ invokeProfileCallback(DB,P); }
#else
#define checkProfileCallback(DB,P)	/*no-op */
#endif

/*
 * The following routine destroys a virtual machine that is created by
 * the sqlite3_compile() routine. The integer returned is an SQLITE_
 * success/failure code that describes the result of executing the virtual
 * machine.
 *
 * This routine sets the error code and string returned by
 * sqlite3_errcode(), sqlite3_errmsg() and sqlite3_errmsg16().
 */
int
sqlite3_finalize(sqlite3_stmt * pStmt)
{
	int rc;
	if (pStmt == 0) {
		/* IMPLEMENTATION-OF: R-57228-12904 Invoking sqlite3_finalize() on a NULL
		 * pointer is a harmless no-op.
		 */
		rc = SQLITE_OK;
	} else {
		Vdbe *v = (Vdbe *) pStmt;
		sqlite3 *db = v->db;
		if (vdbeSafety(v))
			return SQLITE_MISUSE_BKPT;
		checkProfileCallback(db, v);
		rc = sqlite3VdbeFinalize(v);
		rc = sqlite3ApiExit(db, rc);
	}
	return rc;
}

/*
 * Terminate the current execution of an SQL statement and reset it
 * back to its starting state so that it can be reused. A success code from
 * the prior execution is returned.
 *
 * This routine sets the error code and string returned by
 * sqlite3_errcode(), sqlite3_errmsg() and sqlite3_errmsg16().
 */
int
sqlite3_reset(sqlite3_stmt * pStmt)
{
	int rc;
	if (pStmt == 0) {
		rc = SQLITE_OK;
	} else {
		Vdbe *v = (Vdbe *) pStmt;
		sqlite3 *db = v->db;
		checkProfileCallback(db, v);
		rc = sqlite3VdbeReset(v);
		sqlite3VdbeRewind(v);
		assert((rc & (db->errMask)) == rc);
		rc = sqlite3ApiExit(db, rc);
	}
	return rc;
}

/*
 * Set all the parameters in the compiled SQL statement to NULL.
 */
int
sqlite3_clear_bindings(sqlite3_stmt * pStmt)
{
	int i;
	int rc = SQLITE_OK;
	Vdbe *p = (Vdbe *) pStmt;
	for (i = 0; i < p->nVar; i++) {
		sqlite3VdbeMemRelease(&p->aVar[i]);
		p->aVar[i].flags = MEM_Null;
	}
	if (p->isPrepareV2 && p->expmask) {
		p->expired = 1;
	}
	return rc;
}

/**************************** sqlite3_value_  ******************************
 * The following routines extract information from a Mem or sqlite3_value
 * structure.
 */
const void *
sqlite3_value_blob(sqlite3_value * pVal)
{
	Mem *p = (Mem *) pVal;
	if (p->flags & (MEM_Blob | MEM_Str)) {
		if (ExpandBlob(p) != SQLITE_OK) {
			assert(p->flags == MEM_Null && p->z == 0);
			return 0;
		}
		p->flags |= MEM_Blob;
		return p->n ? p->z : 0;
	} else {
		return sqlite3_value_text(pVal);
	}
}

int
sqlite3_value_bytes(sqlite3_value * pVal)
{
	return sqlite3ValueBytes(pVal);
}

double
sqlite3_value_double(sqlite3_value * pVal)
{
	double v;
	sqlite3VdbeRealValue((Mem *) pVal, &v);
	return v;
}

int
sqlite3_value_int(sqlite3_value * pVal)
{
	int64_t i;
	sqlite3VdbeIntValue((Mem *) pVal, &i);
	return (int)i;
}

sqlite_int64
sqlite3_value_int64(sqlite3_value * pVal)
{
	int64_t i;
	sqlite3VdbeIntValue((Mem *) pVal, &i);
	return i;
}

enum sql_subtype
sqlite3_value_subtype(sqlite3_value * pVal)
{
	return (pVal->flags & MEM_Subtype) != 0 ? pVal->subtype : SQL_SUBTYPE_NO;
}

const unsigned char *
sqlite3_value_text(sqlite3_value * pVal)
{
	return (const unsigned char *)sqlite3ValueText(pVal);
}

/* EVIDENCE-OF: R-12793-43283 Every value in SQLite has one of five
 * fundamental datatypes: 64-bit signed integer 64-bit IEEE floating
 * point number string BLOB NULL
 */
int
sqlite3_value_type(sqlite3_value * pVal)
{
	static const u8 aType[] = {
		SQLITE_BLOB,	/* 0x00 */
		SQLITE_NULL,	/* 0x01 */
		SQLITE_TEXT,	/* 0x02 */
		SQLITE_NULL,	/* 0x03 */
		SQLITE_INTEGER,	/* 0x04 */
		SQLITE_NULL,	/* 0x05 */
		SQLITE_INTEGER,	/* 0x06 */
		SQLITE_NULL,	/* 0x07 */
		SQLITE_FLOAT,	/* 0x08 */
		SQLITE_NULL,	/* 0x09 */
		SQLITE_FLOAT,	/* 0x0a */
		SQLITE_NULL,	/* 0x0b */
		SQLITE_INTEGER,	/* 0x0c */
		SQLITE_NULL,	/* 0x0d */
		SQLITE_INTEGER,	/* 0x0e */
		SQLITE_NULL,	/* 0x0f */
		SQLITE_BLOB,	/* 0x10 */
		SQLITE_NULL,	/* 0x11 */
		SQLITE_TEXT,	/* 0x12 */
		SQLITE_NULL,	/* 0x13 */
		SQLITE_INTEGER,	/* 0x14 */
		SQLITE_NULL,	/* 0x15 */
		SQLITE_INTEGER,	/* 0x16 */
		SQLITE_NULL,	/* 0x17 */
		SQLITE_FLOAT,	/* 0x18 */
		SQLITE_NULL,	/* 0x19 */
		SQLITE_FLOAT,	/* 0x1a */
		SQLITE_NULL,	/* 0x1b */
		SQLITE_INTEGER,	/* 0x1c */
		SQLITE_NULL,	/* 0x1d */
		SQLITE_INTEGER,	/* 0x1e */
		SQLITE_NULL,	/* 0x1f */
	};
	return aType[pVal->flags & MEM_AffMask];
}

/* Make a copy of an sqlite3_value object
 */
sqlite3_value *
sqlite3_value_dup(const sqlite3_value * pOrig)
{
	sqlite3_value *pNew;
	if (pOrig == 0)
		return 0;
	pNew = sqlite3_malloc(sizeof(*pNew));
	if (pNew == 0)
		return 0;
	memset(pNew, 0, sizeof(*pNew));
	memcpy(pNew, pOrig, MEMCELLSIZE);
	pNew->flags &= ~MEM_Dyn;
	pNew->db = 0;
	if (pNew->flags & (MEM_Str | MEM_Blob)) {
		pNew->flags &= ~(MEM_Static | MEM_Dyn);
		pNew->flags |= MEM_Ephem;
		if (sqlite3VdbeMemMakeWriteable(pNew) != SQLITE_OK) {
			sqlite3ValueFree(pNew);
			pNew = 0;
		}
	}
	return pNew;
}

/* Destroy an sqlite3_value object previously obtained from
 * sqlite3_value_dup().
 */
void
sqlite3_value_free(sqlite3_value * pOld)
{
	sqlite3ValueFree(pOld);
}

/**************************** sqlite3_result_  ******************************
 * The following routines are used by user-defined functions to specify
 * the function result.
 *
 * The setStrOrError() function calls sqlite3VdbeMemSetStr() to store the
 * result as a string or blob but if the string or blob is too large, it
 * then sets the error code to SQLITE_TOOBIG
 *
 * The invokeValueDestructor(P,X) routine invokes destructor function X()
 * on value P is not going to be used and need to be destroyed.
 */
static void
setResultStrOrError(sqlite3_context * pCtx,	/* Function context */
		    const char *z,	/* String pointer */
		    int n,	/* Bytes in string, or negative */
		    void (*xDel) (void *)	/* Destructor function */
    )
{
	if (sqlite3VdbeMemSetStr(pCtx->pOut, z, n,1, xDel) == SQLITE_TOOBIG) {
		sqlite3_result_error_toobig(pCtx);
	}
}

static int
invokeValueDestructor(const void *p,	/* Value to destroy */
		      void (*xDel) (void *),	/* The destructor */
		      sqlite3_context * pCtx	/* Set a SQLITE_TOOBIG error if no NULL */
    )
{
	assert(xDel != SQLITE_DYNAMIC);
	if (xDel == 0) {
		/* noop */
	} else if (xDel == SQLITE_TRANSIENT) {
		/* noop */
	} else {
		xDel((void *)p);
	}
	if (pCtx)
		sqlite3_result_error_toobig(pCtx);
	return SQLITE_TOOBIG;
}

void
sqlite3_result_msgpack(sqlite3_context * pCtx,
		    const void *z, int n, void (*xDel) (void *)
    )
{
	assert(n >= 0);
	if (sqlite3VdbeMemSetStr(pCtx->pOut, z, n,0, xDel) == SQLITE_TOOBIG) {
		sqlite3_result_error_toobig(pCtx);
	}
	pCtx->pOut->flags|= MEM_Subtype;
	pCtx->pOut->subtype = SQL_SUBTYPE_MSGPACK;
}

void
sqlite3_result_blob(sqlite3_context * pCtx,
		    const void *z, int n, void (*xDel) (void *)
    )
{
	assert(n >= 0);
	if (sqlite3VdbeMemSetStr(pCtx->pOut, z, n,0, xDel) == SQLITE_TOOBIG) {
		sqlite3_result_error_toobig(pCtx);
	}
}

void
sqlite3_result_blob64(sqlite3_context * pCtx,
		      const void *z, sqlite3_uint64 n, void (*xDel) (void *)
    )
{
	assert(xDel != SQLITE_DYNAMIC);
	if (n > 0x7fffffff) {
		(void)invokeValueDestructor(z, xDel, pCtx);
	} else {
		setResultStrOrError(pCtx, z, (int)n, xDel);
	}
}

void
sqlite3_result_double(sqlite3_context * pCtx, double rVal)
{
	sqlite3VdbeMemSetDouble(pCtx->pOut, rVal);
}

void
sqlite3_result_error(sqlite3_context * pCtx, const char *z, int n)
{
	pCtx->isError = SQLITE_ERROR;
	pCtx->fErrorOrAux = 1;
	sqlite3VdbeMemSetStr(pCtx->pOut, z, n, 1, SQLITE_TRANSIENT);
}

void
sqlite3_result_int(sqlite3_context * pCtx, int iVal)
{
	sqlite3VdbeMemSetInt64(pCtx->pOut, (i64) iVal);
}

void
sqlite3_result_int64(sqlite3_context * pCtx, i64 iVal)
{
	sqlite3VdbeMemSetInt64(pCtx->pOut, iVal);
}

void
sqlite3_result_null(sqlite3_context * pCtx)
{
	sqlite3VdbeMemSetNull(pCtx->pOut);
}

void
sqlite3_result_text(sqlite3_context * pCtx,
		    const char *z, int n, void (*xDel) (void *)
    )
{
	setResultStrOrError(pCtx, z, n, xDel);
}

void
sqlite3_result_text64(sqlite3_context * pCtx,
		      const char *z,
		      sqlite3_uint64 n,
		      void (*xDel) (void *))
{
	assert(xDel != SQLITE_DYNAMIC);
	if (n > 0x7fffffff) {
		(void)invokeValueDestructor(z, xDel, pCtx);
	} else {
		setResultStrOrError(pCtx, z, (int)n, xDel);
	}
}

void
sqlite3_result_value(sqlite3_context * pCtx, sqlite3_value * pValue)
{
	sqlite3VdbeMemCopy(pCtx->pOut, pValue);
}

void
sqlite3_result_zeroblob(sqlite3_context * pCtx, int n)
{
	sqlite3VdbeMemSetZeroBlob(pCtx->pOut, n);
}

int
sqlite3_result_zeroblob64(sqlite3_context * pCtx, u64 n)
{
	Mem *pOut = pCtx->pOut;
	if (n > (u64) pOut->db->aLimit[SQLITE_LIMIT_LENGTH]) {
		return SQLITE_TOOBIG;
	}
	sqlite3VdbeMemSetZeroBlob(pCtx->pOut, (int)n);
	return SQLITE_OK;
}

void
sqlite3_result_error_code(sqlite3_context * pCtx, int errCode)
{
	pCtx->isError = errCode;
	pCtx->fErrorOrAux = 1;
	if (pCtx->pOut->flags & MEM_Null) {
		sqlite3VdbeMemSetStr(pCtx->pOut, sqlite3ErrStr(errCode), -1, 1,
				     SQLITE_STATIC);
	}
}

/* Force an SQLITE_TOOBIG error. */
void
sqlite3_result_error_toobig(sqlite3_context * pCtx)
{
	pCtx->isError = SQLITE_TOOBIG;
	pCtx->fErrorOrAux = 1;
	sqlite3VdbeMemSetStr(pCtx->pOut, "string or blob too big", -1, 1,
			     SQLITE_STATIC);
}

/* An SQLITE_NOMEM error. */
void
sqlite3_result_error_nomem(sqlite3_context * pCtx)
{
	sqlite3VdbeMemSetNull(pCtx->pOut);
	pCtx->isError = SQLITE_NOMEM_BKPT;
	pCtx->fErrorOrAux = 1;
	sqlite3OomFault(pCtx->pOut->db);
}

/*
 * Execute the statement pStmt, either until a row of data is ready, the
 * statement is completely executed or an error occurs.
 *
 * This routine implements the bulk of the logic behind the sqlite_step()
 * API.  The only thing omitted is the automatic recompile if a
 * schema change has occurred.  That detail is handled by the
 * outer sqlite3_step() wrapper procedure.
 */
static int
sqlite3Step(Vdbe * p)
{
	sqlite3 *db;
	int rc;

	assert(p);
	if (p->magic != VDBE_MAGIC_RUN) {
		/* We used to require that sqlite3_reset() be called before retrying
		 * sqlite3_step() after any error or after SQLITE_DONE.  But beginning
		 * with version 3.7.0, we changed this so that sqlite3_reset() would
		 * be called automatically instead of throwing the SQLITE_MISUSE error.
		 * This "automatic-reset" change is not technically an incompatibility,
		 * since any application that receives an SQLITE_MISUSE is broken by
		 * definition.
		 *
		 * Nevertheless, some published applications that were originally written
		 * for version 3.6.23 or earlier do in fact depend on SQLITE_MISUSE
		 * returns, and those were broken by the automatic-reset change.  As a
		 * a work-around, the SQLITE_OMIT_AUTORESET compile-time restores the
		 * legacy behavior of returning SQLITE_MISUSE for cases where the
		 * previous sqlite3_step() returned something other than a SQLITE_LOCKED
		 * or SQLITE_BUSY error.
		 */
#ifdef SQLITE_OMIT_AUTORESET
		if ((rc = p->rc & 0xff) == SQLITE_BUSY || rc == SQLITE_LOCKED) {
			sqlite3_reset((sqlite3_stmt *) p);
		} else {
			return SQLITE_MISUSE_BKPT;
		}
#else
		sqlite3_reset((sqlite3_stmt *) p);
#endif
	}

	/* Check that malloc() has not failed. If it has, return early. */
	db = p->db;
	if (db->mallocFailed) {
		p->rc = SQLITE_NOMEM;
		return SQLITE_NOMEM_BKPT;
	}

	if (p->pc <= 0 && p->expired) {
		p->rc = SQLITE_SCHEMA;
		rc = SQLITE_ERROR;
		goto end_of_step;
	}
	if (p->pc < 0) {
		/* If there are no other statements currently running, then
		 * reset the interrupt flag.  This prevents a call to sqlite3_interrupt
		 * from interrupting a statement that has not yet started.
		 */
		if (db->nVdbeActive == 0) {
			db->u1.isInterrupted = 0;
		}

#ifndef SQLITE_OMIT_TRACE
		if ((db->xProfile || (db->mTrace & SQLITE_TRACE_PROFILE) != 0)
		    && !db->init.busy && p->zSql) {
			sqlite3OsCurrentTimeInt64(db->pVfs, &p->startTime);
		} else {
			assert(p->startTime == 0);
		}
#endif

		db->nVdbeActive++;
		p->pc = 0;
	}
	if (p->explain) {
		rc = sqlite3VdbeList(p);
	} else {
		db->nVdbeExec++;
		rc = sqlite3VdbeExec(p);
		db->nVdbeExec--;
	}

#ifndef SQLITE_OMIT_TRACE
	/* If the statement completed successfully, invoke the profile callback */
	if (rc != SQLITE_ROW)
		checkProfileCallback(db, p);
#endif

	db->errCode = rc;
	if (SQLITE_NOMEM == sqlite3ApiExit(p->db, p->rc)) {
		p->rc = SQLITE_NOMEM_BKPT;
	}
 end_of_step:
	/* At this point local variable rc holds the value that should be
	 * returned if this statement was compiled using the legacy
	 * sqlite3_prepare() interface. According to the docs, this can only
	 * be one of the values in the first assert() below. Variable p->rc
	 * contains the value that would be returned if sqlite3_finalize()
	 * were called on statement p.
	 */
	assert(rc == SQLITE_ROW || rc == SQLITE_DONE || rc == SQLITE_ERROR
	       || (rc & 0xff) == SQLITE_BUSY || rc == SQLITE_MISUSE);
	if (p->isPrepareV2 && rc != SQLITE_ROW && rc != SQLITE_DONE) {
		/* If this statement was prepared using sqlite3_prepare_v2(), and an
		 * error has occurred, then return the error code in p->rc to the
		 * caller. Set the error code in the database handle to the same value.
		 */
		rc = sqlite3VdbeTransferError(p);
	}
	return (rc & db->errMask);
}

/*
 * This is the top-level implementation of sqlite3_step().  Call
 * sqlite3Step() to do most of the work.  If a schema error occurs,
 * call sqlite3Reprepare() and try again.
 */
int
sqlite3_step(sqlite3_stmt * pStmt)
{
	int rc;			/* Result from sqlite3Step() */
	int rc2 = SQLITE_OK;	/* Result from sqlite3Reprepare() */
	Vdbe *v = (Vdbe *) pStmt;	/* the prepared statement */
	int cnt = 0;		/* Counter to prevent infinite loop of reprepares */
	sqlite3 *db;		/* The database connection */

	if (vdbeSafetyNotNull(v)) {
		return SQLITE_MISUSE_BKPT;
	}
	db = v->db;
	v->doingRerun = 0;
	while ((rc = sqlite3Step(v)) == SQLITE_SCHEMA
	       && cnt++ < SQLITE_MAX_SCHEMA_RETRY) {
		int savedPc = v->pc;
		rc2 = rc = sqlite3Reprepare(v);
		if (rc != SQLITE_OK)
			break;
		sqlite3_reset(pStmt);
		if (savedPc >= 0)
			v->doingRerun = 1;
		assert(v->expired == 0);
	}
	if (rc2 != SQLITE_OK) {
		/* This case occurs after failing to recompile an sql statement.
		 * The error message from the SQL compiler has already been loaded
		 * into the database handle. This block copies the error message
		 * from the database handle into the statement and sets the statement
		 * program counter to 0 to ensure that when the statement is
		 * finalized or reset the parser error message is available via
		 * sqlite3_errmsg() and sqlite3_errcode().
		 */
		const char *zErr = (const char *)sqlite3_value_text(db->pErr);
		sqlite3DbFree(db, v->zErrMsg);
		if (!db->mallocFailed) {
			v->zErrMsg = sqlite3DbStrDup(db, zErr);
			v->rc = rc2;
		} else {
			v->zErrMsg = 0;
			v->rc = rc = SQLITE_NOMEM_BKPT;
		}
	}
	rc = sqlite3ApiExit(db, rc);
	return rc;
}

/*
 * Extract the user data from a sqlite3_context structure and return a
 * pointer to it.
 */
void *
sqlite3_user_data(sqlite3_context * p)
{
	assert(p && p->pFunc);
	return p->pFunc->pUserData;
}

/*
 * Extract the user data from a sqlite3_context structure and return a
 * pointer to it.
 *
 * IMPLEMENTATION-OF: R-46798-50301 The sqlite3_context_db_handle() interface
 * returns a copy of the pointer to the database connection (the 1st
 * parameter) of the sqlite3_create_function() and
 * sqlite3_create_function16() routines that originally registered the
 * application defined function.
 */
sqlite3 *
sqlite3_context_db_handle(sqlite3_context * p)
{
	assert(p && p->pOut);
	return p->pOut->db;
}

/*
 * Return the current time for a statement.  If the current time
 * is requested more than once within the same run of a single prepared
 * statement, the exact same time is returned for each invocation regardless
 * of the amount of time that elapses between invocations.  In other words,
 * the time returned is always the time of the first call.
 */
sqlite3_int64
sqlite3StmtCurrentTime(sqlite3_context * p)
{
	int rc;
#ifndef SQLITE_ENABLE_OR_STAT4
	sqlite3_int64 *piTime = &p->pVdbe->iCurrentTime;
	assert(p->pVdbe != 0);
#else
	sqlite3_int64 iTime = 0;
	sqlite3_int64 *piTime =
	    p->pVdbe != 0 ? &p->pVdbe->iCurrentTime : &iTime;
#endif
	if (*piTime == 0) {
		rc = sqlite3OsCurrentTimeInt64(p->pOut->db->pVfs, piTime);
		if (rc)
			*piTime = 0;
	}
	return *piTime;
}

/*
 * The following is the implementation of an SQL function that always
 * fails with an error message stating that the function is used in the
 * wrong context.  The sqlite3_overload_function() API might construct
 * SQL function that use this routine so that the functions will exist
 * for name resolution.
 */
void
sqlite3InvalidFunction(sqlite3_context * context,	/* The function calling context */
		       int NotUsed,	/* Number of arguments to the function */
		       sqlite3_value ** NotUsed2	/* Value of each argument */
    )
{
	const char *zName = context->pFunc->zName;
	char *zErr;
	UNUSED_PARAMETER2(NotUsed, NotUsed2);
	zErr =
	    sqlite3_mprintf
	    ("unable to use function %s in the requested context", zName);
	sqlite3_result_error(context, zErr, -1);
	sqlite3_free(zErr);
}

/*
 * Create a new aggregate context for p and return a pointer to
 * its pMem->z element.
 */
static SQLITE_NOINLINE void *
createAggContext(sqlite3_context * p, int nByte)
{
	Mem *pMem = p->pMem;
	assert((pMem->flags & MEM_Agg) == 0);
	if (nByte <= 0) {
		sqlite3VdbeMemSetNull(pMem);
		pMem->z = 0;
	} else {
		sqlite3VdbeMemClearAndResize(pMem, nByte);
		pMem->flags = MEM_Agg;
		pMem->u.pDef = p->pFunc;
		if (pMem->z) {
			memset(pMem->z, 0, nByte);
		}
	}
	return (void *)pMem->z;
}

/*
 * Allocate or return the aggregate context for a user function.  A new
 * context is allocated on the first call.  Subsequent calls return the
 * same context that was returned on prior calls.
 */
void *
sqlite3_aggregate_context(sqlite3_context * p, int nByte)
{
	assert(p && p->pFunc && p->pFunc->xFinalize);
	testcase(nByte < 0);
	if ((p->pMem->flags & MEM_Agg) == 0) {
		return createAggContext(p, nByte);
	} else {
		return (void *)p->pMem->z;
	}
}

/*
 * Return the auxiliary data pointer, if any, for the iArg'th argument to
 * the user-function defined by pCtx.
 */
void *
sqlite3_get_auxdata(sqlite3_context * pCtx, int iArg)
{
	AuxData *pAuxData;

	if (pCtx->pVdbe == 0)
		return 0;

	for (pAuxData = pCtx->pVdbe->pAuxData; pAuxData;
	     pAuxData = pAuxData->pNext) {
		if (pAuxData->iOp == pCtx->iOp && pAuxData->iArg == iArg)
			break;
	}

	return (pAuxData ? pAuxData->pAux : 0);
}

/*
 * Set the auxiliary data pointer and delete function, for the iArg'th
 * argument to the user-function defined by pCtx. Any previous value is
 * deleted by calling the delete function specified when it was set.
 */
void
sqlite3_set_auxdata(sqlite3_context * pCtx,
		    int iArg, void *pAux, void (*xDelete) (void *)
    )
{
	AuxData *pAuxData;
	Vdbe *pVdbe = pCtx->pVdbe;

	if (iArg < 0)
		goto failed;
	if (pVdbe == 0)
		goto failed;

	for (pAuxData = pVdbe->pAuxData; pAuxData; pAuxData = pAuxData->pNext) {
		if (pAuxData->iOp == pCtx->iOp && pAuxData->iArg == iArg)
			break;
	}
	if (pAuxData == 0) {
		pAuxData = sqlite3DbMallocZero(pVdbe->db, sizeof(AuxData));
		if (!pAuxData)
			goto failed;
		pAuxData->iOp = pCtx->iOp;
		pAuxData->iArg = iArg;
		pAuxData->pNext = pVdbe->pAuxData;
		pVdbe->pAuxData = pAuxData;
		if (pCtx->fErrorOrAux == 0) {
			pCtx->isError = 0;
			pCtx->fErrorOrAux = 1;
		}
	} else if (pAuxData->xDelete) {
		pAuxData->xDelete(pAuxData->pAux);
	}

	pAuxData->pAux = pAux;
	pAuxData->xDelete = xDelete;
	return;

 failed:
	if (xDelete) {
		xDelete(pAux);
	}
}

/*
 * Return the number of columns in the result set for the statement pStmt.
 */
int
sqlite3_column_count(sqlite3_stmt * pStmt)
{
	Vdbe *pVm = (Vdbe *) pStmt;
	return pVm ? pVm->nResColumn : 0;
}

/*
 * Return the number of values available from the current row of the
 * currently executing statement pStmt.
 */
int
sqlite3_data_count(sqlite3_stmt * pStmt)
{
	Vdbe *pVm = (Vdbe *) pStmt;
	if (pVm == 0 || pVm->pResultSet == 0)
		return 0;
	return pVm->nResColumn;
}

/*
 * Return a pointer to static memory containing an SQL NULL value.
 */
static const Mem *
columnNullValue(void)
{
	/* Even though the Mem structure contains an element
	 * of type i64, on certain architectures (x86) with certain compiler
	 * switches (-Os), gcc may align this Mem object on a 4-byte boundary
	 * instead of an 8-byte one. This all works fine, except that when
	 * running with SQLITE_DEBUG defined the SQLite code sometimes assert()s
	 * that a Mem structure is located on an 8-byte boundary. To prevent
	 * these assert()s from failing, when building with SQLITE_DEBUG defined
	 * using gcc, we force nullMem to be 8-byte aligned using the magical
	 * __attribute__((aligned(8))) macro.
	 */
	static const Mem nullMem
#if defined(SQLITE_DEBUG) && defined(__GNUC__)
	    __attribute__ ((aligned(8)))
#endif
	    = {
		/* .u          = */  {
		0},
		    /* .flags      = */ (u16) MEM_Null,
		    /* .eSubtype   = */ (u8) 0,
		    /* .n          = */ (int)0,
		    /* .z          = */ (char *)0,
		    /* .zMalloc    = */ (char *)0,
		    /* .szMalloc   = */ (int)0,
		    /* .uTemp      = */ (u32) 0,
		    /* .db         = */ (sqlite3 *) 0,
		    /* .xDel       = */ (void (*)(void *))0,
#ifdef SQLITE_DEBUG
		    /* .pScopyFrom = */ (Mem *) 0,
		    /* .pFiller    = */ (void *)0,
#endif
	};
	return &nullMem;
}

/*
 * Check to see if column iCol of the given statement is valid.  If
 * it is, return a pointer to the Mem for the value of that column.
 * If iCol is not valid, return a pointer to a Mem which has a value
 * of NULL.
 */
static Mem *
columnMem(sqlite3_stmt * pStmt, int i)
{
	Vdbe *pVm;
	Mem *pOut;

	pVm = (Vdbe *) pStmt;
	if (pVm == 0)
		return (Mem *) columnNullValue();
	assert(pVm->db);
	if (pVm->pResultSet != 0 && i < pVm->nResColumn && i >= 0) {
		pOut = &pVm->pResultSet[i];
	} else {
		sqlite3Error(pVm->db, SQLITE_RANGE);
		pOut = (Mem *) columnNullValue();
	}
	return pOut;
}

/*
 * This function is called after invoking an sqlite3_value_XXX function on a
 * column value (i.e. a value returned by evaluating an SQL expression in the
 * select list of a SELECT statement) that may cause a malloc() failure. If
 * malloc() has failed, the threads mallocFailed flag is cleared and the result
 * code of statement pStmt set to SQLITE_NOMEM.
 *
 * Specifically, this is called from within:
 *
 *     sqlite3_column_int()
 *     sqlite3_column_int64()
 *     sqlite3_column_text()
 *     sqlite3_column_real()
 *     sqlite3_column_bytes()
 *     sqlite3_column_bytes16()
 *     sqiite3_column_blob()
 */
static void
columnMallocFailure(sqlite3_stmt * pStmt)
{
	/* If malloc() failed during an encoding conversion within an
	 * sqlite3_column_XXX API, then set the return code of the statement to
	 * SQLITE_NOMEM. The next call to _step() (if any) will return SQLITE_ERROR
	 * and _finalize() will return NOMEM.
	 */
	Vdbe *p = (Vdbe *) pStmt;
	if (p) {
		assert(p->db != 0);
		p->rc = sqlite3ApiExit(p->db, p->rc);
	}
}

/**************************** sqlite3_column_  ******************************
 * The following routines are used to access elements of the current row
 * in the result set.
 */
const void *
sqlite3_column_blob(sqlite3_stmt * pStmt, int i)
{
	const void *val;
	val = sqlite3_value_blob(columnMem(pStmt, i));
	/* Even though there is no encoding conversion, value_blob() might
	 * need to call malloc() to expand the result of a zeroblob()
	 * expression.
	 */
	columnMallocFailure(pStmt);
	return val;
}

int
sqlite3_column_bytes(sqlite3_stmt * pStmt, int i)
{
	int val = sqlite3_value_bytes(columnMem(pStmt, i));
	columnMallocFailure(pStmt);
	return val;
}

double
sqlite3_column_double(sqlite3_stmt * pStmt, int i)
{
	double val = sqlite3_value_double(columnMem(pStmt, i));
	columnMallocFailure(pStmt);
	return val;
}

int
sqlite3_column_int(sqlite3_stmt * pStmt, int i)
{
	int val = sqlite3_value_int(columnMem(pStmt, i));
	columnMallocFailure(pStmt);
	return val;
}

sqlite_int64
sqlite3_column_int64(sqlite3_stmt * pStmt, int i)
{
	sqlite_int64 val = sqlite3_value_int64(columnMem(pStmt, i));
	columnMallocFailure(pStmt);
	return val;
}

const unsigned char *
sqlite3_column_text(sqlite3_stmt * pStmt, int i)
{
	const unsigned char *val = sqlite3_value_text(columnMem(pStmt, i));
	columnMallocFailure(pStmt);
	return val;
}

sqlite3_value *
sqlite3_column_value(sqlite3_stmt * pStmt, int i)
{
	Mem *pOut = columnMem(pStmt, i);
	if (pOut->flags & MEM_Static) {
		pOut->flags &= ~MEM_Static;
		pOut->flags |= MEM_Ephem;
	}
	columnMallocFailure(pStmt);
	return (sqlite3_value *) pOut;
}

int
sqlite3_column_type(sqlite3_stmt * pStmt, int i)
{
	int iType = sqlite3_value_type(columnMem(pStmt, i));
	columnMallocFailure(pStmt);
	return iType;
}

enum sql_subtype
sql_column_subtype(struct sqlite3_stmt *stmt, int i)
{
	return sqlite3_value_subtype(columnMem(stmt, i));
}

/*
 * Convert the N-th element of pStmt->pColName[] into a string using
 * xFunc() then return that string.  If N is out of range, return 0.
 *
 * There are up to 5 names for each column.  useType determines which
 * name is returned.  Here are the names:
 *
 *    0      The column name as it should be displayed for output
 *    1      The datatype name for the column
 *    2      The name of the database that the column derives from
 *    3      The name of the table that the column derives from
 *    4      The name of the table column that the result column derives from
 *
 * If the result is not a simple column reference (if it is an expression
 * or a constant) then useTypes 2, 3, and 4 return NULL.
 */
static const void *
columnName(sqlite3_stmt * pStmt,
	   int N, const void *(*xFunc) (Mem *), int useType)
{
	const void *ret;
	Vdbe *p;
	int n;
	sqlite3 *db;
#ifdef SQLITE_ENABLE_API_ARMOR
	if (pStmt == 0) {
		(void)SQLITE_MISUSE_BKPT;
		return 0;
	}
#endif
	ret = 0;
	p = (Vdbe *) pStmt;
	db = p->db;
	assert(db != 0);
	n = sqlite3_column_count(pStmt);
	if (N < n && N >= 0) {
		N += useType * n;
		assert(db->mallocFailed == 0);
		ret = xFunc(&p->aColName[N]);
		/* A malloc may have failed inside of the xFunc() call. If this
		 * is the case, clear the mallocFailed flag and return NULL.
		 */
		if (db->mallocFailed) {
			sqlite3OomClear(db);
			ret = 0;
		}
	}
	return ret;
}

/*
 * Return the name of the Nth column of the result set returned by SQL
 * statement pStmt.
 */
const char *
sqlite3_column_name(sqlite3_stmt * pStmt, int N)
{
	return columnName(pStmt, N, (const void *(*)(Mem *))sqlite3_value_text,
			  COLNAME_NAME);
}

const char *
sqlite3_column_datatype(sqlite3_stmt *pStmt, int N)
{
	return columnName(pStmt, N, (const void *(*)(Mem *))sqlite3_value_text,
			  COLNAME_DECLTYPE);
}

/*
 * Constraint:  If you have ENABLE_COLUMN_METADATA then you must
 * not define OMIT_DECLTYPE.
 */
#if defined(SQLITE_OMIT_DECLTYPE) && defined(SQLITE_ENABLE_COLUMN_METADATA)
#error "Must not define both SQLITE_OMIT_DECLTYPE \
         and SQLITE_ENABLE_COLUMN_METADATA"
#endif

#ifndef SQLITE_OMIT_DECLTYPE
/*
 * Return the column declaration type (if applicable) of the 'i'th column
 * of the result set of SQL statement pStmt.
 */
const char *
sqlite3_column_decltype(sqlite3_stmt * pStmt, int N)
{
	return columnName(pStmt, N, (const void *(*)(Mem *))sqlite3_value_text,
			  COLNAME_DECLTYPE);
}
#endif				/* SQLITE_OMIT_DECLTYPE */

#ifdef SQLITE_ENABLE_COLUMN_METADATA
/*
 * Return the name of the database from which a result column derives.
 * NULL is returned if the result column is an expression or constant or
 * anything else which is not an unambiguous reference to a database column.
 */
const char *
sqlite3_column_database_name(sqlite3_stmt * pStmt, int N)
{
	return columnName(pStmt, N, (const void *(*)(Mem *))sqlite3_value_text,
			  COLNAME_DATABASE);
}

/*
 * Return the name of the table from which a result column derives.
 * NULL is returned if the result column is an expression or constant or
 * anything else which is not an unambiguous reference to a database column.
 */
const char *
sqlite3_column_table_name(sqlite3_stmt * pStmt, int N)
{
	return columnName(pStmt, N, (const void *(*)(Mem *))sqlite3_value_text,
			  COLNAME_TABLE);
}

/*
 * Return the name of the table column from which a result column derives.
 * NULL is returned if the result column is an expression or constant or
 * anything else which is not an unambiguous reference to a database column.
 */
const char *
sqlite3_column_origin_name(sqlite3_stmt * pStmt, int N)
{
	return columnName(pStmt, N, (const void *(*)(Mem *))sqlite3_value_text,
			  COLNAME_COLUMN);
}
#endif				/* SQLITE_ENABLE_COLUMN_METADATA */

/******************************* sqlite3_bind_  **************************
 *
 * Routines used to attach values to wildcards in a compiled SQL statement.
 */
/*
 * Unbind the value bound to variable i in virtual machine p. This is the
 * the same as binding a NULL value to the column. If the "i" parameter is
 * out of range, then SQLITE_RANGE is returned. Othewise SQLITE_OK.
 *
 * The error code stored in database p->db is overwritten with the return
 * value in any case.
 */
static int
vdbeUnbind(Vdbe * p, int i)
{
	Mem *pVar;
	if (vdbeSafetyNotNull(p)) {
		return SQLITE_MISUSE_BKPT;
	}
	if (p->magic != VDBE_MAGIC_RUN || p->pc >= 0) {
		sqlite3Error(p->db, SQLITE_MISUSE);
		sqlite3_log(SQLITE_MISUSE,
			    "bind on a busy prepared statement: [%s]", p->zSql);
		return SQLITE_MISUSE_BKPT;
	}
	if (i < 1 || i > p->nVar) {
		sqlite3Error(p->db, SQLITE_RANGE);
		return SQLITE_RANGE;
	}
	i--;
	pVar = &p->aVar[i];
	sqlite3VdbeMemRelease(pVar);
	pVar->flags = MEM_Null;
	sqlite3Error(p->db, SQLITE_OK);

	/* If the bit corresponding to this variable in Vdbe.expmask is set, then
	 * binding a new value to this variable invalidates the current query plan.
	 *
	 * IMPLEMENTATION-OF: R-48440-37595 If the specific value bound to host
	 * parameter in the WHERE clause might influence the choice of query plan
	 * for a statement, then the statement will be automatically recompiled,
	 * as if there had been a schema change, on the first sqlite3_step() call
	 * following any change to the bindings of that parameter.
	 */
	if (p->isPrepareV2 &&
	    ((i < 32 && p->expmask & ((u32) 1 << i))
	     || p->expmask == 0xffffffff)
	    ) {
		p->expired = 1;
	}
	return SQLITE_OK;
}

/**
 * This function sets type for bound variable.
 * We should bind types only for variables which occur in
 * result set of SELECT query. For example:
 *
 * SELECT id, ?, ?, a WHERE id = ?;
 *
 * In this case we should set types only for two variables.
 * That one which is situated under WHERE condition - is out
 * of our interest.
 *
 * For named binding parameters we should propagate type
 * for all occurrences of this parameter - since binding
 * routine takes place only once for each DISTINCT parameter
 * from list.
 *
 * @param v Current VDBE.
 * @param position Ordinal position of binding parameter.
 * @param type String literal representing type of binding param.
 * @retval 0 on success.
 */
static int
sql_bind_type(struct Vdbe *v, uint32_t position, const char *type)
{
	if (v->res_var_count < position)
		return 0;
	int rc = sqlite3VdbeSetColName(v, v->var_pos[position - 1],
				       COLNAME_DECLTYPE, type,
				       SQLITE_TRANSIENT);
	const char *bind_name = v->aColName[position - 1].z;
	if (strcmp(bind_name, "?") == 0)
		return rc;
	for (uint32_t i = position; i < v->res_var_count; ++i) {
		if (strcmp(bind_name,  v->aColName[i].z) == 0) {
			rc = sqlite3VdbeSetColName(v, v->var_pos[i],
						   COLNAME_DECLTYPE, type,
						   SQLITE_TRANSIENT);
			if (rc != 0)
				return rc;
		}
	}
	return 0;
}

/*
 * Bind a text or BLOB value.
 */
static int
bindText(sqlite3_stmt * pStmt,	/* The statement to bind against */
	 int i,			/* Index of the parameter to bind */
	 const void *zData,	/* Pointer to the data to be bound */
	 int nData,		/* Number of bytes of data to be bound */
	 void (*xDel) (void *)	/* Destructor for the data */
    )
{
	Vdbe *p = (Vdbe *) pStmt;
	Mem *pVar;
	int rc;

	rc = vdbeUnbind(p, i);
	if (rc == SQLITE_OK) {
		if (zData != 0) {
			pVar = &p->aVar[i - 1];
			rc = sqlite3VdbeMemSetStr(pVar, zData, nData, 1, xDel);
			if (rc == SQLITE_OK)
				rc = sql_bind_type(p, i, "TEXT");
			sqlite3Error(p->db, rc);
			rc = sqlite3ApiExit(p->db, rc);
		}
	} else if (xDel != SQLITE_STATIC && xDel != SQLITE_TRANSIENT) {
		xDel((void *)zData);
	}
	return rc;
}

/*
 * Bind a blob value to an SQL statement variable.
 */
int
sqlite3_bind_blob(sqlite3_stmt * pStmt,
		  int i, const void *zData, int nData, void (*xDel) (void *)
    )
{
#ifdef SQLITE_ENABLE_API_ARMOR
	if (nData < 0)
		return SQLITE_MISUSE_BKPT;
#endif
	return bindText(pStmt, i, zData, nData, xDel);
}

int
sqlite3_bind_blob64(sqlite3_stmt * pStmt,
		    int i,
		    const void *zData,
		    sqlite3_uint64 nData, void (*xDel) (void *)
    )
{
	assert(xDel != SQLITE_DYNAMIC);
	if (nData > 0x7fffffff) {
		return invokeValueDestructor(zData, xDel, 0);
	} else {
		return bindText(pStmt, i, zData, (int)nData, xDel);
	}
}

int
sqlite3_bind_double(sqlite3_stmt * pStmt, int i, double rValue)
{
	int rc;
	Vdbe *p = (Vdbe *) pStmt;
	rc = vdbeUnbind(p, i);
	if (rc == SQLITE_OK) {
		rc = sql_bind_type(p, i, "NUMERIC");
		sqlite3VdbeMemSetDouble(&p->aVar[i - 1], rValue);
	}
	return rc;
}

int
sqlite3_bind_int(sqlite3_stmt * p, int i, int iValue)
{
	return sqlite3_bind_int64(p, i, (i64) iValue);
}

int
sqlite3_bind_int64(sqlite3_stmt * pStmt, int i, sqlite_int64 iValue)
{
	int rc;
	Vdbe *p = (Vdbe *) pStmt;
	rc = vdbeUnbind(p, i);
	if (rc == SQLITE_OK) {
		rc = sql_bind_type(p, i, "INTEGER");
		sqlite3VdbeMemSetInt64(&p->aVar[i - 1], iValue);
	}
	return rc;
}

int
sqlite3_bind_null(sqlite3_stmt * pStmt, int i)
{
	int rc;
	Vdbe *p = (Vdbe *) pStmt;
	rc = vdbeUnbind(p, i);
	if (rc == SQLITE_OK)
		rc = sql_bind_type(p, i, "BOOLEAN");
	return rc;
}

int
sqlite3_bind_text(sqlite3_stmt * pStmt,
		  int i, const char *zData, int nData, void (*xDel) (void *)
    )
{
	return bindText(pStmt, i, zData, nData, xDel);
}

int
sqlite3_bind_text64(sqlite3_stmt * pStmt,
		    int i,
		    const char *zData,
		    sqlite3_uint64 nData,
		    void (*xDel) (void *))
{
	assert(xDel != SQLITE_DYNAMIC);
	if (nData > 0x7fffffff) {
		return invokeValueDestructor(zData, xDel, 0);
	} else {
		return bindText(pStmt, i, zData, (int)nData, xDel);
	}
}

int
sqlite3_bind_value(sqlite3_stmt * pStmt, int i, const sqlite3_value * pValue)
{
	int rc;
	switch (sqlite3_value_type((sqlite3_value *) pValue)) {
	case SQLITE_INTEGER:{
			rc = sqlite3_bind_int64(pStmt, i, pValue->u.i);
			break;
		}
	case SQLITE_FLOAT:{
			rc = sqlite3_bind_double(pStmt, i, pValue->u.r);
			break;
		}
	case SQLITE_BLOB:{
			if (pValue->flags & MEM_Zero) {
				rc = sqlite3_bind_zeroblob(pStmt, i,
							   pValue->u.nZero);
			} else {
				rc = sqlite3_bind_blob(pStmt, i, pValue->z,
						       pValue->n,
						       SQLITE_TRANSIENT);
			}
			break;
		}
	case SQLITE_TEXT:{
			rc = bindText(pStmt, i, pValue->z, pValue->n,
				      SQLITE_TRANSIENT);
			break;
		}
	default:{
			rc = sqlite3_bind_null(pStmt, i);
			break;
		}
	}
	return rc;
}

int
sqlite3_bind_zeroblob(sqlite3_stmt * pStmt, int i, int n)
{
	int rc;
	Vdbe *p = (Vdbe *) pStmt;
	rc = vdbeUnbind(p, i);
	if (rc == SQLITE_OK) {
		sqlite3VdbeMemSetZeroBlob(&p->aVar[i - 1], n);
	}
	return rc;
}

int
sqlite3_bind_zeroblob64(sqlite3_stmt * pStmt, int i, sqlite3_uint64 n)
{
	int rc;
	Vdbe *p = (Vdbe *) pStmt;
	if (n > (u64) p->db->aLimit[SQLITE_LIMIT_LENGTH]) {
		rc = SQLITE_TOOBIG;
	} else {
		assert((n & 0x7FFFFFFF) == n);
		rc = sqlite3_bind_zeroblob(pStmt, i, n);
	}
	rc = sqlite3ApiExit(p->db, rc);
	return rc;
}

/*
 * Return the number of wildcards that can be potentially bound to.
 * This routine is added to support DBD::SQLite.
 */
int
sqlite3_bind_parameter_count(sqlite3_stmt * pStmt)
{
	Vdbe *p = (Vdbe *) pStmt;
	return p ? p->nVar : 0;
}

/*
 * Return the name of a wildcard parameter.  Return NULL if the index
 * is out of range or if the wildcard is unnamed.
 *
 * The result is always UTF-8.
 */
const char *
sqlite3_bind_parameter_name(sqlite3_stmt * pStmt, int i)
{
	Vdbe *p = (Vdbe *) pStmt;
	if (p == 0)
		return 0;
	return sqlite3VListNumToName(p->pVList, i);
}

/*
 * Given a wildcard parameter name, return the index of the variable
 * with that name.  If there is no variable with the given name,
 * return 0.
 */
int
sqlite3VdbeParameterIndex(Vdbe * p, const char *zName, int nName)
{
	if (p == 0 || zName == 0)
		return 0;
	return sqlite3VListNameToNum(p->pVList, zName, nName);
}

int
sqlite3_bind_parameter_index(sqlite3_stmt * pStmt, const char *zName)
{
	return sqlite3VdbeParameterIndex((Vdbe *) pStmt, zName,
					 sqlite3Strlen30(zName));
}

int
sqlite3_bind_parameter_lindex(sqlite3_stmt * pStmt, const char *zName,
			      int nName)
{
	return sqlite3VdbeParameterIndex((Vdbe *) pStmt, zName, nName);
}

/*
 * Transfer all bindings from the first statement over to the second.
 */
int
sqlite3TransferBindings(sqlite3_stmt * pFromStmt, sqlite3_stmt * pToStmt)
{
	Vdbe *pFrom = (Vdbe *) pFromStmt;
	Vdbe *pTo = (Vdbe *) pToStmt;
	int i;
	assert(pTo->db == pFrom->db);
	assert(pTo->nVar == pFrom->nVar);
	for (i = 0; i < pFrom->nVar; i++) {
		sqlite3VdbeMemMove(&pTo->aVar[i], &pFrom->aVar[i]);
	}
	return SQLITE_OK;
}

/*
 * Return the sqlite3* database handle to which the prepared statement given
 * in the argument belongs.  This is the same database handle that was
 * the first argument to the sqlite3_prepare() that was used to create
 * the statement in the first place.
 */
sqlite3 *
sqlite3_db_handle(sqlite3_stmt * pStmt)
{
	return pStmt ? ((Vdbe *) pStmt)->db : 0;
}

/*
 * Return true if the prepared statement is in need of being reset.
 */
int
sqlite3_stmt_busy(sqlite3_stmt * pStmt)
{
	Vdbe *v = (Vdbe *) pStmt;
	return v != 0 && v->magic == VDBE_MAGIC_RUN && v->pc >= 0;
}

/*
 * Return a pointer to the next prepared statement after pStmt associated
 * with database connection pDb.  If pStmt is NULL, return the first
 * prepared statement for the database connection.  Return NULL if there
 * are no more.
 */
sqlite3_stmt *
sqlite3_next_stmt(sqlite3 * pDb, sqlite3_stmt * pStmt)
{
	sqlite3_stmt *pNext;
#ifdef SQLITE_ENABLE_API_ARMOR
	if (!sqlite3SafetyCheckOk(pDb)) {
		(void)SQLITE_MISUSE_BKPT;
		return 0;
	}
#endif
	if (pStmt == 0) {
		pNext = (sqlite3_stmt *) pDb->pVdbe;
	} else {
		pNext = (sqlite3_stmt *) ((Vdbe *) pStmt)->pNext;
	}
	return pNext;
}

/*
 * Return the value of a status counter for a prepared statement
 */
int
sqlite3_stmt_status(sqlite3_stmt * pStmt, int op, int resetFlag)
{
	Vdbe *pVdbe = (Vdbe *) pStmt;
	u32 v;
#ifdef SQLITE_ENABLE_API_ARMOR
	if (!pStmt) {
		(void)SQLITE_MISUSE_BKPT;
		return 0;
	}
#endif
	v = pVdbe->aCounter[op];
	if (resetFlag)
		pVdbe->aCounter[op] = 0;
	return (int)v;
}

/*
 * Return the SQL associated with a prepared statement
 */
const char *
sqlite3_sql(sqlite3_stmt * pStmt)
{
	Vdbe *p = (Vdbe *) pStmt;
	return p ? p->zSql : 0;
}

/*
 * Return the SQL associated with a prepared statement with
 * bound parameters expanded.  Space to hold the returned string is
 * obtained from sqlite3_malloc().  The caller is responsible for
 * freeing the returned string by passing it to sqlite3_free().
 *
 * The SQLITE_TRACE_SIZE_LIMIT puts an upper bound on the size of
 * expanded bound parameters.
 */
char *
sqlite3_expanded_sql(sqlite3_stmt * pStmt)
{
#ifdef SQLITE_OMIT_TRACE
	return 0;
#else
	char *z = 0;
	const char *zSql = sqlite3_sql(pStmt);
	if (zSql) {
		Vdbe *p = (Vdbe *) pStmt;
		z = sqlite3VdbeExpandSql(p, zSql);
	}
	return z;
#endif
}

#ifdef SQLITE_ENABLE_STMT_SCANSTATUS
/*
 * Return status data for a single loop within query pStmt.
 */
int
sqlite3_stmt_scanstatus(sqlite3_stmt * pStmt,	/* Prepared statement being queried */
			int idx,	/* Index of loop to report on */
			int iScanStatusOp,	/* Which metric to return */
			void *pOut	/* OUT: Write the answer here */
    )
{
	Vdbe *p = (Vdbe *) pStmt;
	ScanStatus *pScan;
	if (idx < 0 || idx >= p->nScan)
		return 1;
	pScan = &p->aScan[idx];
	switch (iScanStatusOp) {
	case SQLITE_SCANSTAT_NLOOP:{
			*(sqlite3_int64 *) pOut = p->anExec[pScan->addrLoop];
			break;
		}
	case SQLITE_SCANSTAT_NVISIT:{
			*(sqlite3_int64 *) pOut = p->anExec[pScan->addrVisit];
			break;
		}
	case SQLITE_SCANSTAT_EST:{
			double r = 1.0;
			LogEst x = pScan->nEst;
			while (x < 100) {
				x += 10;
				r *= 0.5;
			}
			*(double *)pOut = r * sqlite3LogEstToInt(x);
			break;
		}
	case SQLITE_SCANSTAT_NAME:{
			*(const char **)pOut = pScan->zName;
			break;
		}
	case SQLITE_SCANSTAT_EXPLAIN:{
			if (pScan->addrExplain) {
				*(const char **)pOut =
				    p->aOp[pScan->addrExplain].p4.z;
			} else {
				*(const char **)pOut = 0;
			}
			break;
		}
	case SQLITE_SCANSTAT_SELECTID:{
			if (pScan->addrExplain) {
				*(int *)pOut = p->aOp[pScan->addrExplain].p1;
			} else {
				*(int *)pOut = -1;
			}
			break;
		}
	default:{
			return 1;
		}
	}
	return 0;
}

/*
 * Zero all counters associated with the sqlite3_stmt_scanstatus() data.
 */
void
sqlite3_stmt_scanstatus_reset(sqlite3_stmt * pStmt)
{
	Vdbe *p = (Vdbe *) pStmt;
	memset(p->anExec, 0, p->nOp * sizeof(i64));
}
#endif				/* SQLITE_ENABLE_STMT_SCANSTATUS */
