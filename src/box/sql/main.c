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
 * Main file for the SQLite library.  The routines in this file
 * implement the programmer interface to the library.  Routines in
 * other files are for internal use by SQLite and should not be
 * accessed by users of the library.
 */
#include "sqliteInt.h"
#include "vdbeInt.h"
#include "version.h"
#include "box/session.h"

#ifdef SQLITE_ENABLE_FTS3
#include "fts3.h"
#endif
#ifdef SQLITE_ENABLE_RTREE
#include "rtree.h"
#endif
#ifdef SQLITE_ENABLE_ICU
#include "sqliteicu.h"
#endif
#ifdef SQLITE_ENABLE_JSON1
int sqlite3Json1Init(sqlite3 *);
#endif
#ifdef SQLITE_ENABLE_FTS5
int sqlite3Fts5Init(sqlite3 *);
#endif

#if !defined(SQLITE_OMIT_TRACE) && defined(SQLITE_ENABLE_IOTRACE)
/*
 * If the following function pointer is not NULL and if
 * SQLITE_ENABLE_IOTRACE is enabled, then messages describing
 * I/O active are written using this function.  These messages
 * are intended for debugging activity only.
 */
SQLITE_API void (SQLITE_CDECL * sqlite3IoTrace) (const char *, ...) = 0;
#endif

/*
 * If the following global variable points to a string which is the
 * name of a directory, then that directory will be used to store
 * temporary files.
 *
 * See also the "PRAGMA temp_store_directory" SQL command.
 */
char *sqlite3_temp_directory = 0;

/*
 * If the following global variable points to a string which is the
 * name of a directory, then that directory will be used to store
 * all database files specified with a relative pathname.
 *
 * See also the "PRAGMA data_store_directory" SQL command.
 */
char *sqlite3_data_directory = 0;

/*
 * Initialize SQLite.
 *
 * This routine must be called to initialize the memory allocation,
 * and VFS subsystems prior to doing any serious work with
 * SQLite.  But as long as you do not compile with SQLITE_OMIT_AUTOINIT
 * this routine will be called automatically by key routines such as
 * sqlite3_open().
 *
 * This routine is a no-op except on its very first call for the process,
 * or for the first call after a call to sqlite3_shutdown.
 *
 * The first thread to call this routine runs the initialization to
 * completion.  If subsequent threads call this routine before the first
 * thread has finished the initialization process, then the subsequent
 * threads must block until the first thread finishes with the initialization.
 *
 * The first thread might call this routine recursively.  Recursive
 * calls to this routine should not block, of course.  Otherwise the
 * initialization process would never complete.
 *
 * Let X be the first thread to enter this routine.  Let Y be some other
 * thread.  Then while the initial invocation of this routine by X is
 * incomplete, it is required that:
 *
 *    *  Calls to this routine from Y must block until the outer-most
 *       call by X completes.
 *
 *    *  Recursive calls to this routine from thread X return immediately
 *       without blocking.
 */
int
sqlite3_initialize(void)
{
	int rc = SQLITE_OK;
#ifdef SQLITE_EXTRA_INIT
	int bRunExtraInit = 0;	/* Extra initialization needed */
#endif

#ifdef SQLITE_OMIT_WSD
	rc = sqlite3_wsd_init(4096, 24);
	if (rc != SQLITE_OK) {
		return rc;
	}
#endif

	/* If the following assert() fails on some obscure processor/compiler
	 * combination, the work-around is to set the correct pointer
	 * size at compile-time using -DSQLITE_PTRSIZE=n compile-time option
	 */
	assert(SQLITE_PTRSIZE == sizeof(char *));

	/* If SQLite is already completely initialized, then this call
	 * to sqlite3_initialize() should be a no-op.  But the initialization
	 * must be complete.  So isInit must not be set until the very end
	 * of this routine.
	 */
	if (sqlite3GlobalConfig.isInit)
		return SQLITE_OK;

	if (!sqlite3GlobalConfig.isMallocInit)
		sqlite3MallocInit();
	if (rc == SQLITE_OK)
		sqlite3GlobalConfig.isMallocInit = 1;

	/* If rc is not SQLITE_OK at this point, then the malloc
	 * subsystem could not be initialized.
	 */
	if (rc != SQLITE_OK)
		return rc;

	/* Do the rest of the initialization
	 * that we will be able to handle recursive calls into
	 * sqlite3_initialize().  The recursive calls normally come through
	 * sqlite3_os_init() when it invokes sqlite3_vfs_register(), but other
	 * recursive calls might also be possible.
	 *
	 * IMPLEMENTATION-OF: R-00140-37445 SQLite automatically serializes calls
	 * to the xInit method, so the xInit method need not be threadsafe.
	 *
         * The sqlite3_pcache_methods.xInit() all is embedded in the
	 * call to sqlite3PcacheInitialize().
	 */
	if (sqlite3GlobalConfig.isInit == 0
	    && sqlite3GlobalConfig.inProgress == 0) {
		sqlite3GlobalConfig.inProgress = 1;
#ifdef SQLITE_ENABLE_SQLLOG
		{
			extern void sqlite3_init_sqllog(void);
			sqlite3_init_sqllog();
		}
#endif
		memset(&sqlite3BuiltinFunctions, 0,
		       sizeof(sqlite3BuiltinFunctions));
		sqlite3RegisterBuiltinFunctions();
		if (rc == SQLITE_OK) {
			rc = sqlite3OsInit();
		}
		if (rc == SQLITE_OK) {
			sqlite3GlobalConfig.isInit = 1;
#ifdef SQLITE_EXTRA_INIT
			bRunExtraInit = 1;
#endif
		}
		sqlite3GlobalConfig.inProgress = 0;
	}

	/* The following is just a sanity check to make sure SQLite has
	 * been compiled correctly.  It is important to run this code, but
	 * we don't want to run it too often and soak up CPU cycles for no
	 * reason.  So we run it once during initialization.
	 */
#ifndef NDEBUG
#ifndef SQLITE_OMIT_FLOATING_POINT
	/* This section of code's only "output" is via assert() statements. */
	if (rc == SQLITE_OK) {
		u64 x = (((u64) 1) << 63) - 1;
		double y;
		assert(sizeof(x) == 8);
		assert(sizeof(x) == sizeof(y));
		memcpy(&y, &x, 8);
		assert(sqlite3IsNaN(y));
	}
#endif
#endif

	/* Do extra initialization steps requested by the SQLITE_EXTRA_INIT
	 * compile-time option.
	 */
#ifdef SQLITE_EXTRA_INIT
	if (bRunExtraInit) {
		int SQLITE_EXTRA_INIT(const char *);
		rc = SQLITE_EXTRA_INIT(0);
	}
#endif

	return rc;
}

/*
 * Undo the effects of sqlite3_initialize().  Must not be called while
 * there are outstanding database connections or memory allocations or
 * while any part of SQLite is otherwise in use in any thread.  This
 * routine is not threadsafe.  But it is safe to invoke this routine
 * on when SQLite is already shut down.  If SQLite is already shut down
 * when this routine is invoked, then this routine is a harmless no-op.
 */
int
sqlite3_shutdown(void)
{
#ifdef SQLITE_OMIT_WSD
	int rc = sqlite3_wsd_init(4096, 24);
	if (rc != SQLITE_OK) {
		return rc;
	}
#endif

	if (sqlite3GlobalConfig.isInit) {
#ifdef SQLITE_EXTRA_SHUTDOWN
		void SQLITE_EXTRA_SHUTDOWN(void);
		SQLITE_EXTRA_SHUTDOWN();
#endif
		sqlite3_os_end();
		sqlite3GlobalConfig.isInit = 0;
	}
	if (sqlite3GlobalConfig.isMallocInit) {
		sqlite3MallocEnd();
		sqlite3GlobalConfig.isMallocInit = 0;

#ifndef SQLITE_OMIT_SHUTDOWN_DIRECTORIES
		/* The heap subsystem has now been shutdown and these values are supposed
		 * to be NULL or point to memory that was obtained from sqlite3_malloc(),
		 * which would rely on that heap subsystem; therefore, make sure these
		 * values cannot refer to heap memory that was just invalidated when the
		 * heap subsystem was shutdown.  This is only done if the current call to
		 * this function resulted in the heap subsystem actually being shutdown.
		 */
		sqlite3_data_directory = 0;
		sqlite3_temp_directory = 0;
#endif
	}

	return SQLITE_OK;
}

/*
 * This API allows applications to modify the global configuration of
 * the SQLite library at run-time.
 *
 * This routine should only be called when there are no outstanding
 * database connections or memory allocations.  This routine is not
 * threadsafe.  Failure to heed these warnings can lead to unpredictable
 * behavior.
 */
int
sqlite3_config(int op, ...)
{
	va_list ap;
	int rc = SQLITE_OK;

	/* sqlite3_config() shall return SQLITE_MISUSE if it is invoked while
	 * the SQLite library is in use.
	 */
	if (sqlite3GlobalConfig.isInit)
		return SQLITE_MISUSE_BKPT;

	va_start(ap, op);
	switch (op) {
	case SQLITE_CONFIG_MEMSTATUS:{
			/* EVIDENCE-OF: R-61275-35157 The SQLITE_CONFIG_MEMSTATUS option takes
			 * single argument of type int, interpreted as a boolean, which enables
			 * or disables the collection of memory allocation statistics.
			 */
			sqlite3GlobalConfig.bMemstat = va_arg(ap, int);
			break;
		}
	case SQLITE_CONFIG_SCRATCH:{
			/* EVIDENCE-OF: R-08404-60887 There are three arguments to
			 * SQLITE_CONFIG_SCRATCH: A pointer an 8-byte aligned memory buffer from
			 * which the scratch allocations will be drawn, the size of each scratch
			 * allocation (sz), and the maximum number of scratch allocations (N).
			 */
			sqlite3GlobalConfig.pScratch = va_arg(ap, void *);
			sqlite3GlobalConfig.szScratch = va_arg(ap, int);
			sqlite3GlobalConfig.nScratch = va_arg(ap, int);
			break;
		}

	case SQLITE_CONFIG_LOOKASIDE:{
			sqlite3GlobalConfig.szLookaside = va_arg(ap, int);
			sqlite3GlobalConfig.nLookaside = va_arg(ap, int);
			break;
		}

		/* Record a pointer to the logger function and its first argument.
		 * The default is NULL.  Logging is disabled if the function pointer is
		 * NULL.
		 */
	case SQLITE_CONFIG_LOG:{
			typedef void (*LOGFUNC_t) (void *, int, const char *);
			sqlite3GlobalConfig.xLog = va_arg(ap, LOGFUNC_t);
			sqlite3GlobalConfig.pLogArg = va_arg(ap, void *);
			break;
		}

		/* EVIDENCE-OF: R-55548-33817 The compile-time setting for URI filenames
		 * can be changed at start-time using the
		 * sqlite3_config(SQLITE_CONFIG_URI,1) or
		 * sqlite3_config(SQLITE_CONFIG_URI,0) configuration calls.
		 */
	case SQLITE_CONFIG_URI:{
			/* EVIDENCE-OF: R-25451-61125 The SQLITE_CONFIG_URI option takes a single
			 * argument of type int. If non-zero, then URI handling is globally
			 * enabled. If the parameter is zero, then URI handling is globally
			 * disabled.
			 */
			sqlite3GlobalConfig.bOpenUri = va_arg(ap, int);
			break;
		}

	case SQLITE_CONFIG_COVERING_INDEX_SCAN:{
			/* EVIDENCE-OF: R-36592-02772 The SQLITE_CONFIG_COVERING_INDEX_SCAN
			 * option takes a single integer argument which is interpreted as a
			 * boolean in order to enable or disable the use of covering indices for
			 * full table scans in the query optimizer.
			 */
			sqlite3GlobalConfig.bUseCis = va_arg(ap, int);
			break;
		}

#ifdef SQLITE_ENABLE_SQLLOG
	case SQLITE_CONFIG_SQLLOG:{
			typedef void (*SQLLOGFUNC_t) (void *, sqlite3 *,
						      const char *, int);
			sqlite3GlobalConfig.xSqllog = va_arg(ap, SQLLOGFUNC_t);
			sqlite3GlobalConfig.pSqllogArg = va_arg(ap, void *);
			break;
		}
#endif

	case SQLITE_CONFIG_MMAP_SIZE:{
			/* EVIDENCE-OF: R-58063-38258 SQLITE_CONFIG_MMAP_SIZE takes two 64-bit
			 * integer (sqlite3_int64) values that are the default mmap size limit
			 * (the default setting for PRAGMA mmap_size) and the maximum allowed
			 * mmap size limit.
			 */
			sqlite3_int64 szMmap = va_arg(ap, sqlite3_int64);
			sqlite3_int64 mxMmap = va_arg(ap, sqlite3_int64);
			/* EVIDENCE-OF: R-53367-43190 If either argument to this option is
			 * negative, then that argument is changed to its compile-time default.
			 *
			 * EVIDENCE-OF: R-34993-45031 The maximum allowed mmap size will be
			 * silently truncated if necessary so that it does not exceed the
			 * compile-time maximum mmap size set by the SQLITE_MAX_MMAP_SIZE
			 * compile-time option.
			 */
			if (mxMmap < 0 || mxMmap > SQLITE_MAX_MMAP_SIZE) {
				mxMmap = SQLITE_MAX_MMAP_SIZE;
			}
			if (szMmap < 0)
				szMmap = SQLITE_DEFAULT_MMAP_SIZE;
			if (szMmap > mxMmap)
				szMmap = mxMmap;
			sqlite3GlobalConfig.mxMmap = mxMmap;
			sqlite3GlobalConfig.szMmap = szMmap;
			break;
		}

	case SQLITE_CONFIG_PMASZ:{
			sqlite3GlobalConfig.szPma = va_arg(ap, unsigned int);
			break;
		}

	case SQLITE_CONFIG_STMTJRNL_SPILL:{
			sqlite3GlobalConfig.nStmtSpill = va_arg(ap, int);
			break;
		}

	default:{
			rc = SQLITE_ERROR;
			break;
		}
	}
	va_end(ap);
	return rc;
}

/*
 * Set up the lookaside buffers for a database connection.
 * Return SQLITE_OK on success.
 * If lookaside is already active, return SQLITE_BUSY.
 *
 * The sz parameter is the number of bytes in each lookaside slot.
 * The cnt parameter is the number of slots.  If pStart is NULL the
 * space for the lookaside memory is obtained from sqlite3_malloc().
 * If pStart is not NULL then it is sz*cnt bytes of memory to use for
 * the lookaside memory.
 */
static int
setupLookaside(sqlite3 * db, void *pBuf, int sz, int cnt)
{
#ifndef SQLITE_OMIT_LOOKASIDE
	void *pStart;
	if (db->lookaside.nOut) {
		return SQLITE_BUSY;
	}
	/* Free any existing lookaside buffer for this handle before
	 * allocating a new one so we don't have to have space for
	 * both at the same time.
	 */
	if (db->lookaside.bMalloced) {
		sqlite3_free(db->lookaside.pStart);
	}
	/* The size of a lookaside slot after ROUNDDOWN8 needs to be larger
	 * than a pointer to be useful.
	 */
	sz = ROUNDDOWN8(sz);	/* IMP: R-33038-09382 */
	if (sz <= (int)sizeof(LookasideSlot *))
		sz = 0;
	if (cnt < 0)
		cnt = 0;
	if (sz == 0 || cnt == 0) {
		sz = 0;
		pStart = 0;
	} else if (pBuf == 0) {
		sqlite3BeginBenignMalloc();
		pStart = sqlite3Malloc(sz * cnt);	/* IMP: R-61949-35727 */
		sqlite3EndBenignMalloc();
		if (pStart)
			cnt = sqlite3MallocSize(pStart) / sz;
	} else {
		pStart = pBuf;
	}
	db->lookaside.pStart = pStart;
	db->lookaside.pFree = 0;
	db->lookaside.sz = (u16) sz;
	if (pStart) {
		int i;
		LookasideSlot *p;
		assert(sz > (int)sizeof(LookasideSlot *));
		p = (LookasideSlot *) pStart;
		for (i = cnt - 1; i >= 0; i--) {
			p->pNext = db->lookaside.pFree;
			db->lookaside.pFree = p;
			p = (LookasideSlot *) & ((u8 *) p)[sz];
		}
		db->lookaside.pEnd = p;
		db->lookaside.bDisable = 0;
		db->lookaside.bMalloced = pBuf == 0 ? 1 : 0;
	} else {
		db->lookaside.pStart = db;
		db->lookaside.pEnd = db;
		db->lookaside.bDisable = 1;
		db->lookaside.bMalloced = 0;
	}
#endif				/* SQLITE_OMIT_LOOKASIDE */
	return SQLITE_OK;
}

void
sql_row_count(struct sqlite3_context *context, MAYBE_UNUSED int unused1,
	      MAYBE_UNUSED sqlite3_value **unused2)
{
	sqlite3 *db = sqlite3_context_db_handle(context);
	sqlite3_result_int(context, db->nChange);
}

/*
 * Close all open savepoints.
 * This procedure is trivial as savepoints are allocated on the "region" and
 * would be destroyed automatically.
 */
void
sqlite3CloseSavepoints(Vdbe * pVdbe)
{
	pVdbe->anonymous_savepoint = NULL;
}

/*
 * Invoke the destructor function associated with FuncDef p, if any. Except,
 * if this is not the last copy of the function, do not invoke it. Multiple
 * copies of a single function are created when create_function() is called
 * with SQLITE_ANY as the encoding.
 */
static void
functionDestroy(sqlite3 * db, FuncDef * p)
{
	FuncDestructor *pDestructor = p->u.pDestructor;
	if (pDestructor) {
		pDestructor->nRef--;
		if (pDestructor->nRef == 0) {
			pDestructor->xDestroy(pDestructor->pUserData);
			sqlite3DbFree(db, pDestructor);
		}
	}
}

/*
 * Return TRUE if database connection db has unfinalized prepared
 * statement.
 */
static int
connectionIsBusy(sqlite3 * db)
{
	if (db->pVdbe)
		return 1;
	return 0;
}

/*
 * Close an existing SQLite database
 */
static int
sqlite3Close(sqlite3 * db, int forceZombie)
{
	assert(db);
	if (!sqlite3SafetyCheckSickOrOk(db)) {
		return SQLITE_MISUSE_BKPT;
	}
	if (db->mTrace & SQLITE_TRACE_CLOSE) {
		db->xTrace(SQLITE_TRACE_CLOSE, db->pTraceArg, db, 0);
	}

	/* Legacy behavior (sqlite3_close() behavior) is to return
	 * SQLITE_BUSY if the connection can not be closed immediately.
	 */
	if (!forceZombie && connectionIsBusy(db)) {
		sqlite3ErrorWithMsg(db, SQLITE_BUSY,
				    "unable to close due to unfinalized "
				    "statements");
		return SQLITE_BUSY;
	}
#ifdef SQLITE_ENABLE_SQLLOG
	if (sqlite3GlobalConfig.xSqllog) {
		/* Closing the handle. Fourth parameter is passed the value 2. */
		sqlite3GlobalConfig.xSqllog(sqlite3GlobalConfig.pSqllogArg, db,
					    0, 2);
	}
#endif

	/* Convert the connection into a zombie and then close it.
	 */
	db->magic = SQLITE_MAGIC_ZOMBIE;

	return SQLITE_OK;
}

/*
 * Two variations on the public interface for closing a database
 * connection. The sqlite3_close() version returns SQLITE_BUSY and
 * leaves the connection option if there are unfinalized prepared
 * statements.
 */
int
sqlite3_close(sqlite3 * db)
{
	return sqlite3Close(db, 0);
}

/*
 * Rollback all database files.  If tripCode is not SQLITE_OK, then
 * any write cursors are invalidated ("tripped" - as in "tripping a circuit
 * breaker") and made to return tripCode if there are any further
 * attempts to use that cursor.  Read cursors remain open and valid
 * but are "saved" in case the table pages are moved around.
 */
void
sqlite3RollbackAll(Vdbe * pVdbe)
{
	sqlite3 *db = pVdbe->db;

	/* If one has been configured, invoke the rollback-hook callback */
	if (db->xRollbackCallback && (!pVdbe->auto_commit)) {
		db->xRollbackCallback(db->pRollbackArg);
	}
}

/*
 * Return a static string that describes the kind of error specified in the
 * argument.
 */
const char *
sqlite3ErrStr(int rc)
{
	static const char *const aMsg[] = {
		/* SQLITE_OK          */ "not an error",
		/* SQLITE_ERROR       */ "SQL logic error or missing database",
		/* SQLITE_PERM        */ "access permission denied",
		/* SQLITE_ABORT       */ "callback requested query abort",
		/* SQLITE_BUSY        */ "database is locked",
		/* SQLITE_LOCKED      */ "database table is locked",
		/* SQLITE_NOMEM       */ "out of memory",
		/* SQLITE_INTERRUPT   */ "interrupted",
		/* SQLITE_IOERR       */ "disk I/O error",
		/* SQLITE_NOTFOUND    */ "unknown operation",
		/* SQLITE_FULL        */ "database or disk is full",
		/* SQLITE_CANTOPEN    */ "unable to open database file",
		/* SQLITE_SCHEMA      */ "database schema has changed",
		/* SQLITE_TOOBIG      */ "string or blob too big",
		/* SQLITE_CONSTRAINT  */ "constraint failed",
		/* SQLITE_MISMATCH    */ "datatype mismatch",
		/* SQLITE_MISUSE      */
		    "library routine called out of sequence",
		/* SQLITE_RANGE       */ "bind or column index out of range",
		/* SQL_TARANTOOL_ITERATOR_FAIL */ "Tarantool's iterator failed",
		/* SQL_TARANTOOL_INSERT_FAIL */ "Tarantool's insert failed",
		/* SQL_TARANTOOL_DELETE_FAIL */ "Tarantool's delete failed",
		/* SQL_TARANTOOL_ERROR */ "SQL-/Tarantool error",
	};
	const char *zErr = "unknown error";
	rc &= 0xff;
	if (ALWAYS(rc >= 0) && rc < ArraySize(aMsg) && aMsg[rc] != 0)
		zErr = aMsg[rc];
	return zErr;
}

/*
 * This routine implements a busy callback that sleeps and tries
 * again until a timeout value is reached.  The timeout value is
 * an integer number of milliseconds passed in as the first
 * argument.
 */
static int
sqliteDefaultBusyCallback(void *ptr,	/* Database connection */
			  int count)	/* Number of times table has been busy */
{
	sqlite3 *db = (sqlite3 *) ptr;
	int timeout = ((sqlite3 *) ptr)->busyTimeout;
	if ((count + 1) * 1000 > timeout) {
		return 0;
	}
	sqlite3OsSleep(db->pVfs, 1000000);
	return 1;
}

/*
 * This routine sets the busy callback for an Sqlite database to the
 * given callback function with the given argument.
 */
int
sqlite3_busy_handler(sqlite3 * db, int (*xBusy) (void *, int), void *pArg)
{
#ifdef SQLITE_ENABLE_API_ARMOR
	if (!sqlite3SafetyCheckOk(db))
		return SQLITE_MISUSE_BKPT;
#endif
	db->busyHandler.xFunc = xBusy;
	db->busyHandler.pArg = pArg;
	db->busyHandler.nBusy = 0;
	db->busyTimeout = 0;
	return SQLITE_OK;
}

#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
/*
 * This routine sets the progress callback for an Sqlite database to the
 * given callback function with the given argument. The progress callback will
 * be invoked every nOps opcodes.
 */
void
sqlite3_progress_handler(sqlite3 * db,
			 int nOps, int (*xProgress) (void *), void *pArg)
{
#ifdef SQLITE_ENABLE_API_ARMOR
	if (!sqlite3SafetyCheckOk(db)) {
		(void)SQLITE_MISUSE_BKPT;
		return;
	}
#endif
	if (nOps > 0) {
		db->xProgress = xProgress;
		db->nProgressOps = (unsigned)nOps;
		db->pProgressArg = pArg;
	} else {
		db->xProgress = 0;
		db->nProgressOps = 0;
		db->pProgressArg = 0;
	}
}
#endif

/*
 * This routine installs a default busy handler that waits for the
 * specified number of milliseconds before returning 0.
 */
int
sqlite3_busy_timeout(sqlite3 * db, int ms)
{
#ifdef SQLITE_ENABLE_API_ARMOR
	if (!sqlite3SafetyCheckOk(db))
		return SQLITE_MISUSE_BKPT;
#endif
	if (ms > 0) {
		sqlite3_busy_handler(db, sqliteDefaultBusyCallback, (void *)db);
		db->busyTimeout = ms;
	} else {
		sqlite3_busy_handler(db, 0, 0);
	}
	return SQLITE_OK;
}

/*
 * Cause any pending operation to stop at its earliest opportunity.
 */
void
sqlite3_interrupt(sqlite3 * db)
{
#ifdef SQLITE_ENABLE_API_ARMOR
	if (!sqlite3SafetyCheckOk(db)
	    && (db == 0 || db->magic != SQLITE_MAGIC_ZOMBIE)) {
		(void)SQLITE_MISUSE_BKPT;
		return;
	}
#endif
	db->u1.isInterrupted = 1;
}

/*
 * This function is exactly the same as sqlite3_create_function(), except
 * that it is designed to be called by internal code. The difference is
 * that if a malloc() fails in sqlite3_create_function(), an error code
 * is returned and the mallocFailed flag cleared.
 */
int
sqlite3CreateFunc(sqlite3 * db,
		  const char *zFunctionName,
		  enum field_type type,
		  int nArg,
		  int flags,
		  void *pUserData,
		  void (*xSFunc) (sqlite3_context *, int, sqlite3_value **),
		  void (*xStep) (sqlite3_context *, int, sqlite3_value **),
		  void (*xFinal) (sqlite3_context *),
		  FuncDestructor * pDestructor)
{
	FuncDef *p;
	int extraFlags;

	if (zFunctionName == 0 ||
	    (xSFunc && (xFinal || xStep)) ||
	    (!xSFunc && (xFinal && !xStep)) ||
	    (!xSFunc && (!xFinal && xStep)) ||
	    (nArg < -1 || nArg > SQLITE_MAX_FUNCTION_ARG) ||
	    (255 < (sqlite3Strlen30(zFunctionName)))) {
		return SQLITE_MISUSE_BKPT;
	}

	assert(SQLITE_FUNC_CONSTANT == SQLITE_DETERMINISTIC);
	extraFlags = flags & SQLITE_DETERMINISTIC;


	/* Check if an existing function is being overridden or deleted. If so,
	 * and there are active VMs, then return SQLITE_BUSY. If a function
	 * is being overridden/deleted but there are no active VMs, allow the
	 * operation to continue but invalidate all precompiled statements.
	 */
	p = sqlite3FindFunction(db, zFunctionName, nArg, 0);
	if (p && p->nArg == nArg) {
		if (db->nVdbeActive) {
			sqlite3ErrorWithMsg(db, SQLITE_BUSY,
					    "unable to delete/modify user-function due to active statements");
			assert(!db->mallocFailed);
			return SQLITE_BUSY;
		} else {
			sqlite3ExpirePreparedStatements(db);
		}
	}

	p = sqlite3FindFunction(db, zFunctionName, nArg, 1);
	assert(p || db->mallocFailed);
	if (!p) {
		return SQLITE_NOMEM_BKPT;
	}

	/* If an older version of the function with a configured destructor is
	 * being replaced invoke the destructor function here.
	 */
	functionDestroy(db, p);

	if (pDestructor) {
		pDestructor->nRef++;
	}
	p->u.pDestructor = pDestructor;
	p->funcFlags = extraFlags;
	testcase(p->funcFlags & SQLITE_DETERMINISTIC);
	p->xSFunc = xSFunc ? xSFunc : xStep;
	p->xFinalize = xFinal;
	p->pUserData = pUserData;
	p->nArg = (u16) nArg;
	p->ret_type = type;
	return SQLITE_OK;
}

int
sqlite3_create_function_v2(sqlite3 * db,
			   const char *zFunc,
			   enum field_type type,
			   int nArg,
			   int flags,
			   void *p,
			   void (*xSFunc) (sqlite3_context *, int,
					   sqlite3_value **),
			   void (*xStep) (sqlite3_context *, int,
					  sqlite3_value **),
			   void (*xFinal) (sqlite3_context *),
			   void (*xDestroy) (void *))
{
	int rc = SQLITE_ERROR;
	FuncDestructor *pArg = 0;

#ifdef SQLITE_ENABLE_API_ARMOR
	if (!sqlite3SafetyCheckOk(db)) {
		return SQLITE_MISUSE_BKPT;
	}
#endif
	if (xDestroy) {
		pArg =
		    (FuncDestructor *) sqlite3DbMallocZero(db,
							   sizeof
							   (FuncDestructor));
		if (!pArg) {
			xDestroy(p);
			goto out;
		}
		pArg->xDestroy = xDestroy;
		pArg->pUserData = p;
	}
	rc = sqlite3CreateFunc(db, zFunc, type, nArg, flags, p, xSFunc, xStep,
			       xFinal, pArg);
	if (pArg && pArg->nRef == 0) {
		assert(rc != SQLITE_OK);
		xDestroy(p);
		sqlite3DbFree(db, pArg);
	}

 out:
	rc = sqlite3ApiExit(db, rc);
	return rc;
}

#ifndef SQLITE_OMIT_TRACE
/* Register a trace callback using the version-2 interface.
 */
int
sqlite3_trace_v2(sqlite3 * db,		/* Trace this connection */
		 unsigned mTrace,	/* Mask of events to be traced */
		 int (*xTrace) (unsigned, void *, void *, void *),	/* Callback to invoke */
		 void *pArg)		/* Context */
{
#ifdef SQLITE_ENABLE_API_ARMOR
	if (!sqlite3SafetyCheckOk(db)) {
		return SQLITE_MISUSE_BKPT;
	}
#endif
	if (mTrace == 0)
		xTrace = 0;
	if (xTrace == 0)
		mTrace = 0;
	db->mTrace = mTrace;
	db->xTrace = xTrace;
	db->pTraceArg = pArg;
	return SQLITE_OK;
}

#endif				/* SQLITE_OMIT_TRACE */

/*
 * Register a function to be invoked when a transaction commits.
 * If the invoked function returns non-zero, then the commit becomes a
 * rollback.
 */
void *
sqlite3_commit_hook(sqlite3 * db,	/* Attach the hook to this database */
		    int (*xCallback) (void *),	/* Function to invoke on each commit */
		    void *pArg)		/* Argument to the function */
{
	void *pOld;

#ifdef SQLITE_ENABLE_API_ARMOR
	if (!sqlite3SafetyCheckOk(db)) {
		(void)SQLITE_MISUSE_BKPT;
		return 0;
	}
#endif
	pOld = db->pCommitArg;
	db->xCommitCallback = xCallback;
	db->pCommitArg = pArg;
	return pOld;
}

/*
 * Register a callback to be invoked each time a row is updated,
 * inserted or deleted using this database connection.
 */
void *
sqlite3_update_hook(sqlite3 * db,	/* Attach the hook to this database */
		    void (*xCallback) (void *, int, char const *,
				       char const *, sqlite_int64),
		    void *pArg)		/* Argument to the function */
{
	void *pRet;

#ifdef SQLITE_ENABLE_API_ARMOR
	if (!sqlite3SafetyCheckOk(db)) {
		(void)SQLITE_MISUSE_BKPT;
		return 0;
	}
#endif
	pRet = db->pUpdateArg;
	db->xUpdateCallback = xCallback;
	db->pUpdateArg = pArg;
	return pRet;
}

/*
 * Register a callback to be invoked each time a transaction is rolled
 * back by this database connection.
 */
void *
sqlite3_rollback_hook(sqlite3 * db,	/* Attach the hook to this database */
		      void (*xCallback) (void *),	/* Callback function */
		      void *pArg)	/* Argument to the function */
{
	void *pRet;

#ifdef SQLITE_ENABLE_API_ARMOR
	if (!sqlite3SafetyCheckOk(db)) {
		(void)SQLITE_MISUSE_BKPT;
		return 0;
	}
#endif
	pRet = db->pRollbackArg;
	db->xRollbackCallback = xCallback;
	db->pRollbackArg = pArg;
	return pRet;
}

/*
 * Configure an sqlite3_wal_hook() callback to automatically checkpoint
 * a database after committing a transaction if there are nFrame or
 * more frames in the log file. Passing zero or a negative value as the
 * nFrame parameter disables automatic checkpoints entirely.
 *
 * The callback registered by this function replaces any existing callback
 * registered using sqlite3_wal_hook(). Likewise, registering a callback
 * using sqlite3_wal_hook() disables the automatic checkpoint mechanism
 * configured by this function.
 */
int
sqlite3_wal_autocheckpoint(sqlite3 * db, int nFrame)
{
	UNUSED_PARAMETER(db);
	UNUSED_PARAMETER(nFrame);
	return SQLITE_OK;
}

/*
 * This function returns true if main-memory should be used instead of
 * a temporary file for transient pager files and statement journals.
 * The value returned depends on the value of db->temp_store (runtime
 * parameter) and the compile time value of SQLITE_TEMP_STORE. The
 * following table describes the relationship between these two values
 * and this functions return value.
 *
 *   SQLITE_TEMP_STORE     db->temp_store     Location of temporary database
 *   -----------------     --------------     ------------------------------
 *   0                     any                file      (return 0)
 *   1                     1                  file      (return 0)
 *   1                     2                  memory    (return 1)
 *   1                     0                  file      (return 0)
 *   2                     1                  file      (return 0)
 *   2                     2                  memory    (return 1)
 *   2                     0                  memory    (return 1)
 *   3                     any                memory    (return 1)
 */
int
sqlite3TempInMemory(const sqlite3 * db)
{
#if SQLITE_TEMP_STORE==1
	return (db->temp_store == 2);
#endif
#if SQLITE_TEMP_STORE==2
	return (db->temp_store != 1);
#endif
#if SQLITE_TEMP_STORE==3
	UNUSED_PARAMETER(db);
	return 1;
#endif
#if SQLITE_TEMP_STORE<1 || SQLITE_TEMP_STORE>3
	UNUSED_PARAMETER(db);
	return 0;
#endif
}

/*
 * Return UTF-8 encoded English language explanation of the most recent
 * error.
 */
const char *
sqlite3_errmsg(sqlite3 * db)
{
	const char *z;
	if (!db) {
		return sqlite3ErrStr(SQLITE_NOMEM_BKPT);
	}
	if (!sqlite3SafetyCheckSickOrOk(db)) {
		return sqlite3ErrStr(SQLITE_MISUSE_BKPT);
	}
	if (db->mallocFailed) {
		z = sqlite3ErrStr(SQLITE_NOMEM_BKPT);
	} else {
		testcase(db->pErr == 0);
		assert(!db->mallocFailed);
		if (db->errCode != SQL_TARANTOOL_ERROR) {
			z = (char *)sqlite3_value_text(db->pErr);
			if (z == NULL)
				z = sqlite3ErrStr(db->errCode);
		} else {
			z = diag_last_error(diag_get())->errmsg;
		}
		assert(z != NULL);
	}
	return z;
}

/*
 * Return the most recent error code generated by an SQLite routine. If NULL is
 * passed to this function, we assume a malloc() failed during sqlite3_open().
 */
int
sqlite3_errcode(sqlite3 * db)
{
	if (db && !sqlite3SafetyCheckSickOrOk(db)) {
		return SQLITE_MISUSE_BKPT;
	}
	if (!db || db->mallocFailed) {
		return SQLITE_NOMEM_BKPT;
	}
	return db->errCode & db->errMask;
}

int
sqlite3_extended_errcode(sqlite3 * db)
{
	if (db && !sqlite3SafetyCheckSickOrOk(db)) {
		return SQLITE_MISUSE_BKPT;
	}
	if (!db || db->mallocFailed) {
		return SQLITE_NOMEM_BKPT;
	}
	return db->errCode;
}

int
sqlite3_system_errno(sqlite3 * db)
{
	return db ? db->iSysErrno : 0;
}

/*
 * Return a string that describes the kind of error specified in the
 * argument.  For now, this simply calls the internal sqlite3ErrStr()
 * function.
 */
const char *
sqlite3_errstr(int rc)
{
	return sqlite3ErrStr(rc);
}

/*
 * This array defines hard upper bounds on limit values.  The
 * initializer must be kept in sync with the SQLITE_LIMIT_*
 * #defines in sqlite3.h.
 */
static const int aHardLimit[] = {
	SQLITE_MAX_LENGTH,
	SQLITE_MAX_SQL_LENGTH,
	SQLITE_MAX_COLUMN,
	SQLITE_MAX_EXPR_DEPTH,
	SQL_MAX_COMPOUND_SELECT,
	SQLITE_MAX_VDBE_OP,
	SQLITE_MAX_FUNCTION_ARG,
	SQLITE_MAX_ATTACHED,
	SQLITE_MAX_LIKE_PATTERN_LENGTH,
	SQLITE_MAX_TRIGGER_DEPTH,
	SQLITE_MAX_WORKER_THREADS,
};

/*
 * Make sure the hard limits are set to reasonable values
 */
#if SQLITE_MAX_LENGTH<100
#error SQLITE_MAX_LENGTH must be at least 100
#endif
#if SQLITE_MAX_SQL_LENGTH<100
#error SQLITE_MAX_SQL_LENGTH must be at least 100
#endif
#if SQLITE_MAX_SQL_LENGTH>SQLITE_MAX_LENGTH
#error SQLITE_MAX_SQL_LENGTH must not be greater than SQLITE_MAX_LENGTH
#endif
#if SQL_MAX_COMPOUND_SELECT<2
#error SQL_MAX_COMPOUND_SELECT must be at least 2
#endif
#if SQLITE_MAX_VDBE_OP<40
#error SQLITE_MAX_VDBE_OP must be at least 40
#endif
#if SQLITE_MAX_FUNCTION_ARG<0 || SQLITE_MAX_FUNCTION_ARG>127
#error SQLITE_MAX_FUNCTION_ARG must be between 0 and 127
#endif
#if SQLITE_MAX_ATTACHED<0 || SQLITE_MAX_ATTACHED>125
#error SQLITE_MAX_ATTACHED must be between 0 and 125
#endif
#if SQLITE_MAX_LIKE_PATTERN_LENGTH<1
#error SQLITE_MAX_LIKE_PATTERN_LENGTH must be at least 1
#endif
#if SQLITE_MAX_COLUMN>32767
#error SQLITE_MAX_COLUMN must not exceed 32767
#endif
#if SQLITE_MAX_TRIGGER_DEPTH<1
#error SQLITE_MAX_TRIGGER_DEPTH must be at least 1
#endif
#if SQLITE_MAX_WORKER_THREADS<0 || SQLITE_MAX_WORKER_THREADS>50
#error SQLITE_MAX_WORKER_THREADS must be between 0 and 50
#endif

/*
 * Change the value of a limit.  Report the old value.
 * If an invalid limit index is supplied, report -1.
 * Make no changes but still report the old value if the
 * new limit is negative.
 *
 * A new lower limit does not shrink existing constructs.
 * It merely prevents new constructs that exceed the limit
 * from forming.
 */
int
sqlite3_limit(sqlite3 * db, int limitId, int newLimit)
{
	int oldLimit;

#ifdef SQLITE_ENABLE_API_ARMOR
	if (!sqlite3SafetyCheckOk(db)) {
		(void)SQLITE_MISUSE_BKPT;
		return -1;
	}
#endif

	/* EVIDENCE-OF: R-30189-54097 For each limit category SQLITE_LIMIT_NAME
	 * there is a hard upper bound set at compile-time by a C preprocessor
	 * macro called SQLITE_MAX_NAME. (The "_LIMIT_" in the name is changed to
	 * "_MAX_".)
	 */
	assert(aHardLimit[SQLITE_LIMIT_LENGTH] == SQLITE_MAX_LENGTH);
	assert(aHardLimit[SQLITE_LIMIT_SQL_LENGTH] == SQLITE_MAX_SQL_LENGTH);
	assert(aHardLimit[SQLITE_LIMIT_COLUMN] == SQLITE_MAX_COLUMN);
	assert(aHardLimit[SQLITE_LIMIT_EXPR_DEPTH] == SQLITE_MAX_EXPR_DEPTH);
	assert(aHardLimit[SQL_LIMIT_COMPOUND_SELECT] ==
	       SQL_MAX_COMPOUND_SELECT);
	assert(aHardLimit[SQLITE_LIMIT_VDBE_OP] == SQLITE_MAX_VDBE_OP);
	assert(aHardLimit[SQLITE_LIMIT_FUNCTION_ARG] ==
	       SQLITE_MAX_FUNCTION_ARG);
	assert(aHardLimit[SQLITE_LIMIT_ATTACHED] == SQLITE_MAX_ATTACHED);
	assert(aHardLimit[SQLITE_LIMIT_LIKE_PATTERN_LENGTH] ==
	       SQLITE_MAX_LIKE_PATTERN_LENGTH);
	assert(aHardLimit[SQLITE_LIMIT_TRIGGER_DEPTH] ==
	       SQLITE_MAX_TRIGGER_DEPTH);
	assert(aHardLimit[SQLITE_LIMIT_WORKER_THREADS] ==
	       SQLITE_MAX_WORKER_THREADS);
	assert(SQLITE_LIMIT_WORKER_THREADS == (SQLITE_N_LIMIT - 1));

	if (limitId < 0 || limitId >= SQLITE_N_LIMIT) {
		return -1;
	}
	oldLimit = db->aLimit[limitId];
	if (newLimit >= 0) {	/* IMP: R-52476-28732 */
		if (newLimit > aHardLimit[limitId]) {
			newLimit = aHardLimit[limitId];	/* IMP: R-51463-25634 */
		}
		db->aLimit[limitId] = newLimit;
	}
	return oldLimit;	/* IMP: R-53341-35419 */
}

/*
 * This function is used to parse both URIs and non-URI filenames passed by the
 * user to API functions sqlite3_open() or sqlite3_open_v2(), and for database
 * URIs specified as part of ATTACH statements.
 *
 * The first argument to this function is the name of the VFS to use (or
 * a NULL to signify the default VFS) if the URI does not contain a "vfs=xxx"
 * query parameter. The second argument contains the URI (or non-URI filename)
 * itself. When this function is called the *pFlags variable should contain
 * the default flags to open the database handle with. The value stored in
 * *pFlags may be updated before returning if the URI filename contains
 * "cache=xxx" or "mode=xxx" query parameters.
 *
 * If successful, SQLITE_OK is returned. In this case *ppVfs is set to point to
 * the VFS that should be used to open the database file. *pzFile is set to
 * point to a buffer containing the name of the file to open. It is the
 * responsibility of the caller to eventually call sqlite3_free() to release
 * this buffer.
 *
 * If an error occurs, then an SQLite error code is returned and *pzErrMsg
 * may be set to point to a buffer containing an English language error
 * message. It is the responsibility of the caller to eventually release
 * this buffer by calling sqlite3_free().
 */
int
sqlite3ParseUri(const char *zDefaultVfs,	/* VFS to use if no "vfs=xxx" query option */
		const char *zUri,		/* Nul-terminated URI to parse */
		unsigned int *pFlags,		/* IN/OUT: SQLITE_OPEN_XXX flags */
		sqlite3_vfs ** ppVfs,		/* OUT: VFS to use */
		char **pzFile,			/* OUT: Filename component of URI */
		char **pzErrMsg)		/* OUT: Error message (if rc!=SQLITE_OK) */
{
	int rc = SQLITE_OK;
	unsigned int flags = *pFlags;
	const char *zVfs = zDefaultVfs;
	char *zFile;
	char c;
	int nUri = sqlite3Strlen30(zUri);

	assert(*pzErrMsg == 0);

	if (((flags & SQLITE_OPEN_URI)	/* IMP: R-48725-32206 */
	     ||sqlite3GlobalConfig.bOpenUri)	/* IMP: R-51689-46548 */
	    &&nUri >= 5 && memcmp(zUri, "file:", 5) == 0	/* IMP: R-57884-37496 */
	    ) {
		char *zOpt;
		int eState;	/* Parser state when parsing URI */
		int iIn;	/* Input character index */
		int iOut = 0;	/* Output character index */
		u64 nByte = nUri + 2;	/* Bytes of space to allocate */

		/* Make sure the SQLITE_OPEN_URI flag is set to indicate to the VFS xOpen
		 * method that there may be extra parameters following the file-name.
		 */
		flags |= SQLITE_OPEN_URI;

		for (iIn = 0; iIn < nUri; iIn++)
			nByte += (zUri[iIn] == '&');
		zFile = sqlite3_malloc64(nByte);
		if (!zFile)
			return SQLITE_NOMEM_BKPT;

		iIn = 5;
#ifdef SQLITE_ALLOW_URI_AUTHORITY
		if (strncmp(zUri + 5, "///", 3) == 0) {
			iIn = 7;
			/* The following condition causes URIs with five leading / characters
			 * like file://///host/path to be converted into UNCs like //host/path.
			 * The correct URI for that UNC has only two or four leading / characters
			 * file://host/path or file:////host/path.  But 5 leading slashes is a
			 * common error, we are told, so we handle it as a special case.
			 */
			if (strncmp(zUri + 7, "///", 3) == 0) {
				iIn++;
			}
		} else if (strncmp(zUri + 5, "//localhost/", 12) == 0) {
			iIn = 16;
		}
#else
		/* Discard the scheme and authority segments of the URI. */
		if (zUri[5] == '/' && zUri[6] == '/') {
			iIn = 7;
			while (zUri[iIn] && zUri[iIn] != '/')
				iIn++;
			if (iIn != 7
			    && (iIn != 16
				|| memcmp("localhost", &zUri[7], 9))) {
				*pzErrMsg =
				    sqlite3_mprintf
				    ("invalid uri authority: %.*s", iIn - 7,
				     &zUri[7]);
				rc = SQLITE_ERROR;
				goto parse_uri_out;
			}
		}
#endif

		/* Copy the filename and any query parameters into the zFile buffer.
		 * Decode %HH escape codes along the way.
		 *
		 * Within this loop, variable eState may be set to 0, 1 or 2, depending
		 * on the parsing context. As follows:
		 *
		 *   0: Parsing file-name.
		 *   1: Parsing name section of a name=value query parameter.
		 *   2: Parsing value section of a name=value query parameter.
		 */
		eState = 0;
		while ((c = zUri[iIn]) != 0 && c != '#') {
			iIn++;
			if (c == '%' && sqlite3Isxdigit(zUri[iIn])
			    && sqlite3Isxdigit(zUri[iIn + 1])
			    ) {
				int octet = (sqlite3HexToInt(zUri[iIn++]) << 4);
				octet += sqlite3HexToInt(zUri[iIn++]);

				assert(octet >= 0 && octet < 256);
				if (octet == 0) {
#ifndef SQLITE_ENABLE_URI_00_ERROR
					/* This branch is taken when "%00" appears within the URI. In this
					 * case we ignore all text in the remainder of the path, name or
					 * value currently being parsed. So ignore the current character
					 * and skip to the next "?", "=" or "&", as appropriate.
					 */
					while ((c = zUri[iIn]) != 0 && c != '#'
					       && (eState != 0 || c != '?')
					       && (eState != 1
						   || (c != '=' && c != '&'))
					       && (eState != 2 || c != '&')
					    ) {
						iIn++;
					}
					continue;
#else
					/* If ENABLE_URI_00_ERROR is defined, "%00" in a URI is an error. */
					*pzErrMsg =
					    sqlite3_mprintf
					    ("unexpected %%00 in uri");
					rc = SQLITE_ERROR;
					goto parse_uri_out;
#endif
				}
				c = octet;
			} else if (eState == 1 && (c == '&' || c == '=')) {
				if (zFile[iOut - 1] == 0) {
					/* An empty option name. Ignore this option altogether. */
					while (zUri[iIn] && zUri[iIn] != '#'
					       && zUri[iIn - 1] != '&')
						iIn++;
					continue;
				}
				if (c == '&') {
					zFile[iOut++] = '\0';
				} else {
					eState = 2;
				}
				c = 0;
			} else if ((eState == 0 && c == '?')
				   || (eState == 2 && c == '&')) {
				c = 0;
				eState = 1;
			}
			zFile[iOut++] = c;
		}
		if (eState == 1)
			zFile[iOut++] = '\0';
		zFile[iOut++] = '\0';
		zFile[iOut++] = '\0';

		/* Check if there were any options specified that should be interpreted
		 * here. Options that are interpreted here include "vfs" and those that
		 * correspond to flags that may be passed to the sqlite3_open_v2()
		 * method.
		 */
		zOpt = &zFile[sqlite3Strlen30(zFile) + 1];
		while (zOpt[0]) {
			int nOpt = sqlite3Strlen30(zOpt);
			char *zVal = &zOpt[nOpt + 1];
			unsigned int nVal = sqlite3Strlen30(zVal);

			if (nOpt == 3 && memcmp("vfs", zOpt, 3) == 0) {
				zVfs = zVal;
			} else {
				struct OpenMode {
					const char *z;
					int mode;
				} *aMode = 0;
				char *zModeType = 0;
				int mask = 0;
				int limit = 0;

				if (nOpt == 5 && memcmp("cache", zOpt, 5) == 0) {
					static struct OpenMode aCacheMode[] = {
						{"shared",
						 SQLITE_OPEN_SHAREDCACHE},
						{"private",
						 SQLITE_OPEN_PRIVATECACHE},
						{0, 0}
					};

					mask =
					    SQLITE_OPEN_SHAREDCACHE |
					    SQLITE_OPEN_PRIVATECACHE;
					aMode = aCacheMode;
					limit = mask;
					zModeType = "cache";
				}
				if (nOpt == 4 && memcmp("mode", zOpt, 4) == 0) {
					static struct OpenMode aOpenMode[] = {
						{"ro", SQLITE_OPEN_READONLY},
						{"rw", SQLITE_OPEN_READWRITE},
						{"rwc",
						 SQLITE_OPEN_READWRITE |
						 SQLITE_OPEN_CREATE},
						{"memory", SQLITE_OPEN_MEMORY},
						{0, 0}
					};

					mask =
					    SQLITE_OPEN_READONLY |
					    SQLITE_OPEN_READWRITE |
					    SQLITE_OPEN_CREATE |
					    SQLITE_OPEN_MEMORY;
					aMode = aOpenMode;
					limit = mask & flags;
					zModeType = "access";
				}

				if (aMode) {
					int i;
					int mode = 0;
					for (i = 0; aMode[i].z; i++) {
						const char *z = aMode[i].z;
						if (nVal == sqlite3Strlen30(z)
						    && 0 == memcmp(zVal, z,
								   nVal)) {
							mode = aMode[i].mode;
							break;
						}
					}
					if (mode == 0) {
						*pzErrMsg =
						    sqlite3_mprintf
						    ("no such %s mode: %s",
						     zModeType, zVal);
						rc = SQLITE_ERROR;
						goto parse_uri_out;
					}
					if ((mode & ~SQLITE_OPEN_MEMORY) >
					    limit) {
						*pzErrMsg =
						    sqlite3_mprintf
						    ("%s mode not allowed: %s",
						     zModeType, zVal);
						rc = SQLITE_PERM;
						goto parse_uri_out;
					}
					flags = (flags & ~mask) | mode;
				}
			}

			zOpt = &zVal[nVal + 1];
		}

	} else {
		zFile = sqlite3_malloc64(nUri + 2);
		if (!zFile)
			return SQLITE_NOMEM_BKPT;
		if (nUri) {
			memcpy(zFile, zUri, nUri);
		}
		zFile[nUri] = '\0';
		zFile[nUri + 1] = '\0';
		flags &= ~SQLITE_OPEN_URI;
	}

	*ppVfs = sqlite3_vfs_find(zVfs);
	if (*ppVfs == 0) {
		*pzErrMsg = sqlite3_mprintf("no such vfs: %s", zVfs);
		rc = SQLITE_ERROR;
	}
 parse_uri_out:
	if (rc != SQLITE_OK) {
		sqlite3_free(zFile);
		zFile = 0;
	}
	*pFlags = flags;
	*pzFile = zFile;
	return rc;
}

/**
 * This routine does the work of initialization of main
 * SQL connection instance.
 *
 * @param[out] out_db returned database handle.
 * @return error status code.
 */
int
sql_init_db(sqlite3 **out_db)
{
	sqlite3 *db;
	int rc;			/* Return code */

#ifdef SQLITE_ENABLE_API_ARMOR
	if (ppDb == 0)
		return SQLITE_MISUSE_BKPT;
#endif
#ifndef SQLITE_OMIT_AUTOINIT
	rc = sqlite3_initialize();
	if (rc)
		return rc;
#endif

	/* Allocate the sqlite data structure */
	db = sqlite3MallocZero(sizeof(sqlite3));
	if (db == 0)
		goto opendb_out;
	db->errMask = 0xff;
	db->magic = SQLITE_MAGIC_BUSY;

	db->pVfs = sqlite3_vfs_find(0);

	assert(sizeof(db->aLimit) == sizeof(aHardLimit));
	memcpy(db->aLimit, aHardLimit, sizeof(db->aLimit));
	db->aLimit[SQLITE_LIMIT_WORKER_THREADS] = SQLITE_DEFAULT_WORKER_THREADS;
	db->aLimit[SQL_LIMIT_COMPOUND_SELECT] = SQL_DEFAULT_COMPOUND_SELECT;
	db->szMmap = sqlite3GlobalConfig.szMmap;
	db->nMaxSorterMmap = 0x7FFFFFFF;

	db->magic = SQLITE_MAGIC_OPEN;
	if (db->mallocFailed) {
		goto opendb_out;
	}

	/* Register all built-in functions, but do not attempt to read the
	 * database schema yet. This is delayed until the first time the database
	 * is accessed.
	 */
	sqlite3Error(db, SQLITE_OK);
	sqlite3RegisterPerConnectionBuiltinFunctions(db);
	rc = sqlite3_errcode(db);

#ifdef SQLITE_ENABLE_FTS5
	/* Register any built-in FTS5 module before loading the automatic
	 * extensions. This allows automatic extensions to register FTS5
	 * tokenizers and auxiliary functions.
	 */
	if (!db->mallocFailed && rc == SQLITE_OK) {
		rc = sqlite3Fts5Init(db);
	}
#endif

#ifdef SQLITE_ENABLE_FTS1
	if (!db->mallocFailed) {
		extern int sqlite3Fts1Init(sqlite3 *);
		rc = sqlite3Fts1Init(db);
	}
#endif

#ifdef SQLITE_ENABLE_FTS2
	if (!db->mallocFailed && rc == SQLITE_OK) {
		extern int sqlite3Fts2Init(sqlite3 *);
		rc = sqlite3Fts2Init(db);
	}
#endif

#ifdef SQLITE_ENABLE_FTS3	/* automatically defined by SQLITE_ENABLE_FTS4 */
	if (!db->mallocFailed && rc == SQLITE_OK) {
		rc = sqlite3Fts3Init(db);
	}
#endif

#ifdef SQLITE_ENABLE_ICU
	if (!db->mallocFailed && rc == SQLITE_OK) {
		rc = sqlite3IcuInit(db);
	}
#endif

#ifdef SQLITE_ENABLE_RTREE
	if (!db->mallocFailed && rc == SQLITE_OK) {
		rc = sqlite3RtreeInit(db);
	}
#endif

#ifdef SQLITE_ENABLE_JSON1
	if (!db->mallocFailed && rc == SQLITE_OK) {
		rc = sqlite3Json1Init(db);
	}
#endif

	if (rc)
		sqlite3Error(db, rc);

	/* Enable the lookaside-malloc subsystem */
	setupLookaside(db, 0, sqlite3GlobalConfig.szLookaside,
		       sqlite3GlobalConfig.nLookaside);

opendb_out:
	rc = sqlite3_errcode(db);
	assert(db != 0 || rc == SQLITE_NOMEM);
	if (rc == SQLITE_NOMEM) {
		sqlite3_close(db);
		db = 0;
	} else if (rc != SQLITE_OK)
		db->magic = SQLITE_MAGIC_SICK;

	*out_db = db;
#ifdef SQLITE_ENABLE_SQLLOG
	if (sqlite3GlobalConfig.xSqllog) {
		/* Opening a db handle. Fourth parameter is passed 0. */
		void *pArg = sqlite3GlobalConfig.pSqllogArg;
		sqlite3GlobalConfig.xSqllog(pArg, db, zFilename, 0);
	}
#endif

	return rc;
}

/*
 * The following routines are substitutes for constants
 * SQLITE_MISUSE, SQLITE_CANTOPEN, SQLITE_NOMEM and possibly other error
 * constants.  They serve two purposes:
 *
 *   1.  Serve as a convenient place to set a breakpoint in a debugger
 *       to detect when version error conditions occurs.
 *
 *   2.  Invoke sqlite3_log() to provide the source code location where
 *       a low-level error is first detected.
 */
static int
reportError(int iErr, int lineno, const char *zType)
{
	sqlite3_log(iErr, "%s at line %d of [%.10s]",
		    zType, lineno, 20 + tarantool_version());
	return iErr;
}

int
sqlite3MisuseError(int lineno)
{
	testcase(sqlite3GlobalConfig.xLog != 0);
	return reportError(SQLITE_MISUSE, lineno, "misuse");
}

int
sqlite3CantopenError(int lineno)
{
	testcase(sqlite3GlobalConfig.xLog != 0);
	return reportError(SQLITE_CANTOPEN, lineno, "cannot open file");
}

#ifdef SQLITE_DEBUG
int
sqlite3NomemError(int lineno)
{
	testcase(sqlite3GlobalConfig.xLog != 0);
	return reportError(SQLITE_NOMEM, lineno, "OOM");
}

int
sqlite3IoerrnomemError(int lineno)
{
	testcase(sqlite3GlobalConfig.xLog != 0);
	return reportError(SQLITE_IOERR_NOMEM, lineno, "I/O OOM error");
}
#endif

/*
 * Sleep for a little while.  Return the amount of time slept.
 */
int
sqlite3_sleep(int ms)
{
	sqlite3_vfs *pVfs;
	int rc;
	pVfs = sqlite3_vfs_find(0);
	if (pVfs == 0)
		return 0;

	/* This function works in milliseconds, but the underlying OsSleep()
	 * API uses microseconds. Hence the 1000's.
	 */
	rc = (sqlite3OsSleep(pVfs, 1000 * ms) / 1000);
	return rc;
}

/*
 * Enable or disable the extended result codes.
 */
int
sqlite3_extended_result_codes(sqlite3 * db, int onoff)
{
#ifdef SQLITE_ENABLE_API_ARMOR
	if (!sqlite3SafetyCheckOk(db))
		return SQLITE_MISUSE_BKPT;
#endif
	db->errMask = onoff ? 0xffffffff : 0xff;
	return SQLITE_OK;
}

/*
 * Interface to the testing logic.
 */
int
sqlite3_test_control(int op, ...)
{
	int rc = 0;
#ifdef SQLITE_UNTESTABLE
	UNUSED_PARAMETER(op);
#else
	va_list ap;
	va_start(ap, op);
	switch (op) {

		/*
		 * Save the current state of the PRNG.
		 */
	case SQLITE_TESTCTRL_PRNG_SAVE:{
			sqlite3PrngSaveState();
			break;
		}

		/*
		 * Restore the state of the PRNG to the last state saved using
		 * PRNG_SAVE.  If PRNG_SAVE has never before been called, then
		 * this verb acts like PRNG_RESET.
		 */
	case SQLITE_TESTCTRL_PRNG_RESTORE:{
			sqlite3PrngRestoreState();
			break;
		}

		/*
		 * Reset the PRNG back to its uninitialized state.  The next call
		 * to sqlite3_randomness() will reseed the PRNG using a single call
		 * to the xRandomness method of the default VFS.
		 */
	case SQLITE_TESTCTRL_PRNG_RESET:{
			sqlite3_randomness(0, 0);
			break;
		}

		/*
		 *  sqlite3_test_control(FAULT_INSTALL, xCallback)
		 *
		 * Arrange to invoke xCallback() whenever sqlite3FaultSim() is called,
		 * if xCallback is not NULL.
		 *
		 * As a test of the fault simulator mechanism itself, sqlite3FaultSim(0)
		 * is called immediately after installing the new callback and the return
		 * value from sqlite3FaultSim(0) becomes the return from
		 * sqlite3_test_control().
		 */
	case SQLITE_TESTCTRL_FAULT_INSTALL:{
			typedef int (*TESTCALLBACKFUNC_t) (int);
			sqlite3GlobalConfig.xTestCallback =
			    va_arg(ap, TESTCALLBACKFUNC_t);
			rc = sqlite3FaultSim(0);
			break;
		}

		/*
		 *  sqlite3_test_control(BENIGN_MALLOC_HOOKS, xBegin, xEnd)
		 *
		 * Register hooks to call to indicate which malloc() failures
		 * are benign.
		 */
	case SQLITE_TESTCTRL_BENIGN_MALLOC_HOOKS:{
			typedef void (*void_function) (void);
			void_function xBenignBegin;
			void_function xBenignEnd;
			xBenignBegin = va_arg(ap, void_function);
			xBenignEnd = va_arg(ap, void_function);
			sqlite3BenignMallocHooks(xBenignBegin, xBenignEnd);
			break;
		}

		/*
		 *  sqlite3_test_control(SQLITE_TESTCTRL_PENDING_BYTE, unsigned int X)
		 *
		 * Set the PENDING byte to the value in the argument, if X>0.
		 * Make no changes if X==0.  Return the value of the pending byte
		 * as it existing before this routine was called.
		 *
		 * IMPORTANT:  Changing the PENDING byte from 0x40000000 results in
		 * an incompatible database file format.  Changing the PENDING byte
		 * while any database connection is open results in undefined and
		 * deleterious behavior.
		 */
	case SQLITE_TESTCTRL_PENDING_BYTE:{
			rc = PENDING_BYTE;
#ifndef SQLITE_OMIT_WSD
			{
				unsigned int newVal = va_arg(ap, unsigned int);
				if (newVal)
					sqlite3PendingByte = newVal;
			}
#endif
			break;
		}

		/*
		 *  sqlite3_test_control(SQLITE_TESTCTRL_ASSERT, int X)
		 *
		 * This action provides a run-time test to see whether or not
		 * assert() was enabled at compile-time.  If X is true and assert()
		 * is enabled, then the return value is true.  If X is true and
		 * assert() is disabled, then the return value is zero.  If X is
		 * false and assert() is enabled, then the assertion fires and the
		 * process aborts.  If X is false and assert() is disabled, then the
		 * return value is zero.
		 */
	case SQLITE_TESTCTRL_ASSERT:{
			volatile int x = 0;
			assert( /*side-effects-ok */ (x = va_arg(ap, int)) !=
			       0);
			rc = x;
			break;
		}

		/*
		 *  sqlite3_test_control(SQLITE_TESTCTRL_ALWAYS, int X)
		 *
		 * This action provides a run-time test to see how the ALWAYS and
		 * NEVER macros were defined at compile-time.
		 *
		 * The return value is ALWAYS(X).
		 *
		 * The recommended test is X==2.  If the return value is 2, that means
		 * ALWAYS() and NEVER() are both no-op pass-through macros, which is the
		 * default setting.  If the return value is 1, then ALWAYS() is either
		 * hard-coded to true or else it asserts if its argument is false.
		 * The first behavior (hard-coded to true) is the case if
		 * SQLITE_TESTCTRL_ASSERT shows that assert() is disabled and the second
		 * behavior (assert if the argument to ALWAYS() is false) is the case if
		 * SQLITE_TESTCTRL_ASSERT shows that assert() is enabled.
		 *
		 * The run-time test procedure might look something like this:
		 *
		 *    if( sqlite3_test_control(SQLITE_TESTCTRL_ALWAYS, 2)==2 ){
		 *      // ALWAYS() and NEVER() are no-op pass-through macros
		 *    }else if( sqlite3_test_control(SQLITE_TESTCTRL_ASSERT, 1) ){
		 *      // ALWAYS(x) asserts that x is true. NEVER(x) asserts x is false.
		 *    }else{
		 *      // ALWAYS(x) is a constant 1.  NEVER(x) is a constant 0.
		 *    }
		 */
	case SQLITE_TESTCTRL_ALWAYS:{
			int x = va_arg(ap, int);
			rc = ALWAYS(x);
			break;
		}

		/*
		 *   sqlite3_test_control(SQLITE_TESTCTRL_BYTEORDER);
		 *
		 * The integer returned reveals the byte-order of the computer on which
		 * SQLite is running:
		 *
		 *       1     big-endian,    determined at run-time
		 *      10     little-endian, determined at run-time
		 *  432101     big-endian,    determined at compile-time
		 *  123410     little-endian, determined at compile-time
		 */
	case SQLITE_TESTCTRL_BYTEORDER:{
			rc = SQLITE_BYTEORDER * 100 + SQLITE_LITTLEENDIAN * 10 +
			    SQLITE_BIGENDIAN;
			break;
		}

		/*  sqlite3_test_control(SQLITE_TESTCTRL_OPTIMIZATIONS, sqlite3 *db, int N)
		 *
		 * Enable or disable various optimizations for testing purposes.  The
		 * argument N is a bitmask of optimizations to be disabled.  For normal
		 * operation N should be 0.  The idea is that a test program (like the
		 * SQL Logic Test or SLT test module) can run the same SQL multiple times
		 * with various optimizations disabled to verify that the same answer
		 * is obtained in every case.
		 */
	case SQLITE_TESTCTRL_OPTIMIZATIONS:{
			sqlite3 *db = va_arg(ap, sqlite3 *);
			db->dbOptFlags = (u16) (va_arg(ap, int) & 0xffff);
			break;
		}

#ifdef SQLITE_N_KEYWORD
		/* sqlite3_test_control(SQLITE_TESTCTRL_ISKEYWORD, const char *zWord)
		 *
		 * If zWord is a keyword recognized by the parser, then return the
		 * number of keywords.  Or if zWord is not a keyword, return 0.
		 *
		 * This test feature is only available in the amalgamation since
		 * the SQLITE_N_KEYWORD macro is not defined in this file if SQLite
		 * is built using separate source files.
		 */
	case SQLITE_TESTCTRL_ISKEYWORD:{
			const char *zWord = va_arg(ap, const char *);
			int n = sqlite3Strlen30(zWord);
			rc = (sqlite3KeywordCode((u8 *) zWord, n) !=
			      TK_ID) ? SQLITE_N_KEYWORD : 0;
			break;
		}
#endif

		/* sqlite3_test_control(SQLITE_TESTCTRL_SCRATCHMALLOC, sz, &pNew, pFree);
		 *
		 * Pass pFree into sqlite3ScratchFree().
		 * If sz>0 then allocate a scratch buffer into pNew.
		 */
	case SQLITE_TESTCTRL_SCRATCHMALLOC:{
			void *pFree, **ppNew;
			int sz;
			sz = va_arg(ap, int);
			ppNew = va_arg(ap, void **);
			pFree = va_arg(ap, void *);
			if (sz)
				*ppNew = sqlite3ScratchMalloc(sz);
			sqlite3ScratchFree(pFree);
			break;
		}

		/*   sqlite3_test_control(SQLITE_TESTCTRL_LOCALTIME_FAULT, int onoff);
		 *
		 * If parameter onoff is non-zero, configure the wrappers so that all
		 * subsequent calls to localtime() and variants fail. If onoff is zero,
		 * undo this setting.
		 */
	case SQLITE_TESTCTRL_LOCALTIME_FAULT:{
			sqlite3GlobalConfig.bLocaltimeFault = va_arg(ap, int);
			break;
		}

		/*   sqlite3_test_control(SQLITE_TESTCTRL_NEVER_CORRUPT, int);
		 *
		 * Set or clear a flag that indicates that the database file is always well-
		 * formed and never corrupt.  This flag is clear by default, indicating that
		 * database files might have arbitrary corruption.  Setting the flag during
		 * testing causes certain assert() statements in the code to be activated
		 * that demonstrat invariants on well-formed database files.
		 */
	case SQLITE_TESTCTRL_NEVER_CORRUPT:{
			sqlite3GlobalConfig.neverCorrupt = va_arg(ap, int);
			break;
		}

		/* Set the threshold at which OP_Once counters reset back to zero.
		 * By default this is 0x7ffffffe (over 2 billion), but that value is
		 * too big to test in a reasonable amount of time, so this control is
		 * provided to set a small and easily reachable reset value.
		 */
	case SQLITE_TESTCTRL_ONCE_RESET_THRESHOLD:{
			sqlite3GlobalConfig.iOnceResetThreshold =
			    va_arg(ap, int);
			break;
		}

		/*   sqlite3_test_control(SQLITE_TESTCTRL_VDBE_COVERAGE, xCallback, ptr);
		 *
		 * Set the VDBE coverage callback function to xCallback with context
		 * pointer ptr.
		 */
	case SQLITE_TESTCTRL_VDBE_COVERAGE:{
#ifdef SQLITE_VDBE_COVERAGE
			typedef void (*branch_callback) (void *, int, u8, u8);
			sqlite3GlobalConfig.xVdbeBranch =
			    va_arg(ap, branch_callback);
			sqlite3GlobalConfig.pVdbeBranchArg = va_arg(ap, void *);
#endif
			break;
		}

		/*   sqlite3_test_control(SQLITE_TESTCTRL_SORTER_MMAP, db, nMax); */
	case SQLITE_TESTCTRL_SORTER_MMAP:{
			sqlite3 *db = va_arg(ap, sqlite3 *);
			db->nMaxSorterMmap = va_arg(ap, int);
			break;
		}

		/*   sqlite3_test_control(SQLITE_TESTCTRL_ISINIT);
		 *
		 * Return SQLITE_OK if SQLite has been initialized and SQLITE_ERROR if
		 * not.
		 */
	case SQLITE_TESTCTRL_ISINIT:{
			if (sqlite3GlobalConfig.isInit == 0)
				rc = SQLITE_ERROR;
			break;
		}
	}
	va_end(ap);
#endif				/* SQLITE_UNTESTABLE */
	return rc;
}

/*
 * This is a utility routine, useful to VFS implementations, that checks
 * to see if a database file was a URI that contained a specific query
 * parameter, and if so obtains the value of the query parameter.
 *
 * The zFilename argument is the filename pointer passed into the xOpen()
 * method of a VFS implementation.  The zParam argument is the name of the
 * query parameter we seek.  This routine returns the value of the zParam
 * parameter if it exists.  If the parameter does not exist, this routine
 * returns a NULL pointer.
 */
const char *
sqlite3_uri_parameter(const char *zFilename, const char *zParam)
{
	if (zFilename == 0 || zParam == 0)
		return 0;
	zFilename += sqlite3Strlen30(zFilename) + 1;
	while (zFilename[0]) {
		int x = strcmp(zFilename, zParam);
		zFilename += sqlite3Strlen30(zFilename) + 1;
		if (x == 0)
			return zFilename;
		zFilename += sqlite3Strlen30(zFilename) + 1;
	}
	return 0;
}

/*
 * Return a boolean value for a query parameter.
 */
int
sqlite3_uri_boolean(const char *zFilename, const char *zParam, int bDflt)
{
	const char *z = sqlite3_uri_parameter(zFilename, zParam);
	bDflt = bDflt != 0;
	return z ? sqlite3GetBoolean(z, bDflt) : bDflt;
}

/*
 * Return a 64-bit integer value for a query parameter.
 */
sqlite3_int64
sqlite3_uri_int64(const char *zFilename,	/* Filename as passed to xOpen */
		  const char *zParam,	/* URI parameter sought */
		  sqlite3_int64 bDflt)	/* return if parameter is missing */
{
	const char *z = sqlite3_uri_parameter(zFilename, zParam);
	int64_t v;
	if (z != NULL && sql_dec_or_hex_to_i64(z, &v) == 0)
		bDflt = v;
	return bDflt;
}


#ifdef SQLITE_ENABLE_SNAPSHOT
/*
 * Obtain a snapshot handle for the snapshot of database zDb currently
 * being read by handle db.
 */
int
sqlite3_snapshot_get(sqlite3 * db,
		     const char *zDb, sqlite3_snapshot ** ppSnapshot)
{
	int rc = SQLITE_ERROR;
	return rc;
}

/*
 * Open a read-transaction on the snapshot idendified by pSnapshot.
 */
int
sqlite3_snapshot_open(sqlite3 * db,
		      const char *zDb, sqlite3_snapshot * pSnapshot)
{
	int rc = SQLITE_ERROR;
	return rc;
}

/*
 * Recover as many snapshots as possible from the wal file associated with
 * schema zDb of database db.
 */
int
sqlite3_snapshot_recover(sqlite3 * db, const char *zDb)
{
	int rc = SQLITE_ERROR;
	return rc;
}

/*
 * Free a snapshot handle obtained from sqlite3_snapshot_get().
 */
void
sqlite3_snapshot_free(sqlite3_snapshot * pSnapshot)
{
	sqlite3_free(pSnapshot);
}
#endif				/* SQLITE_ENABLE_SNAPSHOT */
