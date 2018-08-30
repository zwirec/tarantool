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
 * This module contains C code that generates VDBE code used to process
 * the WHERE clause of SQL statements.
 *
 * This file was split off from where.c on 2015-06-06 in order to reduce the
 * size of where.c and make it easier to edit.  This file contains the routines
 * that actually generate the bulk of the WHERE loop code.  The original where.c
 * file retains the code that does query planning and analysis.
 */
#include "box/schema.h"
#include "sqliteInt.h"
#include "whereInt.h"

/*
 * Return the name of the i-th column of the pIdx index.
 */
static const char *
explainIndexColumnName(Index * pIdx, int i)
{
	i = pIdx->def->key_def->parts[i].fieldno;
	return pIdx->pTable->def->fields[i].name;
}

/*
 * This routine is a helper for explainIndexRange() below
 *
 * pStr holds the text of an expression that we are building up one term
 * at a time.  This routine adds a new term to the end of the expression.
 * Terms are separated by AND so add the "AND" text for second and subsequent
 * terms only.
 */
static void
explainAppendTerm(StrAccum * pStr,	/* The text expression being built */
		  Index * pIdx,		/* Index to read column names from */
		  struct index_def *def,
		  int nTerm,		/* Number of terms */
		  int iTerm,		/* Zero-based index of first term. */
		  int bAnd,		/* Non-zero to append " AND " */
		  const char *zOp)	/* Name of the operator */
{
	int i;

	assert(nTerm >= 1);
	if (bAnd)
		sqlite3StrAccumAppend(pStr, " AND ", 5);

	if (nTerm > 1)
		sqlite3StrAccumAppend(pStr, "(", 1);
	for (i = 0; i < nTerm; i++) {
		if (i)
			sqlite3StrAccumAppend(pStr, ",", 1);
		const char *name;
		if (pIdx != NULL) {
			name = explainIndexColumnName(pIdx, iTerm + i);
		} else {
			assert(def != NULL);
                        struct space *space = space_cache_find(def->space_id);
                        assert(space != NULL);
                        name = space->def->fields[i + iTerm].name;
		}
		sqlite3StrAccumAppendAll(pStr, name);
	}
	if (nTerm > 1)
		sqlite3StrAccumAppend(pStr, ")", 1);

	sqlite3StrAccumAppend(pStr, zOp, 1);

	if (nTerm > 1)
		sqlite3StrAccumAppend(pStr, "(", 1);
	for (i = 0; i < nTerm; i++) {
		if (i)
			sqlite3StrAccumAppend(pStr, ",", 1);
		sqlite3StrAccumAppend(pStr, "?", 1);
	}
	if (nTerm > 1)
		sqlite3StrAccumAppend(pStr, ")", 1);
}

/*
 * Argument pLevel describes a strategy for scanning table pTab. This
 * function appends text to pStr that describes the subset of table
 * rows scanned by the strategy in the form of an SQL expression.
 *
 * For example, if the query:
 *
 *   SELECT * FROM t1 WHERE a=1 AND b>2;
 *
 * is run and there is an index on (a, b), then this function returns a
 * string similar to:
 *
 *   "a=? AND b>?"
 */
static void
explainIndexRange(StrAccum * pStr, WhereLoop * pLoop)
{
	Index *pIndex = pLoop->pIndex;
	struct index_def *def = pLoop->index_def;
	u16 nEq = pLoop->nEq;
	u16 nSkip = pLoop->nSkip;
	int i, j;

	assert(pIndex != NULL || def != NULL);

	if (nEq == 0
	    && (pLoop->wsFlags & (WHERE_BTM_LIMIT | WHERE_TOP_LIMIT)) == 0)
		return;
	sqlite3StrAccumAppend(pStr, " (", 2);
	for (i = 0; i < nEq; i++) {
		const char *z;
		if (pIndex != NULL) {
			z = explainIndexColumnName(pIndex, i);
		} else {
			struct space *space = space_cache_find(def->space_id);
			assert(space != NULL);
			uint32_t fieldno = def->key_def->parts[i].fieldno;
			z = space->def->fields[fieldno].name;
		}
		if (i)
			sqlite3StrAccumAppend(pStr, " AND ", 5);
		sqlite3XPrintf(pStr, i >= nSkip ? "%s=?" : "ANY(%s)", z);
	}

	j = i;
	if (pLoop->wsFlags & WHERE_BTM_LIMIT) {
		explainAppendTerm(pStr, pIndex, def, pLoop->nBtm, j, i, ">");
		i = 1;
	}
	if (pLoop->wsFlags & WHERE_TOP_LIMIT) {
		explainAppendTerm(pStr, pIndex, def, pLoop->nTop, j, i, "<");
	}
	sqlite3StrAccumAppend(pStr, ")", 1);
}

/*
 * This function is a no-op unless currently processing an EXPLAIN QUERY PLAN
 * command, or if either SQLITE_DEBUG or SQLITE_ENABLE_STMT_SCANSTATUS was
 * defined at compile-time. If it is not a no-op, a single OP_Explain opcode
 * is added to the output to describe the table scan strategy in pLevel.
 *
 * If an OP_Explain opcode is added to the VM, its address is returned.
 * Otherwise, if no OP_Explain is coded, zero is returned.
 */
int
sqlite3WhereExplainOneScan(Parse * pParse,	/* Parse context */
			   SrcList * pTabList,	/* Table list this loop refers to */
			   WhereLevel * pLevel,	/* Scan to write OP_Explain opcode for */
			   int iLevel,		/* Value for "level" column of output */
			   int iFrom,		/* Value for "from" column of output */
			   u16 wctrlFlags)	/* Flags passed to sqlite3WhereBegin() */
{
	int ret = 0;
#if !defined(SQLITE_DEBUG) && !defined(SQLITE_ENABLE_STMT_SCANSTATUS)
	if (pParse->explain == 2)
#endif
	{
		struct SrcList_item *pItem = &pTabList->a[pLevel->iFrom];
		Vdbe *v = pParse->pVdbe;	/* VM being constructed */
		sqlite3 *db = pParse->db;	/* Database handle */
		int iId = pParse->iSelectId;	/* Select id (left-most output column) */
		int isSearch;	/* True for a SEARCH. False for SCAN. */
		WhereLoop *pLoop;	/* The controlling WhereLoop object */
		u32 flags;	/* Flags that describe this loop */
		char *zMsg;	/* Text to add to EQP output */
		StrAccum str;	/* EQP output string */
		char zBuf[100];	/* Initial space for EQP output string */

		pLoop = pLevel->pWLoop;
		flags = pLoop->wsFlags;
		if ((flags & WHERE_MULTI_OR)
		    || (wctrlFlags & WHERE_OR_SUBCLAUSE))
			return 0;

		isSearch = (flags & (WHERE_BTM_LIMIT | WHERE_TOP_LIMIT)) != 0
		    || (pLoop->nEq > 0)
		    || (wctrlFlags & (WHERE_ORDERBY_MIN | WHERE_ORDERBY_MAX));

		sqlite3StrAccumInit(&str, db, zBuf, sizeof(zBuf),
				    SQLITE_MAX_LENGTH);
		sqlite3StrAccumAppendAll(&str, isSearch ? "SEARCH" : "SCAN");
		if (pItem->pSelect) {
			sqlite3XPrintf(&str, " SUBQUERY %d", pItem->iSelectId);
		} else {
			sqlite3XPrintf(&str, " TABLE %s", pItem->zName);
		}

		if (pItem->zAlias) {
			sqlite3XPrintf(&str, " AS %s", pItem->zAlias);
		}
		if ((flags & WHERE_IPK) == 0) {
			const char *zFmt = 0;
			Index *pIdx = pLoop->pIndex;
			struct index_def *idx_def = pLoop->index_def;
			if (pIdx == NULL && idx_def == NULL) return 0;

			assert(pIdx != NULL || idx_def != NULL);
			assert(!(flags & WHERE_AUTO_INDEX)
			       || (flags & WHERE_IDX_ONLY));
			if ((pIdx != NULL && sql_index_is_primary(pIdx)) ||
			    (idx_def != NULL && idx_def->iid == 0)) {
				if (isSearch) {
					zFmt = "PRIMARY KEY";
				}
			} else if (flags & WHERE_AUTO_INDEX) {
				zFmt = "AUTOMATIC COVERING INDEX";
			} else if (flags & WHERE_IDX_ONLY) {
				zFmt = "COVERING INDEX %s";
			} else {
				zFmt = "INDEX %s";
			}
			if (zFmt) {
				sqlite3StrAccumAppend(&str, " USING ", 7);
				if (pIdx != NULL)
					sqlite3XPrintf(&str, zFmt, pIdx->def->name);
				else if (idx_def != NULL)
					sqlite3XPrintf(&str, zFmt, idx_def->name);
				else
					sqlite3XPrintf(&str, zFmt, "EPHEMERAL INDEX");
				explainIndexRange(&str, pLoop);
			}
		} else if ((flags & WHERE_IPK) != 0
			   && (flags & WHERE_CONSTRAINT) != 0) {
			const char *zRangeOp;
			if (flags & (WHERE_COLUMN_EQ | WHERE_COLUMN_IN)) {
				zRangeOp = "=";
			} else if ((flags & WHERE_BOTH_LIMIT) ==
				   WHERE_BOTH_LIMIT) {
				zRangeOp = ">? AND rowid<";
			} else if (flags & WHERE_BTM_LIMIT) {
				zRangeOp = ">";
			} else {
				assert(flags & WHERE_TOP_LIMIT);
				zRangeOp = "<";
			}
			sqlite3XPrintf(&str,
				       " USING INTEGER PRIMARY KEY (rowid%s?)",
				       zRangeOp);
		}
#ifdef SQLITE_EXPLAIN_ESTIMATED_ROWS
		if (pLoop->nOut >= 10) {
			sqlite3XPrintf(&str, " (~%llu rows)",
				       sqlite3LogEstToInt(pLoop->nOut));
		} else {
			sqlite3StrAccumAppend(&str, " (~1 row)", 9);
		}
#endif
		zMsg = sqlite3StrAccumFinish(&str);
		ret =
		    sqlite3VdbeAddOp4(v, OP_Explain, iId, iLevel, iFrom, zMsg,
				      P4_DYNAMIC);
	}
	return ret;
}

#ifdef SQLITE_ENABLE_STMT_SCANSTATUS
/*
 * Configure the VM passed as the first argument with an
 * sqlite3_stmt_scanstatus() entry corresponding to the scan used to
 * implement level pLvl. Argument pSrclist is a pointer to the FROM
 * clause that the scan reads data from.
 *
 * If argument addrExplain is not 0, it must be the address of an
 * OP_Explain instruction that describes the same loop.
 */
void
sqlite3WhereAddScanStatus(Vdbe * v,		/* Vdbe to add scanstatus entry to */
			  SrcList * pSrclist,	/* FROM clause pLvl reads data from */
			  WhereLevel * pLvl,	/* Level to add scanstatus() entry for */
			  int addrExplain)	/* Address of OP_Explain (or 0) */
{
	const char *zObj = 0;
	WhereLoop *pLoop = pLvl->pWLoop;
	if (pLoop->pIndex != 0) {
		zObj = pLoop->pIndex->zName;
	} else {
		zObj = pSrclist->a[pLvl->iFrom].zName;
	}
	sqlite3VdbeScanStatus(v, addrExplain, pLvl->addrBody, pLvl->addrVisit,
			      pLoop->nOut, zObj);
}
#endif

/*
 * Disable a term in the WHERE clause.  Except, do not disable the term
 * if it controls a LEFT OUTER JOIN and it did not originate in the ON
 * or USING clause of that join.
 *
 * Consider the term t2.z='ok' in the following queries:
 *
 *   (1)  SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.x WHERE t2.z='ok'
 *   (2)  SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.x AND t2.z='ok'
 *   (3)  SELECT * FROM t1, t2 WHERE t1.a=t2.x AND t2.z='ok'
 *
 * The t2.z='ok' is disabled in the in (2) because it originates
 * in the ON clause.  The term is disabled in (3) because it is not part
 * of a LEFT OUTER JOIN.  In (1), the term is not disabled.
 *
 * Disabling a term causes that term to not be tested in the inner loop
 * of the join.  Disabling is an optimization.  When terms are satisfied
 * by indices, we disable them to prevent redundant tests in the inner
 * loop.  We would get the correct results if nothing were ever disabled,
 * but joins might run a little slower.  The trick is to disable as much
 * as we can without disabling too much.  If we disabled in (1), we'd get
 * the wrong answer.  See ticket #813.
 *
 * If all the children of a term are disabled, then that term is also
 * automatically disabled.  In this way, terms get disabled if derived
 * virtual terms are tested first.  For example:
 *
 *      x GLOB 'abc*' AND x>='abc' AND x<'acd'
 *      \___________/     \______/     \_____/
 *         parent          child1       child2
 *
 * Only the parent term was in the original WHERE clause.  The child1
 * and child2 terms were added by the LIKE optimization.  If both of
 * the virtual child terms are valid, then testing of the parent can be
 * skipped.
 *
 * Usually the parent term is marked as TERM_CODED.  But if the parent
 * term was originally TERM_LIKE, then the parent gets TERM_LIKECOND instead.
 * The TERM_LIKECOND marking indicates that the term should be coded inside
 * a conditional such that is only evaluated on the second pass of a
 * LIKE-optimization loop, when scanning BLOBs instead of strings.
 */
static void
disableTerm(WhereLevel * pLevel, WhereTerm * pTerm)
{
	int nLoop = 0;
	while (ALWAYS(pTerm != 0)
	       && (pTerm->wtFlags & TERM_CODED) == 0
	       && (pLevel->iLeftJoin == 0
		   || ExprHasProperty(pTerm->pExpr, EP_FromJoin))
	       && (pLevel->not_ready_mask & pTerm->prereq_all_mask) == 0) {
		if (nLoop && (pTerm->wtFlags & TERM_LIKE) != 0) {
			pTerm->wtFlags |= TERM_LIKECOND;
		} else {
			pTerm->wtFlags |= TERM_CODED;
		}
		if (pTerm->iParent < 0)
			break;
		pTerm = &pTerm->pWC->a[pTerm->iParent];
		pTerm->nChild--;
		if (pTerm->nChild != 0)
			break;
		nLoop++;
	}
}

/*
 * Code an OP_Affinity opcode to apply the column affinity string zAff
 * to the n registers starting at base.
 *
 * As an optimization, AFFINITY_BLOB entries (which are no-ops) at the
 * beginning and end of zAff are ignored.  If all entries in zAff are
 * AFFINITY_BLOB, then no code gets generated.
 *
 * This routine makes its own copy of zAff so that the caller is free
 * to modify zAff after this routine returns.
 */
static void
codeApplyAffinity(Parse * pParse, int base, int n, char *zAff)
{
	Vdbe *v = pParse->pVdbe;
	if (zAff == 0) {
		assert(pParse->db->mallocFailed);
		return;
	}
	assert(v != 0);

	/* Adjust base and n to skip over AFFINITY_BLOB entries at the beginning
	 * and end of the affinity string.
	 */
	while (n > 0 && zAff[0] == AFFINITY_BLOB) {
		n--;
		base++;
		zAff++;
	}
	while (n > 1 && zAff[n - 1] == AFFINITY_BLOB) {
		n--;
	}

	/* Code the OP_Affinity opcode if there is anything left to do. */
	if (n > 0) {
		sqlite3VdbeAddOp4(v, OP_Affinity, base, n, 0, zAff, n);
		sqlite3ExprCacheAffinityChange(pParse, base, n);
	}
}

/*
 * Expression pRight, which is the RHS of a comparison operation, is
 * either a vector of n elements or, if n==1, a scalar expression.
 * Before the comparison operation, affinity zAff is to be applied
 * to the pRight values. This function modifies characters within the
 * affinity string to AFFINITY_BLOB if either:
 *
 *   * the comparison will be performed with no affinity, or
 *   * the affinity change in zAff is guaranteed not to change the value.
 */
static void
updateRangeAffinityStr(Expr * pRight,	/* RHS of comparison */
		       int n,		/* Number of vector elements in comparison */
		       char *zAff)	/* Affinity string to modify */
{
	int i;
	for (i = 0; i < n; i++) {
		Expr *p = sqlite3VectorFieldSubexpr(pRight, i);
		if (sqlite3CompareAffinity(p, zAff[i]) == AFFINITY_BLOB
		    || sqlite3ExprNeedsNoAffinityChange(p, zAff[i])) {
			zAff[i] = AFFINITY_BLOB;
		}
	}
}

/*
 * Generate code for a single equality term of the WHERE clause.  An equality
 * term can be either X=expr or X IN (...).   pTerm is the term to be
 * coded.
 *
 * The current value for the constraint is left in a register, the index
 * of which is returned.  An attempt is made store the result in iTarget but
 * this is only guaranteed for TK_ISNULL and TK_IN constraints.  If the
 * constraint is a TK_EQ, then the current value might be left in
 * some other register and it is the caller's responsibility to compensate.
 *
 * For a constraint of the form X=expr, the expression is evaluated in
 * straight-line code.  For constraints of the form X IN (...)
 * this routine sets up a loop that will iterate over all values of X.
 */
static int
codeEqualityTerm(Parse * pParse,	/* The parsing context */
		 WhereTerm * pTerm,	/* The term of the WHERE clause to be coded */
		 WhereLevel * pLevel,	/* The level of the FROM clause we are working on */
		 int iEq,		/* Index of the equality term within this level */
		 int bRev,		/* True for reverse-order IN operations */
		 int iTarget)		/* Attempt to leave results in this register */
{
	Expr *pX = pTerm->pExpr;
	Vdbe *v = pParse->pVdbe;
	int iReg;		/* Register holding results */
	int iSingleIdxCol = 0;	/* Tarantool. In case of (nExpr == 1) store column index here.  */

	assert(pLevel->pWLoop->aLTerm[iEq] == pTerm);
	assert(iTarget > 0);
	if (pX->op == TK_EQ) {
		iReg = sqlite3ExprCodeTarget(pParse, pX->pRight, iTarget);
	} else if (pX->op == TK_ISNULL) {
		iReg = iTarget;
		sqlite3VdbeAddOp2(v, OP_Null, 0, iReg);
	} else {
		int eType = IN_INDEX_NOOP;
		int iTab;
		struct InLoop *pIn;
		WhereLoop *pLoop = pLevel->pWLoop;
		int i;
		int nEq = 0;
		int *aiMap = 0;

		if (pLoop->pIndex != 0 &&
		    pLoop->pIndex->def->key_def->parts[iEq].sort_order) {
			testcase(iEq == 0);
			testcase(bRev);
			bRev = !bRev;
		}
		assert(pX->op == TK_IN);
		iReg = iTarget;

		for (i = 0; i < iEq; i++) {
			if (pLoop->aLTerm[i] && pLoop->aLTerm[i]->pExpr == pX) {
				disableTerm(pLevel, pTerm);
				return iTarget;
			}
		}
		for (i = iEq; i < pLoop->nLTerm; i++) {
			if (ALWAYS(pLoop->aLTerm[i])
			    && pLoop->aLTerm[i]->pExpr == pX)
				nEq++;
		}

		if ((pX->flags & EP_xIsSelect) == 0
		    || pX->x.pSelect->pEList->nExpr == 1) {
			eType =
			    sqlite3FindInIndex(pParse, pX, IN_INDEX_LOOP, 0, 0,
					       &iSingleIdxCol);
		} else {
			Select *pSelect = pX->x.pSelect;
			sqlite3 *db = pParse->db;
			u16 savedDbOptFlags = db->dbOptFlags;
			ExprList *pOrigRhs = pSelect->pEList;
			ExprList *pOrigLhs = pX->pLeft->x.pList;
			ExprList *pRhs = 0;	/* New Select.pEList for RHS */
			ExprList *pLhs = 0;	/* New pX->pLeft vector */

			for (i = iEq; i < pLoop->nLTerm; i++) {
				if (pLoop->aLTerm[i]->pExpr == pX) {
					int iField =
					    pLoop->aLTerm[i]->iField - 1;
					Expr *pNewRhs =
					    sqlite3ExprDup(db,
							   pOrigRhs->a[iField].
							   pExpr, 0);
					Expr *pNewLhs =
					    sqlite3ExprDup(db,
							   pOrigLhs->a[iField].
							   pExpr, 0);

					pRhs =
					    sql_expr_list_append(pParse->db,
								 pRhs, pNewRhs);
					pLhs =
					    sql_expr_list_append(pParse->db,
								 pLhs, pNewLhs);
				}
			}
			if (!db->mallocFailed) {
				Expr *pLeft = pX->pLeft;

				if (pSelect->pOrderBy) {
					/* If the SELECT statement has an ORDER BY clause, zero the
					 * iOrderByCol variables. These are set to non-zero when an
					 * ORDER BY term exactly matches one of the terms of the
					 * result-set. Since the result-set of the SELECT statement may
					 * have been modified or reordered, these variables are no longer
					 * set correctly.  Since setting them is just an optimization,
					 * it's easiest just to zero them here.
					 */
					ExprList *pOrderBy = pSelect->pOrderBy;
					for (i = 0; i < pOrderBy->nExpr; i++) {
						pOrderBy->a[i].u.x.iOrderByCol =
						    0;
					}
				}

				/* Take care here not to generate a TK_VECTOR containing only a
				 * single value. Since the parser never creates such a vector, some
				 * of the subroutines do not handle this case.
				 */
				if (pLhs->nExpr == 1) {
					pX->pLeft = pLhs->a[0].pExpr;
				} else {
					pLeft->x.pList = pLhs;
					aiMap =
					    (int *)sqlite3DbMallocZero(pParse->db,
								       sizeof(int) * nEq);
					testcase(aiMap == 0);
				}
				pSelect->pEList = pRhs;
				db->dbOptFlags |= SQLITE_QueryFlattener;
				eType =
				    sqlite3FindInIndex(pParse, pX,
						       IN_INDEX_LOOP, 0, aiMap,
						       0);
				db->dbOptFlags = savedDbOptFlags;
				testcase(aiMap != 0 && aiMap[0] != 0);
				pSelect->pEList = pOrigRhs;
				pLeft->x.pList = pOrigLhs;
				pX->pLeft = pLeft;
			}
			sql_expr_list_delete(pParse->db, pLhs);
			sql_expr_list_delete(pParse->db, pRhs);
		}

		if (eType == IN_INDEX_INDEX_DESC) {
			testcase(bRev);
			bRev = !bRev;
		}
		iTab = pX->iTable;
		sqlite3VdbeAddOp2(v, bRev ? OP_Last : OP_Rewind, iTab, 0);
		VdbeCoverageIf(v, bRev);
		VdbeCoverageIf(v, !bRev);
		assert((pLoop->wsFlags & WHERE_MULTI_OR) == 0);

		pLoop->wsFlags |= WHERE_IN_ABLE;
		if (pLevel->u.in.nIn == 0) {
			pLevel->addrNxt = sqlite3VdbeMakeLabel(v);
		}

		i = pLevel->u.in.nIn;
		pLevel->u.in.nIn += nEq;
		pLevel->u.in.aInLoop =
		    sqlite3DbReallocOrFree(pParse->db, pLevel->u.in.aInLoop,
					   sizeof(pLevel->u.in.aInLoop[0]) *
					   pLevel->u.in.nIn);
		pIn = pLevel->u.in.aInLoop;
		if (pIn) {
			int iMap = 0;	/* Index in aiMap[] */
			pIn += i;
			for (i = iEq; i < pLoop->nLTerm; i++) {
				if (pLoop->aLTerm[i]->pExpr == pX) {
					int iOut = iReg + i - iEq;
					int iCol =
						aiMap ? aiMap[iMap++] :
						iSingleIdxCol;
					pIn->addrInTop =
						sqlite3VdbeAddOp3(v, OP_Column,
								  iTab, iCol,
								  iOut);
					sqlite3VdbeAddOp1(v, OP_IsNull, iOut);
					VdbeCoverage(v);
					if (i == iEq) {
						pIn->iCur = iTab;
						pIn->eEndLoopOp =
						    bRev ? OP_PrevIfOpen : OP_NextIfOpen;
					} else {
						pIn->eEndLoopOp = OP_Noop;
					}
					pIn++;
				}
			}
		} else {
			pLevel->u.in.nIn = 0;
		}
		sqlite3DbFree(pParse->db, aiMap);
	}
	disableTerm(pLevel, pTerm);
	return iReg;
}

/*
 * Generate code that will evaluate all == and IN constraints for an
 * index scan.
 *
 * For example, consider table t1(a,b,c,d,e,f) with index i1(a,b,c).
 * Suppose the WHERE clause is this:  a==5 AND b IN (1,2,3) AND c>5 AND c<10
 * The index has as many as three equality constraints, but in this
 * example, the third "c" value is an inequality.  So only two
 * constraints are coded.  This routine will generate code to evaluate
 * a==5 and b IN (1,2,3).  The current values for a and b will be stored
 * in consecutive registers and the index of the first register is returned.
 *
 * In the example above nEq==2.  But this subroutine works for any value
 * of nEq including 0.  If nEq==0, this routine is nearly a no-op.
 * The only thing it does is allocate the pLevel->iMem memory cell and
 * compute the affinity string.
 *
 * The nExtraReg parameter is 0 or 1.  It is 0 if all WHERE clause constraints
 * are == or IN and are covered by the nEq.  nExtraReg is 1 if there is
 * an inequality constraint (such as the "c>=5 AND c<10" in the example) that
 * occurs after the nEq quality constraints.
 *
 * This routine allocates a range of nEq+nExtraReg memory cells and returns
 * the index of the first memory cell in that range. The code that
 * calls this routine will use that memory range to store keys for
 * start and termination conditions of the loop.
 * key value of the loop.  If one or more IN operators appear, then
 * this routine allocates an additional nEq memory cells for internal
 * use.
 *
 * Before returning, *pzAff is set to point to a buffer containing a
 * copy of the column affinity string of the index allocated using
 * sqlite3DbMalloc(). Except, entries in the copy of the string associated
 * with equality constraints that use BLOB or NONE affinity are set to
 * AFFINITY_BLOB. This is to deal with SQL such as the following:
 *
 *   CREATE TABLE t1(a TEXT PRIMARY KEY, b);
 *   SELECT ... FROM t1 AS t2, t1 WHERE t1.a = t2.b;
 *
 * In the example above, the index on t1(a) has TEXT affinity. But since
 * the right hand side of the equality constraint (t2.b) has BLOB/NONE affinity,
 * no conversion should be attempted before using a t2.b value as part of
 * a key to search the index. Hence the first byte in the returned affinity
 * string in this example would be set to AFFINITY_BLOB.
 */
static int
codeAllEqualityTerms(Parse * pParse,	/* Parsing context */
		     WhereLevel * pLevel,	/* Which nested loop of the FROM we are coding */
		     int bRev,		/* Reverse the order of IN operators */
		     int nExtraReg,	/* Number of extra registers to allocate */
		     char **pzAff)	/* OUT: Set to point to affinity string */
{
	u16 nEq;		/* The number of == or IN constraints to code */
	u16 nSkip;		/* Number of left-most columns to skip */
	Vdbe *v = pParse->pVdbe;	/* The vm under construction */
	WhereTerm *pTerm;	/* A single constraint term */
	WhereLoop *pLoop;	/* The WhereLoop object */
	int j;			/* Loop counter */
	int regBase;		/* Base register */
	int nReg;		/* Number of registers to allocate */

	/* This module is only called on query plans that use an index. */
	pLoop = pLevel->pWLoop;
	nEq = pLoop->nEq;
	nSkip = pLoop->nSkip;
	struct Index *pIdx = pLoop->pIndex;
	struct index_def *idx_def = pLoop->index_def;
	assert(pIdx != NULL || idx_def != NULL);

	/* Figure out how many memory cells we will need then allocate them.
	 */
	regBase = pParse->nMem + 1;
	nReg = pLoop->nEq + nExtraReg;
	pParse->nMem += nReg;

	char *zAff;
	if (pIdx != NULL) {
		struct space *space = space_by_id(pIdx->def->space_id);
		assert(space != NULL);
		zAff = sqlite3DbStrDup(pParse->db,
				       sql_space_index_affinity_str(pParse->db,
								    space->def,
								    pIdx->def));
	} else {
		struct space *space = space_by_id(idx_def->space_id);
		assert(space != NULL);
		zAff = sql_space_index_affinity_str(pParse->db, space->def,
						    idx_def);
	}
	assert(zAff != 0 || pParse->db->mallocFailed);

	if (nSkip) {
		int iIdxCur = pLevel->iIdxCur;
		sqlite3VdbeAddOp1(v, (bRev ? OP_Last : OP_Rewind), iIdxCur);
		VdbeCoverageIf(v, bRev == 0);
		VdbeCoverageIf(v, bRev != 0);
		VdbeComment((v, "begin skip-scan on %s", pIdx->def->name));
		j = sqlite3VdbeAddOp0(v, OP_Goto);
		pLevel->addrSkip =
		    sqlite3VdbeAddOp4Int(v, (bRev ? OP_SeekLT : OP_SeekGT),
					 iIdxCur, 0, regBase, nSkip);
		VdbeCoverageIf(v, bRev == 0);
		VdbeCoverageIf(v, bRev != 0);
		sqlite3VdbeJumpHere(v, j);
		for (j = 0; j < nSkip; j++) {
			sqlite3VdbeAddOp3(v, OP_Column, iIdxCur,
					  pIdx->def->key_def->parts[j].fieldno,
					  regBase + j);
			VdbeComment((v, "%s", explainIndexColumnName(pIdx, j)));
		}
	}

	/* Evaluate the equality constraints
	 */
	assert(zAff == 0 || (int)strlen(zAff) >= nEq);
	for (j = nSkip; j < nEq; j++) {
		int r1;
		pTerm = pLoop->aLTerm[j];
		assert(pTerm != 0);
		/* The following testcase is true for indices with redundant columns.
		 * Ex: CREATE INDEX i1 ON t1(a,b,a); SELECT * FROM t1 WHERE a=0 AND b=0;
		 */
		testcase((pTerm->wtFlags & TERM_CODED) != 0);
		testcase(pTerm->wtFlags & TERM_VIRTUAL);
		r1 = codeEqualityTerm(pParse, pTerm, pLevel, j, bRev,
				      regBase + j);
		if (r1 != regBase + j) {
			if (nReg == 1) {
				sqlite3ReleaseTempReg(pParse, regBase);
				regBase = r1;
			} else {
				sqlite3VdbeAddOp2(v, OP_SCopy, r1, regBase + j);
			}
		}
		if (pTerm->eOperator & WO_IN) {
			if (pTerm->pExpr->flags & EP_xIsSelect) {
				/* No affinity ever needs to be (or should be) applied to a value
				 * from the RHS of an "? IN (SELECT ...)" expression. The
				 * sqlite3FindInIndex() routine has already ensured that the
				 * affinity of the comparison has been applied to the value.
				 */
				if (zAff)
					zAff[j] = AFFINITY_BLOB;
			}
		} else if ((pTerm->eOperator & WO_ISNULL) == 0) {
			Expr *pRight = pTerm->pExpr->pRight;
			if (sqlite3ExprCanBeNull(pRight)) {
				sqlite3VdbeAddOp2(v, OP_IsNull, regBase + j,
						  pLevel->addrBrk);
				VdbeCoverage(v);
			}
			if (zAff) {
				if (sqlite3CompareAffinity(pRight, zAff[j]) ==
				    AFFINITY_BLOB) {
					zAff[j] = AFFINITY_BLOB;
				}
				if (sqlite3ExprNeedsNoAffinityChange
				    (pRight, zAff[j])) {
					zAff[j] = AFFINITY_BLOB;
				}
			}
		}
	}
	*pzAff = zAff;
	return regBase;
}

#ifndef SQLITE_LIKE_DOESNT_MATCH_BLOBS
/*
 * If the most recently coded instruction is a constant range constraint
 * (a string literal) that originated from the LIKE optimization, then
 * set P3 and P5 on the OP_String opcode so that the string will be cast
 * to a BLOB at appropriate times.
 *
 * The LIKE optimization trys to evaluate "x LIKE 'abc%'" as a range
 * expression: "x>='ABC' AND x<'abd'".  But this requires that the range
 * scan loop run twice, once for strings and a second time for BLOBs.
 * The OP_String opcodes on the second pass convert the upper and lower
 * bound string constants to blobs.  This routine makes the necessary changes
 * to the OP_String opcodes for that to happen.
 *
 * Except, of course, if SQLITE_LIKE_DOESNT_MATCH_BLOBS is defined, then
 * only the one pass through the string space is required, so this routine
 * becomes a no-op.
 */
static void
whereLikeOptimizationStringFixup(Vdbe * v,		/* prepared statement under construction */
				 WhereLevel * pLevel,	/* The loop that contains the LIKE operator */
				 WhereTerm * pTerm)	/* The upper or lower bound just coded */
{
	if (pTerm->wtFlags & TERM_LIKEOPT) {
		VdbeOp *pOp;
		assert(pLevel->iLikeRepCntr > 0);
		pOp = sqlite3VdbeGetOp(v, -1);
		assert(pOp != 0);
		assert(pOp->opcode == OP_String8
		       || pTerm->pWC->pWInfo->pParse->db->mallocFailed);
		pOp->p3 = (int)(pLevel->iLikeRepCntr >> 1);	/* Register holding counter */
		pOp->p5 = (u8) (pLevel->iLikeRepCntr & 1);	/* ASC or DESC */
	}
}
#else
#define whereLikeOptimizationStringFixup(A,B,C)
#endif

/*
 * If the expression passed as the second argument is a vector, generate
 * code to write the first nReg elements of the vector into an array
 * of registers starting with iReg.
 *
 * If the expression is not a vector, then nReg must be passed 1. In
 * this case, generate code to evaluate the expression and leave the
 * result in register iReg.
 */
static void
codeExprOrVector(Parse * pParse, Expr * p, int iReg, int nReg)
{
	assert(nReg > 0);
	if (sqlite3ExprIsVector(p)) {
		if ((p->flags & EP_xIsSelect)) {
			Vdbe *v = pParse->pVdbe;
			int iSelect = sqlite3CodeSubselect(pParse, p, 0);
			sqlite3VdbeAddOp3(v, OP_Copy, iSelect, iReg, nReg - 1);
		} else {
			int i;
			ExprList *pList = p->x.pList;
			assert(nReg <= pList->nExpr);
			for (i = 0; i < nReg; i++) {
				sqlite3ExprCode(pParse, pList->a[i].pExpr,
						iReg + i);
			}
		}
	} else {
		assert(nReg == 1);
		sqlite3ExprCode(pParse, p, iReg);
	}
}


uint64_t
sql_where_code_one_loop(struct WhereInfo *where_info, int level,
			uint64_t not_ready_mask)
{
	int j, k;		/* Loop counters */
	int addrNxt;		/* Where to jump to continue with the next IN case */
	int omitTable;		/* True if we use the index only */
	WhereTerm *pTerm;	/* A WHERE clause term */
	int addrBrk;		/* Jump here to break out of the loop */
	int addrCont;		/* Jump here to continue with next cycle */

	/* Parsing context. */
	struct Parse *parser = where_info->pParse;
	/* The prepared stmt under constructions. */
	struct Vdbe *v = parser->pVdbe;
	/* Decomposition of the entire WHERE clause. */
	struct WhereClause *where_clause = &where_info->sWC;
	/* Database connection. */
	struct sqlite3 *db = parser->db;
	/* The where level to be coded. */
	struct WhereLevel *where_level = &where_info->a[level];
	/* The WhereLoop object being coded. */
	struct WhereLoop *where_loop = where_level->pWLoop;
	/* FROM clause term being coded. */
	struct SrcList_item *src_list_table =
		&where_info->pTabList->a[where_level->iFrom];
	/* The VDBE cursor for the table. */
	int cursor = src_list_table->iCursor;
	where_level->not_ready_mask =
	    not_ready_mask & ~sql_where_get_mask(&where_info->sMaskSet, cursor);
	/* True if we need to scan in reverse order. */
	int is_reversed = (where_info->rev_mask >> level) & 1;
	omitTable = (where_loop->wsFlags & WHERE_IDX_ONLY) != 0 &&
		    (where_info->wctrlFlags & WHERE_OR_SUBCLAUSE) == 0;
	VdbeModuleComment((v, "Begin WHERE-loop%d: %s", level,
			   src_list_table->pTab->zName));

	/* Create labels for the "break" and "continue" instructions
	 * for the current loop.  Jump to addrBrk to break out of a loop.
	 * Jump to cont to go immediately to the next iteration of the
	 * loop.
	 *
	 * When there is an IN operator, we also have a "addrNxt" label that
	 * means to continue with the next IN value combination.  When
	 * there are no IN operators in the constraints, the "addrNxt" label
	 * is the same as "addrBrk".
	 */
	addrBrk = where_level->addrBrk = where_level->addrNxt =
		sqlite3VdbeMakeLabel(v);
	addrCont = where_level->addrCont = sqlite3VdbeMakeLabel(v);

	/* If this is the right table of a LEFT OUTER JOIN, allocate and
	 * initialize a memory cell that records if this table matches any
	 * row of the left table of the join.
	 */
	if (where_level->iFrom > 0 &&
	   (src_list_table[0].fg.jointype & JT_LEFT) != 0) {
		where_level->iLeftJoin = ++parser->nMem;
		sqlite3VdbeAddOp2(v, OP_Integer, 0, where_level->iLeftJoin);
		VdbeComment((v, "init LEFT JOIN no-match flag"));
	}

	/* Special case of a FROM clause subquery implemented as a co-routine */
	if (src_list_table->fg.viaCoroutine) {
		int regYield = src_list_table->regReturn;
		sqlite3VdbeAddOp3(v, OP_InitCoroutine, regYield, 0,
				  src_list_table->addrFillSub);
		where_level->p2 =
			sqlite3VdbeAddOp2(v, OP_Yield, regYield, addrBrk);
		VdbeCoverage(v);
		VdbeComment((v, "next row of \"%s\"",
			    src_list_table->pTab->def->name));
		where_level->op = OP_Goto;
	} else if (where_loop->wsFlags & WHERE_INDEXED) {
		/* Case 4: A scan using an index.
		 *
		 *         The WHERE clause may contain zero or more equality
		 *         terms ("==" or "IN" operators) that refer to the N
		 *         left-most columns of the index. It may also contain
		 *         inequality constraints (>, <, >= or <=) on the indexed
		 *         column that immediately follows the N equalities. Only
		 *         the right-most column can be an inequality - the rest must
		 *         use the "==" and "IN" operators. For example, if the
		 *         index is on (x,y,z), then the following clauses are all
		 *         optimized:
		 *
		 *            x=5
		 *            x=5 AND y=10
		 *            x=5 AND y<10
		 *            x=5 AND y>5 AND y<10
		 *            x=5 AND y=5 AND z<=10
		 *
		 *         The z<10 term of the following cannot be used, only
		 *         the x=5 term:
		 *
		 *            x=5 AND z<10
		 *
		 *         N may be zero if there are inequality constraints.
		 *         If there are no inequality constraints, then N is at
		 *         least one.
		 *
		 *         This case is also used when there are no WHERE clause
		 *         constraints but an index is selected anyway, in order
		 *         to force the output order to conform to an ORDER BY.
		 */
		static const u8 aStartOp[] = {
			0,
			0,
			OP_Rewind,	/* 2: (!start_constraints && startEq &&  !is_reversed) */
			OP_Last,	/* 3: (!start_constraints && startEq &&   is_reversed) */
			OP_SeekGT,	/* 4: (start_constraints  && !startEq && !is_reversed) */
			OP_SeekLT,	/* 5: (start_constraints  && !startEq &&  is_reversed) */
			OP_SeekGE,	/* 6: (start_constraints  &&  startEq && !is_reversed) */
			OP_SeekLE	/* 7: (start_constraints  &&  startEq &&  is_reversed) */
		};
		static const u8 aEndOp[] = {
			OP_IdxGE,	/* 0: (end_constraints && !is_reversed && !endEq) */
			OP_IdxGT,	/* 1: (end_constraints && !is_reversed &&  endEq) */
			OP_IdxLE,	/* 2: (end_constraints &&  is_reversed && !endEq) */
			OP_IdxLT,	/* 3: (end_constraints &&  is_reversed &&  endEq) */
		};
		/* Number of == or IN terms. */
		u16 eq_cnt = where_loop->nEq;
		/* Length of BTM vector */
		u16 btm_cnt = where_loop->nBtm;
		/* Length of TOP vector */
		u16 top_cnt = where_loop->nTop;
		int regBase;	/* Base register holding constraint values */
		WhereTerm *pRangeStart = 0;	/* Inequality constraint at range start */
		WhereTerm *pRangeEnd = 0;	/* Inequality constraint at range end */
		int startEq;	/* True if range start uses ==, >= or <= */
		int endEq;	/* True if range end uses ==, >= or <= */
		int start_constraints;	/* Start of range is constrained */
		int nConstraint;	/* Number of constraint terms */
		int iIdxCur;	/* The VDBE cursor for the index */
		int nExtraReg = 0;	/* Number of extra registers needed */
		int op;		/* Instruction opcode */
		char *zStartAff;	/* Affinity for start of range constraint */
		char *zEndAff = 0;	/* Affinity for end of range constraint */
		u8 bSeekPastNull = 0;	/* True to seek past initial nulls */
		u8 bStopAtNull = 0;	/* Add condition to terminate at NULLs */
		int force_integer_reg = -1;  /* If non-negative: number of
					      * column which must be converted
					      * to integer type, used for IPK.
					      */

		struct Index *index = where_loop->pIndex;
		struct index_def *idx_def = where_loop->index_def;
		assert(index != NULL || idx_def != NULL);
		iIdxCur = where_level->iIdxCur;
		assert(eq_cnt >= where_loop->nSkip);

		/* If this loop satisfies a sort order (pOrderBy) request that
		 * was passed to this function to implement a "SELECT min(x) ..."
		 * query, then the caller will only allow the loop to run for
		 * a single iteration. This means that the first row returned
		 * should not have a NULL value stored in 'x'. If column 'x' is
		 * the first one after the eq_cnt equality constraints in the index,
		 * this requires some special handling.
		 */
		assert(where_info->pOrderBy == 0 ||
		       where_info->pOrderBy->nExpr == 1 ||
		       (where_info->wctrlFlags & WHERE_ORDERBY_MIN) == 0);
		uint32_t part_count;
		if (index != NULL)
			part_count = index->def->key_def->part_count;
		else
			part_count = idx_def->key_def->part_count;
		if ((where_info->wctrlFlags & WHERE_ORDERBY_MIN) != 0 &&
		    where_info->nOBSat > 0 && part_count > eq_cnt) {
			j = index->def->key_def->parts[eq_cnt].fieldno;
			/* Allow seek for column with `NOT NULL` == false attribute.
			 * If a column may contain NULL-s, the comparator installed
			 * by Tarantool is prepared to seek using a NULL value.
			 * Otherwise, the seek will ultimately fail. Fortunately,
			 * if the column MUST NOT contain NULL-s, it suffices to
			 * fetch the very first/last value to obtain min/max.
			 *
			 * FYI: entries in an index are ordered as follows:
			 *      NULL, ... NULL, min_value, ...
			 */
			if (index->pTable->def->fields[j].is_nullable) {
				assert(where_loop->nSkip == 0);
				bSeekPastNull = 1;
				nExtraReg = 1;
			}
		}

		/* Find any inequality constraint terms for the start and end
		 * of the range.
		 */
		j = eq_cnt;
		if (where_loop->wsFlags & WHERE_BTM_LIMIT) {
			pRangeStart = where_loop->aLTerm[j++];
			nExtraReg = MAX(nExtraReg, where_loop->nBtm);
			/* Like optimization range constraints always occur in pairs */
			assert((pRangeStart->wtFlags & TERM_LIKEOPT) == 0 ||
			       (where_loop->wsFlags & WHERE_TOP_LIMIT) != 0);
		}
		if (where_loop->wsFlags & WHERE_TOP_LIMIT) {
			pRangeEnd = where_loop->aLTerm[j++];
			nExtraReg = MAX(nExtraReg, where_loop->nTop);
#ifndef SQLITE_LIKE_DOESNT_MATCH_BLOBS
			if ((pRangeEnd->wtFlags & TERM_LIKEOPT) != 0) {
				assert(pRangeStart != 0);	/* LIKE opt constraints */
				assert(pRangeStart->wtFlags & TERM_LIKEOPT);	/* occur in pairs */
				where_level->iLikeRepCntr = (u32)++ parser->nMem;
				sqlite3VdbeAddOp2(v, OP_Integer, 1,
						  (int)where_level->iLikeRepCntr);
				VdbeComment((v, "LIKE loop counter"));
				where_level->addrLikeRep = sqlite3VdbeCurrentAddr(v);
				/* iLikeRepCntr actually stores 2x the counter register number.  The
				 * bottom bit indicates whether the search order is ASC or DESC.
				 */
				testcase(index->aSortOrder[eq_cnt] ==
					 SORT_ORDER_DESC);
				assert((is_reversed & ~1) == 0);
				struct key_def *def = index->def->key_def;
				where_level->iLikeRepCntr <<= 1;
				where_level->iLikeRepCntr |=
					is_reversed ^
					(def->parts[eq_cnt].sort_order ==
					SORT_ORDER_DESC);
			}
#endif
			if (pRangeStart == 0) {
				j = index->def->key_def->parts[eq_cnt].fieldno;
				if (index->pTable->def->fields[j].is_nullable)
					bSeekPastNull = 1;
			}
		}
		assert(pRangeEnd == 0
		       || (pRangeEnd->wtFlags & TERM_VNULL) == 0);

		/* If we are doing a reverse order scan on an ascending index, or
		 * a forward order scan on a descending index, interchange the
		 * start and end terms (pRangeStart and pRangeEnd).
		 */
		if ((eq_cnt < part_count && is_reversed ==
		    (index->def->key_def->parts[eq_cnt].sort_order ==
		    SORT_ORDER_ASC)) || (is_reversed && part_count == eq_cnt)) {
			SWAP(pRangeEnd, pRangeStart);
			SWAP(bSeekPastNull, bStopAtNull);
			SWAP(btm_cnt, top_cnt);
		}

		/* Generate code to evaluate all constraint terms using == or IN
		 * and store the values of those terms in an array of registers
		 * starting at regBase.
		 */
		regBase =
		    codeAllEqualityTerms(parser, where_level, is_reversed,
					 nExtraReg, &zStartAff);
		assert(zStartAff == 0 || sqlite3Strlen30(zStartAff) >= eq_cnt);
		if (zStartAff && top_cnt)
			zEndAff = sqlite3DbStrDup(db, &zStartAff[eq_cnt]);
		addrNxt = where_level->addrNxt;

		testcase(pRangeStart && (pRangeStart->eOperator & WO_LE) != 0);
		testcase(pRangeStart && (pRangeStart->eOperator & WO_GE) != 0);
		testcase(pRangeEnd && (pRangeEnd->eOperator & WO_LE) != 0);
		testcase(pRangeEnd && (pRangeEnd->eOperator & WO_GE) != 0);
		startEq = !pRangeStart
		    || pRangeStart->eOperator & (WO_LE | WO_GE);
		endEq = !pRangeEnd || pRangeEnd->eOperator & (WO_LE | WO_GE);
		start_constraints = pRangeStart || eq_cnt > 0;

		/* Seek the index cursor to the start of the range. */
		nConstraint = eq_cnt;
		if (pRangeStart) {
			Expr *pRight = pRangeStart->pExpr->pRight;
			codeExprOrVector(parser, pRight, regBase + eq_cnt, btm_cnt);

			whereLikeOptimizationStringFixup(v, where_level,
							 pRangeStart);
			if ((pRangeStart->wtFlags & TERM_VNULL) == 0
			    && sqlite3ExprCanBeNull(pRight)) {
				sqlite3VdbeAddOp2(v, OP_IsNull, regBase + eq_cnt,
						  addrNxt);
				VdbeCoverage(v);
			}

			if (zStartAff) {
				updateRangeAffinityStr(pRight, btm_cnt,
						       &zStartAff[eq_cnt]);
			}
			nConstraint += btm_cnt;
			testcase(pRangeStart->wtFlags & TERM_VIRTUAL);
			if (sqlite3ExprIsVector(pRight) == 0) {
				disableTerm(where_level, pRangeStart);
			} else {
				startEq = 1;
			}
			bSeekPastNull = 0;
		} else if (bSeekPastNull) {
			sqlite3VdbeAddOp2(v, OP_Null, 0, regBase + eq_cnt);
			nConstraint++;
			startEq = 0;
			start_constraints = 1;
		}
		struct Index *pk = NULL;
		struct index_def *idx_pk = NULL;
		char affinity;
		if (index == NULL) {
			struct space *space = space_cache_find(idx_def->space_id);
			assert(space != NULL);
			idx_pk = space->index[0]->def;
			int fieldno = idx_pk->key_def->parts[0].fieldno;
			affinity = space->def->fields[fieldno].affinity;
			if (affinity == AFFINITY_UNDEFINED) {
				if (idx_pk->key_def->part_count == 1 &&
				    space->def->fields[fieldno].type ==
				    FIELD_TYPE_INTEGER)
					affinity = AFFINITY_INTEGER;
				else
					affinity = AFFINITY_BLOB;
			}
		} else {
			pk = sqlite3PrimaryKeyIndex(index->pTable);
			uint32_t fieldno = pk->def->key_def->parts[0].fieldno;
			affinity = index->pTable->def->fields[fieldno].affinity;
		}

		uint32_t pk_part_count;
		if (pk != NULL)
			pk_part_count = pk->def->key_def->part_count;
		else
			pk_part_count = idx_pk->key_def->part_count;
		if (pk_part_count == 1 && affinity == AFFINITY_INTEGER) {
			/* Right now INTEGER PRIMARY KEY is the only option to
			 * get Tarantool's INTEGER column type. Need special handling
			 * here: try to loosely convert FLOAT to INT. If RHS type
			 * is not INT or FLOAT - skip this ites, i.e. goto addrNxt.
			 */
			int limit = pRangeStart == NULL ? eq_cnt : eq_cnt + 1;
			for (int i = 0; i < limit; i++) {
				if ((index != NULL &&
				     index->def->key_def->parts[i].fieldno ==
				     pk->def->key_def->parts[0].fieldno) ||
				    (idx_pk != NULL &&
				     idx_def->key_def->parts[i].fieldno ==
				     idx_pk->key_def->parts[0].fieldno)) {
					/* Here: we know for sure that table has INTEGER
					   PRIMARY KEY, single column, and Index we're
					   trying to use for scan contains this column. */
					if (i < eq_cnt)
						sqlite3VdbeAddOp2(v, OP_MustBeInt, regBase + i, addrNxt);
					else
						force_integer_reg = regBase + i;
					break;
				}
			}
		}
		codeApplyAffinity(parser, regBase, nConstraint - bSeekPastNull,
				  zStartAff);
		if (where_loop->nSkip > 0 && nConstraint == where_loop->nSkip) {
			/* The skip-scan logic inside the call to codeAllEqualityConstraints()
			 * above has already left the cursor sitting on the correct row,
			 * so no further seeking is needed
			 */
		} else {
			op = aStartOp[(start_constraints << 2) +
				      (startEq << 1) + is_reversed];
			assert(op != 0);
			sqlite3VdbeAddOp4Int(v, op, iIdxCur, addrNxt, regBase,
					     nConstraint);
			/* If this is Seek* opcode, and IPK is detected in the
			 * constraints vector: force it to be integer.
			 */
			if ((op == OP_SeekGE || op == OP_SeekGT
			    || op == OP_SeekLE || op == OP_SeekLT)
			    && force_integer_reg > 0) {
				sqlite3VdbeChangeP5(v, force_integer_reg);
			}
			VdbeCoverage(v);
			VdbeCoverageIf(v, op == OP_Rewind);
			testcase(op == OP_Rewind);
			VdbeCoverageIf(v, op == OP_Last);
			testcase(op == OP_Last);
			VdbeCoverageIf(v, op == OP_SeekGT);
			testcase(op == OP_SeekGT);
			VdbeCoverageIf(v, op == OP_SeekGE);
			testcase(op == OP_SeekGE);
			VdbeCoverageIf(v, op == OP_SeekLE);
			testcase(op == OP_SeekLE);
			VdbeCoverageIf(v, op == OP_SeekLT);
			testcase(op == OP_SeekLT);
		}

		/* Load the value for the inequality constraint at the end of the
		 * range (if any).
		 */
		nConstraint = eq_cnt;
		if (pRangeEnd) {
			Expr *pRight = pRangeEnd->pExpr->pRight;
			sqlite3ExprCacheRemove(parser, regBase + eq_cnt, 1);
			codeExprOrVector(parser, pRight, regBase + eq_cnt, top_cnt);
			whereLikeOptimizationStringFixup(v, where_level,
							 pRangeEnd);
			if ((pRangeEnd->wtFlags & TERM_VNULL) == 0
			    && sqlite3ExprCanBeNull(pRight)) {
				sqlite3VdbeAddOp2(v, OP_IsNull, regBase + eq_cnt,
						  addrNxt);
				VdbeCoverage(v);
			}
			if (zEndAff) {
				updateRangeAffinityStr(pRight, top_cnt, zEndAff);
				codeApplyAffinity(parser, regBase + eq_cnt, top_cnt,
						  zEndAff);
			} else {
				assert(parser->db->mallocFailed);
			}
			nConstraint += top_cnt;
			testcase(pRangeEnd->wtFlags & TERM_VIRTUAL);

			if (sqlite3ExprIsVector(pRight) == 0) {
				disableTerm(where_level, pRangeEnd);
			} else {
				endEq = 1;
			}
		} else if (bStopAtNull) {
			sqlite3VdbeAddOp2(v, OP_Null, 0, regBase + eq_cnt);
			endEq = 0;
			nConstraint++;
		}
		sqlite3DbFree(db, zStartAff);
		sqlite3DbFree(db, zEndAff);

		/* Top of the loop body */
		where_level->p2 = sqlite3VdbeCurrentAddr(v);

		/* Check if the index cursor is past the end of the range. */
		if (nConstraint) {
			op = aEndOp[is_reversed * 2 + endEq];
			sqlite3VdbeAddOp4Int(v, op, iIdxCur, addrNxt, regBase,
					     nConstraint);
			testcase(op == OP_IdxGT);
			VdbeCoverageIf(v, op == OP_IdxGT);
			testcase(op == OP_IdxGE);
			VdbeCoverageIf(v, op == OP_IdxGE);
			testcase(op == OP_IdxLT);
			VdbeCoverageIf(v, op == OP_IdxLT);
			testcase(op == OP_IdxLE);
			VdbeCoverageIf(v, op == OP_IdxLE);
		}

		/* Seek the table cursor, if required */
		if (omitTable) {
			/*
			 * Index is a covering index.
			 * No need to access the main table.
			 */
		}  else if (cursor != iIdxCur) {
			Index *pPk = sqlite3PrimaryKeyIndex(index->pTable);
			int pk_part_count = pPk->def->key_def->part_count;
			int iKeyReg = sqlite3GetTempRange(parser, pk_part_count);
			for (j = 0; j < pk_part_count; j++) {
				k = pPk->def->key_def->parts[j].fieldno;
				sqlite3VdbeAddOp3(v, OP_Column, iIdxCur, k,
						  iKeyReg + j);
			}
			sqlite3VdbeAddOp4Int(v, OP_NotFound, cursor, addrCont,
					     iKeyReg, pk_part_count);
			VdbeCoverage(v);
			sqlite3ReleaseTempRange(parser, iKeyReg, pk_part_count);
		}

		/* Record the instruction used to terminate the loop. */
		if (where_loop->wsFlags & WHERE_ONEROW) {
			where_level->op = OP_Noop;
		} else if (is_reversed) {
			where_level->op = OP_Prev;
		} else {
			where_level->op = OP_Next;
		}
		where_level->p1 = iIdxCur;
		where_level->p3 =
			(where_loop->wsFlags & WHERE_UNQ_WANTED) != 0 ? 1 : 0;
		if ((where_loop->wsFlags & WHERE_CONSTRAINT) == 0) {
			where_level->p5 = SQLITE_STMTSTATUS_FULLSCAN_STEP;
		} else {
			assert(where_level->p5 == 0);
		}
	} else
#ifndef SQLITE_OMIT_OR_OPTIMIZATION
	if (where_loop->wsFlags & WHERE_MULTI_OR) {
		/* Case 5:  Two or more separately indexed terms connected by OR
		 *
		 * Example:
		 *
		 *   CREATE TABLE t1(a,b,c,d);
		 *   CREATE INDEX i1 ON t1(a);
		 *   CREATE INDEX i2 ON t1(b);
		 *   CREATE INDEX i3 ON t1(c);
		 *
		 *   SELECT * FROM t1 WHERE a=5 OR b=7 OR (c=11 AND d=13)
		 *
		 * In the example, there are three indexed terms connected by OR.
		 * In this case, use an ephemeral index to record the primary
		 * keys of the rows we have already seen.
		 */
		WhereClause *pOrWc;	/* The OR-clause broken out into subterms */
		SrcList *pOrTab;	/* Shortened table list or OR-clause generation */
		Index *pCov = 0;	/* Potential covering index (or NULL) */
		/* Cursor used for index scans (if any). */
		int iCovCur = parser->nTab++;

		/* Register used with OP_Gosub. */
		int return_reg = ++parser->nMem;
		int regRowset = 0;	/* Register for RowSet object */
		int regPk = 0;	/* Register holding PK */
		int iLoopBody = sqlite3VdbeMakeLabel(v);	/* Start of loop body */
		int iRetInit;	/* Address of return_reg init */
		int untestedTerms = 0;	/* Some terms not completely tested */
		int ii;		/* Loop counter */
		u16 wctrlFlags;	/* Flags for sub-WHERE clause */
		Expr *pAndExpr = 0;	/* An ".. AND (...)" expression */
		Table *pTab = src_list_table->pTab;

		pTerm = where_loop->aLTerm[0];
		assert(pTerm != 0);
		assert(pTerm->eOperator & WO_OR);
		assert((pTerm->wtFlags & TERM_ORINFO) != 0);
		pOrWc = &pTerm->u.pOrInfo->wc;
		where_level->op = OP_Return;
		where_level->p1 = return_reg;

		/* Set up a new SrcList in pOrTab containing the table being scanned
		 * by this loop in the a[0] slot and all not_ready_mask tables in a[1..] slots.
		 * This becomes the SrcList in the recursive call to sqlite3WhereBegin().
		 */
		if (where_info->nLevel > 1) {
			int nNotReady;	/* The number of not_ready_mask tables */
			struct SrcList_item *origSrc;	/* Original list of tables */
			nNotReady = where_info->nLevel - level - 1;
			pOrTab = sqlite3StackAllocRaw(db,
						      sizeof(*pOrTab) +
						      nNotReady *
						      sizeof(pOrTab->a[0]));
			if (pOrTab == 0)
				return not_ready_mask;
			pOrTab->nAlloc = (u8) (nNotReady + 1);
			pOrTab->nSrc = pOrTab->nAlloc;
			memcpy(pOrTab->a, src_list_table, sizeof(*src_list_table));
			origSrc = where_info->pTabList->a;
			for (k = 1; k <= nNotReady; k++) {
				memcpy(&pOrTab->a[k],
					&origSrc[where_level[k].iFrom],
				       sizeof(pOrTab->a[k]));
			}
		} else {
			pOrTab = where_info->pTabList;
		}

		/* Create an ephemeral index capable of holding primary keys.
		 *
		 * Also initialize return_reg to contain the address of the instruction
		 * immediately following the OP_Return at the bottom of the loop. This
		 * is required in a few obscure LEFT JOIN cases where control jumps
		 * over the top of the loop into the body of it. In this case the
		 * correct response for the end-of-loop code (the OP_Return) is to
		 * fall through to the next instruction, just as an OP_Next does if
		 * called on an uninitialized cursor.
		 */
		if ((where_info->wctrlFlags & WHERE_DUPLICATES_OK) == 0) {
			Index *pPk = sqlite3PrimaryKeyIndex(pTab);
			int pk_part_count = pPk->def->key_def->part_count;
			regRowset = parser->nTab++;
			sqlite3VdbeAddOp2(v, OP_OpenTEphemeral,
					  regRowset, pk_part_count);
			sql_vdbe_set_p4_key_def(parser, pPk);
			regPk = ++parser->nMem;
		}
		iRetInit = sqlite3VdbeAddOp2(v, OP_Integer, 0, return_reg);

		/* If the original WHERE clause is z of the form:  (x1 OR x2 OR ...) AND y
		 * Then for every term xN, evaluate as the subexpression: xN AND z
		 * That way, terms in y that are factored into the disjunction will
		 * be picked up by the recursive calls to sqlite3WhereBegin() below.
		 *
		 * Actually, each subexpression is converted to "xN AND w" where w is
		 * the "interesting" terms of z - terms that did not originate in the
		 * ON or USING clause of a LEFT JOIN, and terms that are usable as
		 * indices.
		 *
		 * This optimization also only applies if the (x1 OR x2 OR ...) term
		 * is not contained in the ON clause of a LEFT JOIN.
		 * See ticket http://www.sqlite.org/src/info/f2369304e4
		 */
		if (where_clause->nTerm > 1) {
			int iTerm;
			for (iTerm = 0; iTerm < where_clause->nTerm; iTerm++) {
				Expr *pExpr = where_clause->a[iTerm].pExpr;
				if (&where_clause->a[iTerm] == pTerm)
					continue;
				if (ExprHasProperty(pExpr, EP_FromJoin))
					continue;
				testcase(where_clause->a[iTerm].wtFlags & TERM_VIRTUAL);
				testcase(where_clause->a[iTerm].wtFlags & TERM_CODED);
				if ((where_clause->a[iTerm].
				     wtFlags & (TERM_VIRTUAL | TERM_CODED)) !=
				    0)
					continue;
				if ((where_clause->a[iTerm].eOperator & WO_ALL) == 0)
					continue;
				testcase(where_clause->a[iTerm].wtFlags & TERM_ORINFO);
				pExpr = sqlite3ExprDup(db, pExpr, 0);
				pAndExpr = sqlite3ExprAnd(db, pAndExpr, pExpr);
			}
			if (pAndExpr) {
				pAndExpr =
				    sqlite3PExpr(parser,
						 TK_AND | TKFLG_DONTFOLD, 0,
						 pAndExpr);
			}
		}

		/* Run a separate WHERE clause for each term of the OR clause.  After
		 * eliminating duplicates from other WHERE clauses, the action for each
		 * sub-WHERE clause is to to invoke the main loop body as a subroutine.
		 */
		wctrlFlags =
		    WHERE_OR_SUBCLAUSE | (where_info->
					  wctrlFlags & WHERE_SEEK_TABLE);
		for (ii = 0; ii < pOrWc->nTerm; ii++) {
			WhereTerm *pOrTerm = &pOrWc->a[ii];
			if (pOrTerm->leftCursor == cursor
			    || (pOrTerm->eOperator & WO_AND) != 0) {
				WhereInfo *pSubWInfo;	/* Info for single OR-term scan */
				Expr *pOrExpr = pOrTerm->pExpr;	/* Current OR clause term */
				int jmp1 = 0;	/* Address of jump operation */
				if (pAndExpr
				    && !ExprHasProperty(pOrExpr, EP_FromJoin)) {
					pAndExpr->pLeft = pOrExpr;
					pOrExpr = pAndExpr;
				}
				/* Loop through table entries that match term pOrTerm. */
				WHERETRACE(0xffff,
					   ("Subplan for OR-clause:\n"));
				pSubWInfo =
				    sqlite3WhereBegin(parser, pOrTab, pOrExpr,
						      0, 0, wctrlFlags,
						      iCovCur);
				assert(pSubWInfo || parser->nErr
				       || db->mallocFailed);
				if (pSubWInfo) {
					WhereLoop *pSubLoop;
					int addrExplain =
					    sqlite3WhereExplainOneScan(parser,
								       pOrTab,
								       &pSubWInfo->
								       a[0],
								       level,
								       where_level->
								       iFrom,
								       0);
					sqlite3WhereAddScanStatus(v, pOrTab,
								  &pSubWInfo->a[0],
								  addrExplain);

					/* This is the sub-WHERE clause body.  First skip over
					 * duplicate rows from prior sub-WHERE clauses, and record the
					 * PRIMARY KEY for the current row so that the same
					 * row will be skipped in subsequent sub-WHERE clauses.
					 */
					if ((where_info->
					     wctrlFlags & WHERE_DUPLICATES_OK)
					    == 0) {
						int r;
						int iSet =
						    ((ii == pOrWc->nTerm - 1) ? -1 : ii);
						Index *pPk = sqlite3PrimaryKeyIndex (pTab);
						struct key_def *def =
							pPk->def->key_def;

						/* Read the PK into an array of temp registers. */
						r = sqlite3GetTempRange(parser,
									def->part_count);
						for (uint32_t iPk = 0;
						     iPk < def->part_count;
						     iPk++) {
							uint32_t fieldno =
								def->parts[iPk].
								fieldno;
							sqlite3ExprCodeGetColumnToReg
								(parser,
								 pTab->def,
								 fieldno,
								 cursor,
								 r + iPk);
						}

						/* Check if the temp table already contains this key. If so,
						 * the row has already been included in the result set and
						 * can be ignored (by jumping past the Gosub below). Otherwise,
						 * insert the key into the temp table and proceed with processing
						 * the row.
						 *
						 * Use optimizations: If iSet
						 * is zero, assume that the key cannot already be present in
						 * the temp table. And if iSet is -1, assume that there is no
						 * need to insert the key into the temp table, as it will never
						 * be tested for.
						 */
						if (iSet) {
							jmp1 = sqlite3VdbeAddOp4Int
								(v, OP_Found,
								 regRowset, 0,
								 r,
								 def->part_count);
							VdbeCoverage(v);
						}
						if (iSet >= 0) {
							sqlite3VdbeAddOp3
								(v, OP_MakeRecord,
								 r, def->part_count, regPk);
							sqlite3VdbeAddOp2
								(v, OP_IdxInsert,
								 regRowset, regPk);
						}

						/* Release the array of temp registers */
						sqlite3ReleaseTempRange(parser, r, def->part_count);
					}

					/* Invoke the main loop body as a subroutine */
					sqlite3VdbeAddOp2(v, OP_Gosub,
							  return_reg, iLoopBody);

					/* Jump here (skipping the main loop body subroutine) if the
					 * current sub-WHERE row is a duplicate from prior sub-WHEREs.
					 */
					if (jmp1)
						sqlite3VdbeJumpHere(v, jmp1);

					/* The pSubWInfo->untestedTerms flag means that this OR term
					 * contained one or more AND term from a not_ready_mask table.  The
					 * terms from the not_ready_mask table could not be tested and will
					 * need to be tested later.
					 */
					if (pSubWInfo->untestedTerms)
						untestedTerms = 1;

					/* If all of the OR-connected terms are optimized using the same
					 * index, and the index is opened using the same cursor number
					 * by each call to sqlite3WhereBegin() made by this loop, it may
					 * be possible to use that index as a covering index.
					 *
					 * If the call to sqlite3WhereBegin() above resulted in a scan that
					 * uses an index, and this is either the first OR-connected term
					 * processed or the index is the same as that used by all previous
					 * terms, set pCov to the candidate covering index. Otherwise, set
					 * pCov to NULL to indicate that no candidate covering index will
					 * be available.
					 */
					pSubLoop = pSubWInfo->a[0].pWLoop;
					assert((pSubLoop->wsFlags & WHERE_AUTO_INDEX) == 0);
					if ((pSubLoop->wsFlags & WHERE_INDEXED) != 0
					    && (ii == 0 || pSubLoop->pIndex == pCov)
					    && !sql_index_is_primary(pSubLoop->pIndex)) {
						assert(pSubWInfo->a[0].
						       iIdxCur == iCovCur);
						pCov = pSubLoop->pIndex;
					} else {
						pCov = 0;
					}

					/* Finish the loop through table entries that match term pOrTerm. */
					sqlite3WhereEnd(pSubWInfo);
				}
			}
		}
		where_level->u.pCovidx = pCov;
		if (pCov)
			where_level->iIdxCur = iCovCur;
		if (pAndExpr) {
			pAndExpr->pLeft = 0;
			sql_expr_delete(db, pAndExpr, false);
		}
		sqlite3VdbeChangeP1(v, iRetInit, sqlite3VdbeCurrentAddr(v));
		sqlite3VdbeGoto(v, where_level->addrBrk);
		sqlite3VdbeResolveLabel(v, iLoopBody);

		if (where_info->nLevel > 1)
			sqlite3StackFree(db, pOrTab);
		if (!untestedTerms)
			disableTerm(where_level, pTerm);
	} else
#endif				/* SQLITE_OMIT_OR_OPTIMIZATION */

	{
		/* Case 6:  There is no usable index.  We must do a complete
		 *          scan of the entire table.
		 */
		static const u8 aStep[] = { OP_Next, OP_Prev };
		static const u8 aStart[] = { OP_Rewind, OP_Last };
		assert(is_reversed == 0 || is_reversed == 1);
		if (src_list_table->fg.isRecursive) {
			/* Tables marked isRecursive have only a single row that is stored in
			 * a pseudo-cursor.  No need to Rewind or Next such cursors.
			 */
			where_level->op = OP_Noop;
		} else {
			where_level->op = aStep[is_reversed];
			where_level->p1 = cursor;
			where_level->p2 =
			    1 + sqlite3VdbeAddOp2(v, aStart[is_reversed],
						  cursor, addrBrk);
			VdbeCoverageIf(v, is_reversed == 0);
			VdbeCoverageIf(v, is_reversed != 0);
			where_level->p5 = SQLITE_STMTSTATUS_FULLSCAN_STEP;
		}
	}

#ifdef SQLITE_ENABLE_STMT_SCANSTATUS
	pLevel->addrVisit = sqlite3VdbeCurrentAddr(v);
#endif

	/* Insert code to test every subexpression that can be completely
	 * computed using the current set of tables.
	 */
	for (pTerm = where_clause->a, j = where_clause->nTerm; j > 0;
	     j--, pTerm++) {
		Expr *pE;
		int skipLikeAddr = 0;
		testcase(pTerm->wtFlags & TERM_VIRTUAL);
		testcase(pTerm->wtFlags & TERM_CODED);
		if (pTerm->wtFlags & (TERM_VIRTUAL | TERM_CODED))
			continue;
		if ((pTerm->prereq_all_mask &
		    where_level->not_ready_mask) != 0) {
			where_info->untestedTerms = 1;
			continue;
		}
		pE = pTerm->pExpr;
		assert(pE != 0);
		if (where_level->iLeftJoin &&
		    !ExprHasProperty(pE, EP_FromJoin))
			continue;
		if (pTerm->wtFlags & TERM_LIKECOND) {
			/* If the TERM_LIKECOND flag is set, that means that the range search
			 * is sufficient to guarantee that the LIKE operator is true, so we
			 * can skip the call to the like(A,B) function.  But this only works
			 * for strings.  So do not skip the call to the function on the pass
			 * that compares BLOBs.
			 */
#ifdef SQLITE_LIKE_DOESNT_MATCH_BLOBS
			continue;
#else
			u32 x = where_level->iLikeRepCntr;
			assert(x > 0);
			skipLikeAddr =
			    sqlite3VdbeAddOp1(v, (x & 1) ? OP_IfNot : OP_If,
					      (int)(x >> 1));
			VdbeCoverage(v);
#endif
		}
		sqlite3ExprIfFalse(parser, pE, addrCont, SQLITE_JUMPIFNULL);
		if (skipLikeAddr)
			sqlite3VdbeJumpHere(v, skipLikeAddr);
		pTerm->wtFlags |= TERM_CODED;
	}

	/* Insert code to test for implied constraints based on transitivity
	 * of the "==" operator.
	 *
	 * Example: If the WHERE clause contains "t1.a=t2.b" and "t2.b=123"
	 * and we are coding the t1 loop and the t2 loop has not yet coded,
	 * then we cannot use the "t1.a=t2.b" constraint, but we can code
	 * the implied "t1.a=123" constraint.
	 */
	for (pTerm = where_clause->a, j = where_clause->nTerm; j > 0;
	     j--, pTerm++) {
		Expr *pE, sEAlt;
		WhereTerm *pAlt;
		if (pTerm->wtFlags & (TERM_VIRTUAL | TERM_CODED))
			continue;
		if ((pTerm->eOperator & WO_EQ) == 0)
			continue;
		if ((pTerm->eOperator & WO_EQUIV) == 0)
			continue;
		if (pTerm->leftCursor != cursor)
			continue;
		if (where_level->iLeftJoin)
			continue;
		pE = pTerm->pExpr;
		assert(!ExprHasProperty(pE, EP_FromJoin));
		assert((pTerm->prereq_right_mask &
			where_level->not_ready_mask) != 0);
		pAlt = sql_where_find_term(where_clause, cursor,
					   pTerm->u.leftColumn, not_ready_mask,
					   WO_EQ | WO_IN, 0);
		if (pAlt == 0)
			continue;
		if (pAlt->wtFlags & (TERM_CODED))
			continue;
		testcase(pAlt->eOperator & WO_EQ);
		testcase(pAlt->eOperator & WO_IN);
		VdbeModuleComment((v, "begin transitive constraint"));
		sEAlt = *pAlt->pExpr;
		sEAlt.pLeft = pE->pLeft;
		sqlite3ExprIfFalse(parser, &sEAlt, addrCont, SQLITE_JUMPIFNULL);
	}

	/* For a LEFT OUTER JOIN, generate code that will record the fact that
	 * at least one row of the right table has matched the left table.
	 */
	if (where_level->iLeftJoin != 0) {
		where_level->addrFirst = sqlite3VdbeCurrentAddr(v);
		sqlite3VdbeAddOp2(v, OP_Integer, 1, where_level->iLeftJoin);
		VdbeComment((v, "record LEFT JOIN hit"));
		sqlite3ExprCacheClear(parser);
		for (pTerm = where_clause->a, j = 0; j < where_clause->nTerm; j++, pTerm++) {
			testcase(pTerm->wtFlags & TERM_VIRTUAL);
			testcase(pTerm->wtFlags & TERM_CODED);
			if (pTerm->wtFlags & (TERM_VIRTUAL | TERM_CODED))
				continue;
			if ((pTerm->prereq_all_mask &
			    where_level->not_ready_mask) != 0) {
				assert(where_info->untestedTerms);
				continue;
			}
			assert(pTerm->pExpr);
			sqlite3ExprIfFalse(parser, pTerm->pExpr, addrCont,
					   SQLITE_JUMPIFNULL);
			pTerm->wtFlags |= TERM_CODED;
		}
	}

	return where_level->not_ready_mask;
}
