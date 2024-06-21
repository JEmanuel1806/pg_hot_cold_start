#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "utils/rel.h"

#include "access/relation.h"

#include "storage/bufmgr.h"
#include "storage/read_stream.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

// needed hooks
static ExecutorRun_hook_type pg_exec_run_prev_hook = NULL;
// Traverse query plan tree and get all relevant relations
static List *get_relations_from_plan(QueryDesc *queryDesc)
{
    // Plan *plan = queryDesc->plannedstmt->planTree;
    RangeTblEntry *rte;

    Relation relation = NULL;
    char *relName = NULL;
    Oid relOid;

    List *oidList = NIL;

    for (int i = 0; i < list_length(queryDesc->plannedstmt->rtable); i++)
    {
        rte = list_nth(queryDesc->plannedstmt->rtable, i);
        if (rte->rtekind == RTE_RELATION)
        {
            relation = RelationIdGetRelation(rte->relid);
            relName = NameStr(relation->rd_rel->relname);
            relOid = RelationGetRelid(relation);

            elog(INFO, "Hook Test! Relation OID : %u", relOid);
            elog(INFO, "Hook Test! Relation Name : %s", relName);

            RelationClose(relation);

            oidList = lappend_oid(oidList, relOid);
        }
    }

    return oidList;
}

static void prewarm_relations(unsigned int relOid)

{
    elog(INFO, "Prewarm Called! Relation OID : %u", relOid);

    Relation rel;
    BlockNumber blkno;
    BlockNumber nblocks;
    Buffer buf;
    

    // Open the relation
    rel = relation_open(relOid, AccessShareLock);

    elog(INFO, "Opened relation with OID: %u", relOid);

    // Get the number of blocks
    nblocks = RelationGetNumberOfBlocks(rel);

    elog(INFO, "Number of blocks in relation: %u", nblocks);

    for (blkno = 0; blkno < nblocks; blkno++)
    {
        // Read the buffer to prewarm it
        buf = ReadBuffer(rel, blkno);

        // Release the buffer
        ReleaseBuffer(buf);
    }

    elog(INFO, "Prewarmed all blocks in relation");

    // Close the relation
    relation_close(rel, AccessShareLock);

    elog(INFO, "Closed relation with OID: %u", relOid);
}

/*
static void empty_buffer()
{
}
*/

// Actual extension starting point
static void pg_query_pre_run_hook(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once)
{
    // Determine relevant relations via OID directly from query plan
    List *relations = get_relations_from_plan(queryDesc);

    // Check GUC

    // Load relations into buffer
    for (int i = 0; i < list_length(relations); i++)
    {
        prewarm_relations(list_nth_oid(relations, i));
    }

    // Empty buffer
    // empty_buffer();

    return pg_exec_run_prev_hook(queryDesc, direction, count, execute_once);
}

// Standard functions to init (load) and unload extensions using postgres hooks

void _PG_init(void)
{
    pg_exec_run_prev_hook = ExecutorRun_hook;

    if (pg_exec_run_prev_hook == NULL)
        pg_exec_run_prev_hook = standard_ExecutorRun;

    ExecutorRun_hook = pg_query_pre_run_hook;
}

void _PG_fini(void)
{
    ExecutorRun_hook = pg_exec_run_prev_hook;
}
