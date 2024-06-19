#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_type.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "storage/bufmgr.h"
#include "access/heapam.h"
#include "executor/executor.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

// needed hooks
static ExecutorRun_hook_type pg_exec_run_prev_hook = NULL;

// Traverse query plan tree and get all relevant relations
static Oid get_relations_from_plan(QueryDesc *queryDesc)
{
    Plan *plan = queryDesc->plannedstmt->planTree;
    RangeTblEntry *rte;

    Relation relation = NULL;
    char *relName = NULL;
    Oid relOid;
    

    rte = list_nth(queryDesc->plannedstmt->rtable, 0);

    relation = RelationIdGetRelation(rte->relid);
    relName = NameStr(relation->rd_rel->relname);
    relOid = RelationGetRelid(relation);

    elog(INFO, "Hook Test! Plan Type : %u", relOid);
    elog(INFO, "Hook Test! Plan Type : %s", relName);

    // Close the relation
    relation_close(relation, AccessShareLock);

    return relOid;
}

static void pg_query_pre_run_hook(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once)
{
    elog(INFO, "Entering pg_query_pre_run_hook");

    // Determine relevant relations directly from query plan and get the OID
    Oid relOid = get_relations_from_plan(queryDesc);
    

    elog(INFO, "Obtained OID: %u", relOid);

    // Use pg_prewarm to load the relation with the obtained OID into the buffer
    Relation rel;
    ForkNumber forknum = MAIN_FORKNUM;
    BlockNumber nblocks;
    BlockNumber blkno;

    // Open the relation
    rel = relation_open(relOid, AccessShareLock);

    elog(INFO, "Opened relation with OID: %u", relOid);

    // Get the number of blocks
    nblocks = RelationGetNumberOfBlocks(rel);

    elog(INFO, "Number of blocks in relation: %u", nblocks);

    for (blkno = 0; blkno < nblocks; blkno++)
    {
        Buffer buf;

        // Read the buffer to prewarm it
        buf = ReadBuffer(rel, blkno);

        // Release the buffer
        ReleaseBuffer(buf);
    }

    elog(INFO, "Prewarmed all blocks in relation");

    // Close the relation
    relation_close(rel, AccessShareLock);

    elog(INFO, "Closed relation with OID: %u", relOid);

    // Call the previous hook function
    if (pg_exec_run_prev_hook)
    {
        elog(INFO, "Calling previous hook");
        pg_exec_run_prev_hook(queryDesc, direction, count, execute_once);
    }
    else
    {
        elog(INFO, "Calling standard_ExecutorRun");
        standard_ExecutorRun(queryDesc, direction, count, execute_once);
    }

    elog(INFO, "Exiting pg_query_pre_run_hook");
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