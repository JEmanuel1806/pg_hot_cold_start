#include "postgres.h"
#include "utils/guc.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "utils/rel.h"
#include "utils/guc.h"

#include "access/relation.h"

#include "storage/bufmgr.h"
#include "storage/read_stream.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern Oid MyDatabaseId;
extern Oid MyDatabaseTableSpace;
extern ProcNumber MyProcNumber;

/* GUC Variable */
static char *experiment_mode = NULL;

/* Used Hook */
static ExecutorRun_hook_type pg_exec_run_prev_hook = NULL;

/* Traverse query plan tree and get all relevant relations */
static void get_relations_from_plan(QueryDesc *queryDesc, Plan *planTree, List **oidList)
{
    RangeTblEntry *rte;
    Oid relOid;
    Oid indexOid;
    Oid bitOid;
    Oid indexOnlyOid;

    /* Check if planTree is empty to check if tree traversed (recursion) */
    if (planTree == NULL)
    {
        return;
    }

    /*
     * Traverse each node in the plan tree and check whether its a relation or an index
     *
     * For relations, check if node represents a sequential scan. If so, retrieve the
     * relOid and stores it in a list (oidList).
     *
     * For indexes, check for the type of index and retrieve its ID over the scan.id. Store this in the oidList.
     *
     * The oidList is later passed to the prewarm function for further processing.
     */

    if (planTree->type == T_SeqScan)
    {
        Scan *scan = (Scan *)planTree;
        rte = list_nth(queryDesc->plannedstmt->rtable, scan->scanrelid - 1);
        relOid = rte->relid;
        elog(INFO, "Scanning relation with OID: %d", relOid);
        *oidList = lappend_oid(*oidList, relOid);
    }
    else if (planTree->type == T_IndexScan)
    {
        IndexScan *indexScan = (IndexScan *)planTree;
        elog(INFO, "Scanning index: %d", indexScan->indexid);
        indexOid = indexScan->indexid;

        // Get relations from index
        rte = list_nth(queryDesc->plannedstmt->rtable, indexScan->scan.scanrelid - 1);
        relOid = rte->relid;
        elog(INFO, "Scanning relation with OID: %d from index: %d", relOid, indexOid);
        *oidList = lappend_oid(*oidList, relOid);
    }
    else if (planTree->type == T_BitmapIndexScan)
    {
        BitmapIndexScan *bitmapIndexScan = (BitmapIndexScan *)planTree;
        elog(INFO, "Scanning bitmap index with OID: %d", bitmapIndexScan->indexid);
        bitOid = bitmapIndexScan->indexid;

        // Get relations from bitmap index
        rte = list_nth(queryDesc->plannedstmt->rtable, bitmapIndexScan->scan.scanrelid - 1);
        relOid = rte->relid;
        elog(INFO, "Scanning relation with OID: %d from index: %d", relOid, bitOid);
        *oidList = lappend_oid(*oidList, relOid);
    }
    else if (planTree->type == T_IndexOnlyScan)
    {
        IndexOnlyScan *indexOnlyScan = (IndexOnlyScan *)planTree;
        elog(INFO, "Scanning index only scan with OID: %d", indexOnlyScan->indexid);
        indexOnlyOid = indexOnlyScan->indexid;

        // Get relations from index only scan index
        rte = list_nth(queryDesc->plannedstmt->rtable, indexOnlyScan->scan.scanrelid - 1);
        relOid = rte->relid;
        elog(INFO, "Scanning relation with OID: %d from index: %d", relOid, indexOnlyOid);
        *oidList = lappend_oid(*oidList, relOid);
    }
    /* Recursively walk through the tree and its children, pass the List */
    get_relations_from_plan(queryDesc, planTree->lefttree, oidList);
    get_relations_from_plan(queryDesc, planTree->righttree, oidList);
}

static void prewarm_relations(unsigned int relOid)

{

    Relation rel;
    BlockNumber blkno;
    BlockNumber nblocks;
    Buffer buf;

    elog(INFO, "Prewarming Relation/Index with OID : %u", relOid);

    // Open the relation
    rel = relation_open(relOid, AccessShareLock);

    elog(INFO, "Opened relation with OID: %u", relOid);

    // Get the number of blocks
    nblocks = RelationGetNumberOfBlocks(rel);

    elog(INFO, "Number of blocks in relation: %u / %u", nblocks, MaxBlockNumber);

    for (blkno = 0; blkno < nblocks; blkno++)
    {
        // Read the buffer to prewarm it
        buf = ReadBuffer(rel, blkno);

        // Release the buffer
        ReleaseBuffer(buf);
    }

    // Close the relation
    relation_close(rel, AccessShareLock);
}

/*
static void evict_relation(unsigned int relOid)
{
    SMgrRelation smgr_reln;
    SMgrRelation smgr_relns[1];  // Create an array of SMgrRelation pointers
    RelFileLocator rfl;

    rfl.relNumber = relOid;
    rfl.dbOid = MyDatabaseId;
    rfl.spcOid = MyDatabaseTableSpace;

    smgr_reln = smgropen(rfl, MyProcNumber);
    smgr_relns[0] = smgr_reln;  // Add the SMgrRelation to the array

    elog(INFO, "Evict relaation with OID: %d from table %d and tablespace %d", rfl.relNumber, rfl.dbOid, rfl.spcOid);

    DropRelationsAllBuffers(smgr_relns, 1);  // Pass the array instead of the single pointer

    smgrclose(smgr_reln);
}
*/

static void evict_relation()
{
    elog(INFO, "Emptied buffer for database: %d", MyDatabaseId);
    DropDatabaseBuffers(MyDatabaseId);
}

/* Actual extension starting point */
static void pg_query_pre_run_hook(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once)
{
    Plan *planTree = queryDesc->plannedstmt->planTree;
    List *oidList = NIL;

    /* Get the relations to be prewarmed out of the query plan */
    get_relations_from_plan(queryDesc, planTree, &oidList);

    /*
     * HOT START
     * Get the relations and indices from the query plan tree and prewarm each of them
     */

    if (strcmp(experiment_mode, "hot") == 0)
    {

        /* Check content of OID List */
        if (list_length(oidList) != 0)
        {
            for (int i = 0; i < list_length(oidList); i++)
            {
                elog(INFO, "List Content OID: %d", list_nth_oid(oidList, i));
            }
        }
        /* Iterate over OIDList and prewarm each relation from it */
        for (int i = 0; i < list_length(oidList); i++)
        {
            prewarm_relations(list_nth_oid(oidList, i));
        }
    }

    /*
     * COLD START
     * Get the relations and indices from the query plan tree and prewarm each of them
     */

    if (strcmp(experiment_mode, "cold") == 0)
        /*
        {
            for (int i = 0; i < list_length(oidList); i++)
            {
                evict_relation(list_nth_oid(oidList, i));
            }
        }
         */
        evict_relation();

    if (strcmp(experiment_mode, "off") == 0)
    {
        elog(INFO, "Extension turned off");
    }

    return pg_exec_run_prev_hook(queryDesc, direction, count, execute_once);
}

// Standard functions to init (load) and unload extensions using postgres hooks

void _PG_init(void)
{

    DefineCustomStringVariable(
        "experiment_mode",
        "Activate either cold or hot start.",
        NULL,
        &experiment_mode,
        "cold", /* default value */
        PGC_SUSET,
        0,
        NULL,
        NULL,
        NULL);

    pg_exec_run_prev_hook = ExecutorRun_hook;

    if (pg_exec_run_prev_hook == NULL)
        pg_exec_run_prev_hook = standard_ExecutorRun;

    ExecutorRun_hook = pg_query_pre_run_hook;
}

void _PG_fini(void)
{
    ExecutorRun_hook = pg_exec_run_prev_hook;
}
