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

    switch (planTree->type)
    {
    case T_SeqScan:
    {
        Scan *scan = (Scan *)planTree;
        rte = list_nth(queryDesc->plannedstmt->rtable, scan->scanrelid - 1);
        relOid = rte->relid;

        elog(INFO, "Scanning relation: %s", rte->eref->aliasname);
        *oidList = lappend_oid(*oidList, relOid);
        break;
    }
    case T_BitmapHeapScan:
    {
        Scan *scan = (Scan *)planTree;
        rte = list_nth(queryDesc->plannedstmt->rtable, scan->scanrelid - 1);
        relOid = rte->relid;

        elog(INFO, "Bitmap Heap Scan with relation: %s", rte->eref->aliasname);
        *oidList = lappend_oid(*oidList, relOid);
        break;
    }
    case T_IndexScan:
    {
        IndexScan *indexScan = (IndexScan *)planTree;
        indexOid = indexScan->indexid;

        // Get relations from index
        rte = list_nth(queryDesc->plannedstmt->rtable, indexScan->scan.scanrelid - 1);
        relOid = rte->relid;
        elog(INFO, "Scanning relation: %s from index scan: %d", rte->eref->aliasname, indexOid);
        *oidList = lappend_oid(*oidList, relOid);
        break;
    }
    case T_BitmapIndexScan:
    {
        BitmapIndexScan *bitmapIndexScan = (BitmapIndexScan *)planTree;
        bitOid = bitmapIndexScan->indexid;

        // Get relations from bitmap index
        rte = list_nth(queryDesc->plannedstmt->rtable, bitmapIndexScan->scan.scanrelid - 1);
        relOid = rte->relid;
        elog(INFO, "Scanning relation: %s from Bitmap index: %d", rte->eref->aliasname, bitOid);
        *oidList = lappend_oid(*oidList, relOid);
        break;
    }
    case T_IndexOnlyScan:
    {
        IndexOnlyScan *indexOnlyScan = (IndexOnlyScan *)planTree;
        indexOnlyOid = indexOnlyScan->indexid;

        // Get relations from index only scan index
        rte = list_nth(queryDesc->plannedstmt->rtable, indexOnlyScan->scan.scanrelid - 1);
        relOid = rte->relid;
        elog(INFO, "Scanning relation: %s from index only scan: %d", rte->eref->aliasname, indexOnlyOid);
        *oidList = lappend_oid(*oidList, relOid);
        break;
    }
    default:
    {
        // Skip unhandled scans
        break;
    }
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

    // elog(INFO, "Prewarming Relation: %s", NameStr(RelationIdGetRelation(relOid)->rd_rel->relname));

    rel = relation_open(relOid, AccessShareLock);

    // Get the number of blocks of the relation
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

    /*
     * HOT START
     * Get the relations and indices from the query plan tree and prewarm each of them
     */

    if (strcmp(experiment_mode, "hot") == 0)
    {

        Plan *planTree = queryDesc->plannedstmt->planTree;
        List *oidList = NIL;

        /* Get the relations to be prewarmed out of the query plan */
        get_relations_from_plan(queryDesc, planTree, &oidList);

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
    {
        evict_relation();
    }

    /*
     * OFF
     * Normal database behavior
     */

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
