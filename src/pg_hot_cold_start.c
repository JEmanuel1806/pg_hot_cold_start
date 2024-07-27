#include "postgres.h"
#include "utils/guc.h"
#include "fmgr.h"
#include "optimizer/planner.h"
#include "utils/rel.h"
#include "utils/guc.h"

#include "access/relation.h"

#include "storage/bufmgr.h"
#include "storage/read_stream.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern ProcNumber MyProcNumber;
extern Oid MyDatabaseId;
extern Oid MyDatabaseTableSpace;

/* GUC Variable */
static char *experiment_mode = NULL;

/* Used Hook */
static planner_hook_type prev_planner_hook = NULL;

/* Traverse query plan tree and get all relevant relations */
static void get_relations_from_plan(Query *query, Plan *planTree, List **oidList)
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
     * For indexes, check for the type of index and retrieve its relation ID over the scan.id. Store this in the oidList.
     *
     * The oidList is later passed to the prewarm function for further processing.
     */

    switch (planTree->type)
    {
    case T_SeqScan:
    {
        Scan *scan = (Scan *)planTree;
        rte = list_nth(query->rtable, scan->scanrelid - 1);
        relOid = rte->relid;

        elog(INFO, "Scanning relation: %s", rte->eref->aliasname);
        *oidList = lappend_oid(*oidList, relOid);
        break;
    }
    case T_BitmapHeapScan:
    {
        Scan *scan = (Scan *)planTree;
        rte = list_nth(query->rtable, scan->scanrelid - 1);
        relOid = rte->relid;

        elog(INFO, "Bitmap Heap Scan with relation: %s", rte->eref->aliasname);
        *oidList = lappend_oid(*oidList, relOid);
        break;
    }
    case T_IndexScan:
    {
        IndexScan *indexScan = (IndexScan *)planTree;
        indexOid = indexScan->indexid;

        /* Get relations from index */
        rte = list_nth(query->rtable, indexScan->scan.scanrelid - 1);
        relOid = rte->relid;
        elog(INFO, "Scanning relation: %s from index scan: %d", rte->eref->aliasname, indexOid);
        *oidList = lappend_oid(*oidList, relOid);
        break;
    }
    case T_BitmapIndexScan:
    {
        BitmapIndexScan *bitmapIndexScan = (BitmapIndexScan *)planTree;
        bitOid = bitmapIndexScan->indexid;

        /* Get relations from bitmap index */
        rte = list_nth(query->rtable, bitmapIndexScan->scan.scanrelid - 1);
        relOid = rte->relid;
        elog(INFO, "Scanning relation: %s from Bitmap index: %d", rte->eref->aliasname, bitOid);
        *oidList = lappend_oid(*oidList, relOid);
        break;
    }
    case T_IndexOnlyScan:
    {
        IndexOnlyScan *indexOnlyScan = (IndexOnlyScan *)planTree;
        indexOnlyOid = indexOnlyScan->indexid;

        /* Get relations from index only scan index*/
        rte = list_nth(query->rtable, indexOnlyScan->scan.scanrelid - 1);
        relOid = rte->relid;
        elog(INFO, "Scanning relation: %s from index only scan: %d", rte->eref->aliasname, indexOnlyOid);
        *oidList = lappend_oid(*oidList, relOid);
        break;
    }
    default:
    {
        /* Skip unhandled scans */
        break;
    }
    }

    /* Iterate through plan , pass the OID list to be filled */
    get_relations_from_plan(query, planTree->lefttree, oidList);
    get_relations_from_plan(query, planTree->righttree, oidList);
}

static void prewarm_relations(unsigned int relOid)

{

    Relation rel;
    BlockNumber blkno;
    BlockNumber nblocks;
    Buffer buf;

    // elog(INFO, "Prewarming Relation: %s", NameStr(RelationIdGetRelation(relOid)->rd_rel->relname));

    rel = relation_open(relOid, AccessShareLock);

    /* Number of bloX per relation */
    nblocks = RelationGetNumberOfBlocks(rel);

    elog(INFO, "Relation Size: %u MB/ %u MB", nblocks * 8 / 1024, MaxBlockNumber * 8 / 1024);

    for (blkno = 0; blkno < nblocks; blkno++)
    {
        /* Read the buffer to prewarm it */
        buf = ReadBuffer(rel, blkno);

        ReleaseBuffer(buf);
    }

    relation_close(rel, AccessShareLock);
}

/*
static void evict_relation(unsigned int relOid)
{
    Relation rel;
    RelFileLocator relFileLocator;
    SMgrRelation smgr = NULL;

    relFileLocator.relNumber = relOid;
    relFileLocator.dbOid = MyDatabaseId;
    relFileLocator.spcOid = MyDatabaseTableSpace;

    rel = relation_open(relOid, AccessShareLock);

    smgr = smgropen(relFileLocator, MyProcNumber);

    for (ForkNumber forkNum = 0; forkNum <= MAX_FORKNUM; ++forkNum)
    {
        DropRelationBuffers(smgr, &forkNum, 4, 0);
    }

    relation_close(rel, AccessShareLock);
}
*/

static void evict_relations()
{
    FlushDatabaseBuffers(MyDatabaseId);
    elog(INFO, "Emptied buffer for database: %d", MyDatabaseId);
    DropDatabaseBuffers(MyDatabaseId);
}

static PlannedStmt *
pg_query_planner_hook(Query *parse, int cursorOptions, const char *query_string, ParamListInfo boundParams)
{
    // Call the previous planner hook if it exists, otherwise call standard_planner
    PlannedStmt *result = NULL;

    if (prev_planner_hook)
    {
        result = prev_planner_hook(parse, query_string, cursorOptions, boundParams);
    }
    else
    {
        result = standard_planner(parse, query_string, cursorOptions, boundParams);
    };

    /*
     * HOT START
     * Get the relations and indices from the query plan tree and prewarm each of them
     */

    if (strcmp(experiment_mode, "hot") == 0)
    {

        Plan *planTree = result->planTree;
        List *oidList = NIL;

        /* Get the relations to be prewarmed out of the query plan */
        get_relations_from_plan(parse, planTree, &oidList);

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
    else if (strcmp(experiment_mode, "cold") == 0)
    {
        evict_relations();
    }
    else if (strcmp(experiment_mode, "off") == 0)
    {
        elog(INFO, "Extension set to off");
    }
    return result;
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

    prev_planner_hook = planner_hook;
    planner_hook = pg_query_planner_hook;
}

// Modify _PG_fini to unset the planner hook
void _PG_fini(void)
{
    planner_hook = prev_planner_hook;
}
