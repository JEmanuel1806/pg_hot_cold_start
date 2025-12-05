# PostgreSQL Hotstart-Coldstart Hook Extension

This extension installs an `ExecutorRun` hook that automatically prewarms or empites a database table before a query is executed.  

For Prewarming, it inspects the query plan, identifies the referenced relation, loads all of its blocks into the buffer cache,  
and then continues with PostgreSQL’s normal executor flow. 

Cold start empties the data base before execution, guaranteeing a consistent runtime, not influenced by internal caching mechanisms.

---
