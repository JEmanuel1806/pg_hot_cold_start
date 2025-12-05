# PostgreSQL Hotstart-Coldstart Hook Extension

This extension installs an `ExecutorRun` hook that automatically prewarms or empties a database table before a query is executed.  

For Prewarming, it inspects the query plan, identifies the referenced relation, loads all of its blocks into the buffer cache and then continues with PostgreSQL’s normal executor flow. 

Cold start empties the data base before execution, guaranteeing a consistent runtime, not influenced by internal caching mechanisms.

The purpose of this hook is to enable reproducible database performance measurements without the influence of internal caching behavior.  
Automatic prewarming, shared buffer reuse, or other cache effects can significantly distort execution times.  
By explicitly forcing either a hotstart (prewarmed buffers) or a coldstart (emptied buffers), this extension ensures that each run begins under controlled, comparable conditions.


---
