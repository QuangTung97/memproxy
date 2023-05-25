# Consistency between Memcached and Database

Memcached, as the name suggests, most commonly used as Cache Server
for protecting other data sources, often be the databases.

The recommended way to use memcached is the **Cache-Aside Pattern**.
In which, the user requests will hit the memcached server first.
And only if memcached does not contain the data, the application server will
fetch from the backing sources (e.g. Databases),
and then set back the fetched data to the memcached server.

![Cache Aside Pattern](images/cache-aside.png)

### But how to make the data consistent between memcached and database?

The solution used for this library is **invalidating (deleting) keys on database updates**.

The general flow will look like this:
![Invalidate Flow](images/invalidate-flow.png)

1. First, user request will open a transaction, does the update as normal,
   and then insert the list of **invalidated keys** in somewhere before
   commit the transaction (*Step 1 to 4*).
   Reader familiar with patterns in distributed systems & microservices
   will recognize this
   as [Transaction Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html).
   This pattern allow to guarantee the keys in the memcached server will be deleted accordingly.
2. After the transaction has been committed, the application will delete the keys in the memcached server
   in the same thread user request is running before returning to the users (*Step 5*).
   This step serves two purposes:
    * Keeps the **read-your-own-writes consistency** in the normal condition (no failure occurs).
    * Helps mitigate the case the background job not working,
      or it can not proceed because some errors has not been handled gracefully.
3. The background job read the invalidated keys and does the deletion again (*Step 6 and 7*).