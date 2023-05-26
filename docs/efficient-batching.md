# Efficient Batching

One of the most important thing make caching efficiently is Batching & Pipeline.
Similar to [the N+1 Problem](https://planetscale.com/blog/what-is-n+1-query-problem-and-how-to-solve-it)
when doing batching gets is much more efficient than doing get one by one.

There are points in the caching solution that when do batching can improve its effectiveness dramatically:

* **Batching Get** for keys of the same type of item: for example multi-get for multiple product infos.
* **Batching Get** for keys of different type of items, when there is no data dependent between items: for example,
  getting product information and getting its prices is often independent, and can be fetched at the same time.
* **Batching Get** to the database (will be N + 1 problem if it's not) and **Batching Set**
  to set back data to the Cache Server when many of the keys are missed at the same time,
  especially at the start when Cache is empty.
* **Batching Get** and "Batching" Sleep for the algorithm that [Preventing Thundering Herd](thundering-herd.md).
  Which means instead of sleep one by one, all keys will sleep for the same duration
  and then **Multi-Get** to retry all the keys again.

This library will help solve all these problems.


### Efficient Batching through Defer Function Calls

#### Previous: [Preventing Thundering Herd](thundering-herd.md)
