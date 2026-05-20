# Apache Spark Catalyst Optimizer: Complete Guide

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Optimization Stages](#optimization-stages)
4. [Optimization Rules](#optimization-rules)
5. [Real-World Examples](#real-world-examples)
6. [Interview Questions](#interview-questions)
7. [Performance Tips](#performance-tips)

---

## Overview

### What is Catalyst?
**Catalyst** is Apache Spark's cost-based query optimizer that transforms SQL queries and DataFrame operations into optimized execution plans. It's the brain behind Spark SQL's performance.

### Why Does Catalyst Matter?
- **Automatic Optimization**: You write logical queries; Catalyst makes them fast
- **Framework Agnostic**: Works with SQL, DataFrames, and Datasets
- **Extensible**: Custom optimization rules can be added
- **Performance**: Can provide 100x+ improvements through intelligent optimizations

### Key Principle
> Transform user-written queries into efficient physical execution plans without user intervention

---

## Architecture

### High-Level Flow
```
SQL/DataFrame Input
    ↓
Parser (creates AST)
    ↓
Logical Planning
    ↓
Logical Optimization
    ↓
Physical Planning
    ↓
Code Generation
    ↓
Execution
```

### Core Components

| Component | Responsibility |
|-----------|-----------------|
| **Parser** | Converts SQL strings into Abstract Syntax Trees (AST) |
| **Analyzer** | Validates AST, resolves column names, applies metadata |
| **Logical Optimizer** | Applies rule-based transformations to logical plan |
| **Physical Planner** | Chooses best physical implementation strategies |
| **Code Generator** | Generates optimized bytecode (whole-stage codegen) |

---

## Optimization Stages

### Stage 1: Parsing & Analysis

**What Happens:**
- SQL string → Abstract Syntax Tree (AST)
- Column name resolution
- Type checking
- Validation against catalog metadata

**Example:**
```sql
SELECT o.id, c.name, COUNT(*) 
FROM orders o 
JOIN customers c ON o.customer_id = c.id 
WHERE o.amount > 100 
GROUP BY o.id, c.name
```

**Output:** Unoptimized logical plan with all references resolved

---

### Stage 2: Logical Optimization

**Purpose:** Apply rule-based transformations to improve the logical plan

#### Key Optimization Rules

##### **1. Predicate Pushdown**
Push filter conditions as early as possible in the plan

**Before:**
```
Join
  ├─ orders (full table)
  └─ customers (full table)
    └─ Filter: o.amount > 100
```

**After:**
```
Join
  ├─ Filter: o.amount > 100
  │  └─ orders
  └─ customers
```

**Impact:** Reduces rows before expensive join operation

##### **2. Projection Pushdown**
Select only needed columns early, discard unnecessary ones

**Before:**
```
Project [id, name]
  └─ Join [all columns from both tables]
```

**After:**
```
Join [only needed columns]
  ├─ Project [needed from orders]
  └─ Project [needed from customers]
```

##### **3. Constant Folding**
Pre-compute expressions with constant values

**Before:**
```
Filter: date >= (now() - 365 days)  // computed for each row
```

**After:**
```
Filter: date >= 2024-05-20  // computed once
```

##### **4. Dead Code Elimination**
Remove unused columns and subqueries

##### **5. Boolean Simplification**
Simplify logical expressions

```
(x = 1 AND x = 1) → (x = 1)
(x = 1 OR TRUE) → TRUE
```

##### **6. Null Propagation**
Eliminate conditions that always evaluate to NULL

---

### Stage 3: Physical Planning

**Purpose:** Choose execution strategies for each logical operator

#### Join Strategy Selection

The physical planner chooses the best join based on:
- Data size
- Available statistics
- Spark configuration

##### **a) Broadcast Hash Join** (Optimal for small tables)
```
Condition: Small table < spark.sql.autoBroadcastJoinThreshold (10MB default)

Advantages:
- No shuffle required
- All workers have access to small table
- Single-pass operation

Example:
SELECT * FROM orders o 
JOIN small_lookup_table l ON o.lookup_id = l.id
```

##### **b) Sort-Merge Join** (For pre-sorted data)
```
Condition: Both tables sorted on join key

Steps:
1. Sort both tables by join key (if needed)
2. Merge sorted streams
3. Match rows with same key

Advantages:
- Memory efficient
- Good for large tables
- Streaming-friendly
```

##### **c) Shuffle Hash Join** (Default for large-large joins)
```
Condition: When neither broadcast nor sort-merge applies

Steps:
1. Shuffle both tables by join key
2. Hash each partition
3. Perform hash join locally

Cost: High shuffle overhead
```

#### Aggregation Strategy Selection

| Strategy | Condition | Use Case |
|----------|-----------|----------|
| **Hash Agg** | Data fits in memory | Small aggregations |
| **Sort-Based Agg** | Memory constrained | Large grouped aggregations |

---

### Stage 4: Code Generation (Whole-Stage CodeGen)

**Purpose:** Generate optimized Java bytecode for entire query stage

**Before CodeGen:**
```
Multiple function calls overhead:
- Virtual function dispatch
- Memory allocation for intermediate objects
- Loop overhead
```

**After CodeGen:**
```java
// Single monolithic function
public void processRow(Object[] row) {
    // Direct field access
    // Inline operations
    // No intermediate objects
}
```

**Performance Impact:** 2-10x speedup for complex queries

---

## Optimization Rules in Action

### Example: Complex Query Optimization

**Original Query:**
```sql
SELECT 
    customer_id, 
    product_id,
    SUM(amount) as total_amount,
    COUNT(*) as order_count
FROM orders
WHERE order_date >= '2024-01-01'
  AND amount > 0
  AND customer_id IS NOT NULL
GROUP BY customer_id, product_id
HAVING SUM(amount) > 1000
```

**Optimization Steps:**

1. **Predicate Pushdown** (filters pushed to table scan)
   ```
   Early filter: order_date >= '2024-01-01' AND amount > 0
   ```

2. **Null Propagation**
   ```
   Filter: customer_id IS NOT NULL → applied early
   ```

3. **Projection Pushdown**
   ```
   Only read: order_date, amount, customer_id, product_id
   ```

4. **Push HAVING down to aggregation**
   ```
   Aggregate on filtered data, then filter results
   ```

5. **Code Generation**
   ```
   Generate single bytecode function for entire pipeline
   ```

**Result:** Scans minimal rows, processes minimal columns, executes optimized code

---

## Real-World Examples

### Example 1: Broadcast Join Optimization

```scala
// Scala Example
val orders = spark.read.parquet("data/orders")
val customers = spark.read.parquet("data/customers")

// Customer table is small (< 10MB)
val result = orders.join(customers, "customer_id")

// Catalyst Optimization:
// 1. Detects customers table is small
// 2. Broadcasts it to all executors
// 3. Each executor performs local hash join
// 4. No shuffle required → massive speedup
```

**Execution Plan (EXPLAIN):**
```
BroadcastHashJoin [customer_id]
├─ Scan parquet orders
└─ BroadcastExchange
   └─ Scan parquet customers
```

### Example 2: Predicate Pushdown

```scala
// Without understanding Catalyst
val df = spark.read.parquet("data/large_sales_table")
    .select("*")  // Load all columns
    .filter($"region" === "US")  // Filter after load

// What Catalyst optimizes to:
val df = spark.read.parquet("data/large_sales_table")
    .filter($"region" === "US")  // Filter during scan
    .select("*")

// Even better: only necessary columns
val df = spark.read.parquet("data/large_sales_table")
    .filter($"region" === "US")
    .select("id", "name", "amount")  // Projection pushdown
```

**Performance Impact:**
- Read 50% less data from disk (predicate pushdown)
- Read only needed columns (projection pushdown)
- Combined: 10-20x faster on large datasets

### Example 3: Avoiding Common Anti-Patterns

```scala
// ❌ BAD: Multiple passes, no optimization
val df1 = data.filter($"age" > 18)
val df2 = df1.select("name", "email")
val df3 = df2.join(other_data, "id")
val result = df3.groupBy("name").count()

// ✅ GOOD: Single logical plan
val result = data
    .filter($"age" > 18)
    .select("name", "email")
    .join(other_data, "id")
    .groupBy("name")
    .count()

// Both are optimized the same by Catalyst, but second is clearer
```

---

## Interview Questions & Answers

### Q1: Explain Catalyst's optimization stages

**Answer:**
Catalyst has 4 main stages:

1. **Parsing & Analysis**: Convert SQL to AST, resolve columns
2. **Logical Optimization**: Apply rules (predicate pushdown, etc.)
3. **Physical Planning**: Choose execution strategies (broadcast join, sort-merge, etc.)
4. **Code Generation**: Generate optimized bytecode

Each stage makes the plan more efficient without changing results.

---

### Q2: What is predicate pushdown and why does it matter?

**Answer:**
Predicate pushdown moves filter conditions earlier in the execution plan, ideally to the table scan itself.

**Why it matters:**
- Reduces data volume early (fewer rows to process downstream)
- Minimizes shuffle operations (fewer rows to shuffle)
- Reduces memory usage

**Example:**
```
Before: Scan → Join → Filter (processes millions of rows)
After:  Filter (at scan) → Join → (processes thousands of rows)
```

Impact: Can provide 10-100x speedup on large datasets.

---

### Q3: When does Catalyst choose broadcast join vs shuffle join?

**Answer:**
Catalyst compares table sizes against `spark.sql.autoBroadcastJoinThreshold` (default 10MB):

**Broadcast Join:**
- Condition: Small table < threshold AND broadcast hints present
- Advantage: No shuffle, all data replicated to executors
- Use: Small dimension tables

**Shuffle Hash Join:**
- Condition: Both tables large
- Advantage: Balanced work distribution
- Cost: Expensive shuffle operation

**Sort-Merge Join:**
- Condition: Data already sorted or natural sort opportunity
- Advantage: Memory efficient, streaming-friendly
- Use: Pre-sorted data or when memory constrained

---

### Q4: What is whole-stage code generation?

**Answer:**
Catalyst generates a single optimized Java function for entire query stages instead of calling multiple functions.

**Benefits:**
- Eliminates virtual function call overhead
- Inlines operations
- Reduces intermediate object creation
- 2-10x performance improvement

**Example:**
```
Instead of: (loop row) → parse → filter → transform → collect
It generates: (loop row) { parse+filter+transform inline }
```

---

### Q5: How do you read a Spark execution plan?

**Answer:**
```scala
df.explain(mode="extended")
```

Returns plans at different levels:

1. **Parsed Logical Plan**: What you wrote
2. **Analyzed Logical Plan**: With resolved columns
3. **Optimized Logical Plan**: After optimizations
4. **Physical Plan**: How it will execute

**Reading Order (bottom-up in physical plan):**
- Scans (where data comes from)
- Filters (reduce rows early)
- Joins (expensive operations)
- Aggregations (group and compute)
- Limits/Sorts (final operations)

---

### Q6: What's the difference between logical and physical plans?

**Answer:**

| Aspect | Logical Plan | Physical Plan |
|--------|------------|--------------|
| **Purpose** | WHAT to compute | HOW to compute it |
| **Focus** | Correctness | Performance |
| **Example** | Join + Filter | BroadcastHashJoin + Filter |
| **Optimization** | Rule-based | Cost-based |

**Example:**
```
Logical: Filter(Join(orders, customers))
Physical: BroadcastHashJoin → Filter
```

---

### Q7: Can you give an example of a query Catalyst struggles with?

**Answer:**
Catalyst struggles with:

1. **Highly Correlated Subqueries**
```sql
SELECT * FROM orders o
WHERE o.amount > (SELECT AVG(amount) FROM orders)
-- Catalyst may not optimize this well
```

2. **Complex User-Defined Functions (UDFs)**
```scala
spark.udf.register("complexLogic", (x: Int) => {...})
df.filter(col("value") > complexLogic(col("id")))
// UDF not optimizable - Catalyst can't see inside it
```

3. **Recursive CTEs**
```sql
WITH RECURSIVE cte AS (...)
-- Limited optimization support
```

**Mitigation:** Use SQL built-ins, avoid complex UDFs, use DataFrame API when possible

---

## Performance Tips

### 1. Enable Adaptive Query Execution (AQE)
```scala
spark.conf.set("spark.sql.adaptive.enabled", "true")
```
Dynamically adjusts plan based on runtime statistics

### 2. Collect Statistics for CBO
```scala
spark.sql("ANALYZE TABLE my_table COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE my_table COMPUTE STATISTICS FOR COLUMNS id, name")
```

### 3. Use Broadcast for Small Tables
```scala
import org.apache.spark.sql.functions.broadcast

df1.join(broadcast(df2), "id")  // Explicit hint
```

### 4. Avoid UDFs When Possible
```scala
// ❌ Slow: UDF not optimizable
df.withColumn("new_col", udf_function($"col"))

// ✅ Fast: Catalyst optimizable
df.withColumn("new_col", when($"col" > 10, "high").otherwise("low"))
```

### 5. Use Columnar Formats (Parquet)
```scala
// ✅ Better: Parquet enables column pruning
spark.read.parquet("data/my_table")

// ❌ Slower: CSV requires full row scan
spark.read.csv("data/my_table")
```

### 6. Check Execution Plans
```scala
df.explain(mode="extended")  // See all plans
df.show()  // See actual performance
```

---

## Summary

**Catalyst is powerful because it:**
- ✅ Automatically optimizes queries
- ✅ Works transparently (no code changes needed)
- ✅ Applies 20+ optimization rules
- ✅ Generates efficient bytecode
- ✅ Makes Spark SQL production-ready

**Master these concepts:**
1. Predicate & Projection Pushdown
2. Join Strategy Selection
3. Code Generation
4. Logical vs Physical Plans
5. Query Plan Reading

---

## References
- [Spark SQL Optimizer Documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Catalyst: A Query Optimizer for Spark SQL](https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html)
- [Whole-Stage Code Generation](https://databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second.html)
