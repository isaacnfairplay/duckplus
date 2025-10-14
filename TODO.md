- [x] Add a DuckCon class with a context manager that will be easy to extend for io operations
- [x] Add a relation class that is immutable and has the DuckCon and a duckdbpy connection under the hood with some metadata stored like columns and Duckdb types (as varchar for now)

- [ ] Add column manipulations
```python
# transforming a column(s)

# uses REPLACE in SQL under the hood
duckdb_relation.select("* REPLACE(column::INTEGER as column)")
relation = (
    relation
    .transform(
        column="{column}::INTEGER" # we can also pass a 
    )
)
# uses REPLACE in SQL under the hood
duckdb_relation.select("* rename(column as column_new)")
relation = (
    relation
    .rename(
        column_new="column"
    )
)
# adding a column(s)

# uses REPLACE in SQL under the hood
duckdb_relation.select(f"*, {expression} as column_new)")
relation = (
    relation
    .add(
        column_new="expression"
    )
)

# keeping a subset of columns

#duckdb_relation.select(','.join(columns))
relation = (
    relation
    .keep(
        'column_one',
        'column_two'
    )
)
# Raises if columns are missing, by default
# alias keep_if_exists for the soft version

## dropping columns
python
# duckdb_relation.select("* exclude({','.join(columns)}))
relation = (
    relation
    .drop(
        'column_three
    )
)
# Raises if columns are missing
# drop_if_exists as the soft version
```

- [ ] Add static Typed expression api to allow easier api discovery

Aggregation spec that can be used to make filters that can be used anywhere
for the purposes of accessing the api easily
ducktype.Numeric.Aggregate.sum("sales") will return "sum(sales)"

this does not provide runtime type checking but it does allow us to explore the duckdb api in our IDE

this should allow us to include ALL column types
ducktype.Any.Aggregate.max_by("customer",ducktype.Blob("customer_id")) may raise a Type Error depending on if you can sort by blob in maxby or not

this can also be used to make a filter condition 
ducktype.Varchar('customer') == 'prime'
and join conditions in the same way when column names are not the same
ducktype.varchar('customer') == ducktype.varchar('sales_customer')
and renames and various other transforms
ducktype('customer').alias('my_customer')
by default will resolve to a str or a dict depending on which helper you use on them but we will also want to have support for all column operations in this api
having expressions too should have this option 
add window function support to this api

- [ ] aggregation support

```python
relation = (
    relation
    .aggregate(
        "customer",
        sum_sales = "sum(sales)",
        "count(distinct transactions) > 1
    )
)
```
- [ ] filter support

```python
relation = (
    filter(
        "sum_sales > 10"
    )
)
```

- [ ] "Error on column conflict" joins standard [inner, left, right, outer and also semi join] automatically joins on shared columns, allows forign join conditions to be specified
- [ ] "Asof" join implementatin using the expression api
- [ ] "io helpers for csv parquet and so on"
- [ ] appenders for csv and ndjson and specialized insert tooling too (you can check long term git history for inspiration)
- [ ] table interfacing api for inserts to tables


