1. download tpch-tools
   from [official website](https://www.tpc.org/TPC_Documents_Current_Versions/download_programs/tools-download-request5.asp?bm_type=TPC-H&bm_vers=3.0.1&mode=CURRENT-ONLY)
2. unzip the file and change dir to dbgen
3. fill the `makefile.suite` and rename it to `Makefile`. the run `make` to build the executable file. (Note that if you
   work on
   MacOS, you need to change `import malloc.h` to `import stdlib.h` in file  `varsub.c` and `bm_utils.c`)
4. run `dbgen` like this `./dbgen -vf -s 1`, and the you will find `.tbl` files in the current dir.
5. Process the obtained .tbl file into csv format for subsequent storage into sqllite. You can use the following script.

```sql
CREATE TABLE nation (
    nationkey INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    regionkey INTEGER NOT NULL,
    comment TEXT
);

CREATE TABLE region (
    regionkey INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    comment TEXT
);

CREATE TABLE part (
    partkey INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    mfgr TEXT NOT NULL,
    brand TEXT NOT NULL,
    type TEXT NOT NULL,
    size INTEGER NOT NULL,
    container TEXT NOT NULL,
    retailprice REAL NOT NULL,
    comment TEXT NOT NULL
);

CREATE TABLE supplier (
    suppkey INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    address TEXT NOT NULL,
    nationkey INTEGER NOT NULL,
    phone TEXT NOT NULL,
    acctbal REAL NOT NULL,
    comment TEXT NOT NULL
);

CREATE TABLE partsupp (
    partkey INTEGER NOT NULL,
    suppkey INTEGER NOT NULL,
    availqty INTEGER NOT NULL,
    supplycost REAL NOT NULL,
    comment TEXT NOT NULL,
    PRIMARY KEY (partkey, suppkey)
);

CREATE TABLE customer (
    custkey INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    address TEXT NOT NULL,
    nationkey INTEGER NOT NULL,
    phone TEXT NOT NULL,
    acctbal REAL NOT NULL,
    mktsegment TEXT NOT NULL,
    comment TEXT NOT NULL
);

CREATE TABLE orders (
    orderkey INTEGER PRIMARY KEY,
    custkey INTEGER NOT NULL,
    orderstatus TEXT NOT NULL,
    totalprice REAL NOT NULL,
    orderdate TEXT NOT NULL,
    orderpriority TEXT NOT NULL,
    clerk TEXT NOT NULL,
    shippriority INTEGER NOT NULL,
    comment TEXT NOT NULL
);

CREATE TABLE lineitem (
    orderkey INTEGER NOT NULL,
    partkey INTEGER NOT NULL,
    suppkey INTEGER NOT NULL,
    linenumber INTEGER NOT NULL,
    quantity REAL NOT NULL,
    extendedprice REAL NOT NULL,
    discount REAL NOT NULL,
    tax REAL NOT NULL,
    returnflag TEXT NOT NULL,
    linestatus TEXT NOT NULL,
    shipdate TEXT NOT NULL,
    commitdate TEXT NOT NULL,
    receiptdate TEXT NOT NULL,
    shipinstruct TEXT NOT NULL,
    shipmode TEXT NOT NULL,
    comment TEXT NOT NULL,
    PRIMARY KEY (orderkey, linenumber)
);
```


```bash
#!/bin/bash
folder_path="."

# hard code the headers
get_header() {
    case "$1" in
        "customer")
            echo "custkey|name|address|nationkey|phone|acctbal|mktsegment|comment"
            ;;
        "lineitem")
            echo "orderkey|partkey|suppkey|linenumber|quantity|extendedprice|discount|tax|returnflag|linestatus|shipdate|commitdate|receiptdate|shipinstruct|shipmode|comment"
            ;;
        "nation")
            echo "nationkey|name|regionkey|comment"
            ;;
        "orders")
            echo "orderkey|custkey|orderstatus|totalprice|orderdate|orderpriority|clerk|shippriority|comment"
            ;;
        "part")
            echo "partkey|name|mfgr|brand|type|size|container|retailprice|comment"
            ;;
        "partsupp")
            echo "partkey|suppkey|availqty|supplycost|comment"
            ;;
        "region")
            echo "regionkey|name|comment"
            ;;
        "supplier")
            echo "suppkey|name|address|nationkey|phone|acctbal|comment"
            ;;
        *)
            echo ""
            ;;
    esac
}

for file in customer.tbl lineitem.tbl nation.tbl orders.tbl part.tbl partsupp.tbl region.tbl supplier.tbl; do
    table_name="${file%.*}" 
    header=$(get_header "$table_name") 

    if [ -n "$header" ]; then
        tmp_file=$(mktemp "${file}.tmp.XXXXXX")
        echo "$header" > "$tmp_file"
        sed 's/|$//' "$file" >> "$tmp_file"
        mv "$tmp_file" "$file"
    else
        echo "cannot found $table_name, skip $file"
    fi
done

```g

6. load the data into the table. you can use follow scripts.

```sql
.mode csv
.separator |
.import customer.tbl customer
.import lineitem.tbl lineitem
.import nation.tbl nation
.import orders.tbl orders
.import part.tbl part
.import partsupp.tbl partsupp
.import region.tbl region
.import supplier.tbl supplier
```