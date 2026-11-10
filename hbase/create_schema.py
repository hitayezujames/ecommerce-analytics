"""
create_schema.py - creates the two HBase tables through the Thrift API (happybase).

Same design as create_schema.hbase (the HBase-shell version). The shell script also
pre-splits the tables into 5 regions; the Thrift API cannot pre-split, which does not
matter on a single-node Docker HBase. Use whichever you prefer:
    python create_schema.py
    docker exec -it hbase hbase shell /scripts/create_schema.hbase
"""
from hbase_utils import config, connect

TABLES = {
    config.HBASE_SESSIONS_TABLE: {
        "s": dict(max_versions=1, bloom_filter_type="ROW", compression="GZ", in_memory=True),
        "pv": dict(max_versions=1, bloom_filter_type="ROW", compression="GZ"),
        "c": dict(max_versions=1, bloom_filter_type="ROW", compression="GZ"),
    },
    config.HBASE_PRODUCT_METRICS_TABLE: {
        "m": dict(max_versions=1, bloom_filter_type="ROW", compression="GZ", time_to_live=400 * 24 * 3600),
    },
}


def main():
    conn = connect(retries=20)
    existing = {t.decode() for t in conn.tables()}
    for name, families in TABLES.items():
        if name in existing:
            print(f"dropping existing table {name}")
            conn.delete_table(name, disable=True)
        conn.create_table(name, families)
        print(f"created {name} with families {list(families)}")
    print("tables now:", [t.decode() for t in conn.tables()])
    conn.close()


if __name__ == "__main__":
    main()
