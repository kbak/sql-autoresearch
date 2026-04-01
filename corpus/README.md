# Query Corpus

Place `.sql` files in `queries/` and register them via:

```
sql-autoresearch corpus add queries/myquery.sql --description "Description"
```

The `manifest.toml` records SHA-256 hashes to lock the corpus before running the brutal test. Verify integrity with:

```
sql-autoresearch corpus verify
```
