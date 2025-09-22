# voter-daily

Repository for sample election analytics pipeline processing CSV voter records using Python and dbt

## MVP plan

1. loader.py -> extract CSV, load to duckdb

- test_loader.py -> schema validate; type check; verify db connectivity

2. main.py -> run loader

- test_main.py -> create test temp db and write mock record

3. astro dag task order

- run main
- run dbt

4. thinking Metabase for visualization

- need to consider astro testing
