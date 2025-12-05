Bolt contains a drop-in copy of [DuckDB](https://duckdb.org/) code. It is
used in tests as a reference in-memory database to check results of Bolt
evaluation for correctness. If you need to update it to pick up a bug fix or
a new feature, first clone DuckDB git repository:

    git clone https://github.com/cwida/duckdb.git
    cd duckdb/

Then generate the amalgamated .cpp and .hpp files:

    python3 scripts/amalgamation.py --extended --splits=8

Then copy the generated files to bolt/external/duckdb:

    export BOLT_PATH="<path/to/bolt>"
    rsync -vrh src/amalgamation/duckdb* ${BOLT_PATH}/bolt/external/duckdb/

After the new files are copied, ensure that the new code compiles and that it
doesn't break any tests. Bolt relies on many internal APIs, so there is a good
chance that this will not work out-of-the-box and that you will have to dig in
to find out what is wrong.

Once everything works, submit a PR.
