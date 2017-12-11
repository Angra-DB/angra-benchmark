# angra-benchmark
scripts for Angra-DB benchmark using python. This script can benchmark CouchDB, MongoDB and MySQL using [YCSB](https://github.com/Angra-DB/YCSB-Angra-DB).

## See `JSON` files and configure to your environment!

### How this works?

 1. Configure all your parameters on `JSON` files;
 2. Run `benchmark.py` script. Take your time, do something, this may take a while.

  > tip: Use `stdbuf` to save terminal screen at each screen change, and verify your tests.

  ~~~
  stdbuf -oL python /path/to/angra-benchmark/benchmark.py all &> screen_bench.txt
  ~~~

 3. Execute `handle_logs.py` script to create CSV with YCSB results.
  ~~~
  stdbuf -oL python /path/to/angra-benchmark/handle_logs.py csv &> screen_logs.txt
  ~~~

 4. Execute `charts.py` script to create pdf and png charts.
   ~~~
   stdbuf -oL python /path/to/angra-benchmark/charts.py &> screen_charts.txt
   ~~~

 5. Analise your results.

 ***

See:
* https://github.com/angra-DB
* https://github.com/Angra-DB/YCSB-Angra-DB
* https://github.com/brianfrankcooper/YCSB/wiki
* https://github.com/arnaudsjs/YCSB-couchdb-binding
