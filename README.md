## Client repository and SIDRA benchmarks
This repository contains:
* The client extension able to run SIDRA client-side.
* The SIDRA benchmarks, which are used to test the entire architecture.

Note: this README is relative to the benchmark scripts of the SIDRA paper. For the SIDRA documentation (including the client), refer to `duckdb/extension/server/README.md`.
The scripts have been generated with the stable version of SIDRA and OpenIVM, updated in May 2025. However, we are aware of potential issues and bugs and SIDRA is constantly being developed. The most up-to-date code resides in `sidra-dev`.

### Benchmark files:
* `ansible_scripts/playbook.yml`: Ansible playbook to run the benchmarks.
* `test_centralized.py`: A test script that runs the benchmarks in a centralized architecture.
* `test_decentralized.py`: A test script that runs the benchmarks in the SIDRA decentralized architecture.
* `test_commons.py`: Common functions used by the test scripts.
* `test_parameters.py`: Parameters used by the test scripts.
* `cpu_log.py`: A script to log CPU, network and storage usage during the benchmarks.
* `client.config`: Configuration file for the client nodes.
* `flush.py`: Script to orchestrate the flush of the staging area.
* `plot.R`: R script to plot the results of the benchmarks.

All the other files are either source code or examples not pertinent to the paper.

### Queries
The queries are the same as the paper, with some slight modifications due to different database engines and limitations of our architecture. The scripts have been compiled with SIDRA and OpenIVM to generate the necessary SQL statements to run the benchmarks. The changes that have been made are:
* The city and day of the base table are columns of the table rather than calculated by a function, for simplicity when generating the (skewed) data.
* We skip the ORDER BY clause in the leaderboard query since it is not supported by OpenIVM.
* LogicalPlanToString does not support the WITH clause, so we propagate deltas manually (to bypass the OpenIVM insertion rule).
* We skip the timestamp logic when refreshing the views (for brevity; OpenIVM supports timestamping).
* The user, password and database are currently hardcoded.
* We do not include filtering on the activity name or null nicknames in the decentralized IVM, to have an accurate comparison with the centralized architecture.

These limitations are in the process of being addressed and the fixes are published in the `sidra-dev` branch.

For brevity, we merged and saved the compiled output of SIDRA and OpenIVM to directly execute it, consolidating all the files and removing what is not strictly necessary. For a detailed example of how the files can be generated, check the `duckdb/extension/server/README.md` file.

Note: in older SIDRA versions, observability metrics such as completeness, responsiveness, and buffer size are described as being computed per window. Additionally, the original results were calculated under the assumption that the centralized view had the same schema as the staging view, including a `sidra_window` column. However, this design was revised late in the process. In the final implementation, centralized views no longer include the sidra_window, and metrics are instead computed per group key (e.g., per `nickname` or `(city, day)`). This change avoids duplication caused by overlapping groups belonging to different windows and ensures that metrics align with the aggregation level actually presented in the final views.

### SQL files:
* `create_tables_compiled_decentralized_server.sql`: SQL script to create the tables and views for the SIDRA server-side code.
* `create_tables_compiled_centralized_server.sql`: SQL script to create the tables and views for the OpenIVM centralized architecture.
* `cleanup.sql`: SQL script to clean up the database between benchmarks.
* `sidra_staging_view_activity_sessions_flush.sql`: SQL script to refresh the SIDRA centralized pipeline.
* `sidra_staging_view_daily_steps_user_flush.sql`: SQL script to refresh the SIDRA centralized pipeline.
* `sidra_staging_view_daily_steps_user_min_agg_flush.sql`: SQL script to refresh the SIDRA centralized pipeline with minimum aggregation.
* `sidra_decentralized_view_flush_openivm.sql`: SQL script to flush the OpenIVM decentralized view.
* `sidra_decentralized_view_flush_openivm_centralized.sql`: SQL script to flush the OpenIVM decentralized view in a centralized architecture.

### Configuration of the architecture
To run the benchmark, one needs:
* A device to orchestrate the clients and run the playbooks (e.g., a laptop); this can be the same as the server but results might be skewed in this case, and the AWS keys must be present on this device.
* A server to run the SIDRA server-side code (we use an AWS EC2 `m7g.2xl` instance; other instances will still work but might yield different results).
* AWS credentials to deploy the playbook.

On the server, you need to adjust the security group to allow incoming connections on the ports used by the server (default is 8080 for the HTTP API, and 5432 for PostgreSQL). You also need to allow incoming SSH connections from the orchestrator device.

### Setting up - orchestrator device
The requirements for the orchestrator device are:
* Python 3.8 or higher
* Ansible 2.9 or higher
* Terraform
* `aws-cli`

In order to change parameters and propagate them to the infrastructure, it is best to fork the repository and edit the files in your fork. This way, you can easily push the changes and run the benchmarks with the new parameters.
After forking, download the repo on your laptop (there is no need to build):
```bash
git clone your-fork-url
cd todo
git checkout sidra-stable
cd extension/client
```
Edit the `client.config` file to set the IP address of the server (we assume the server is reachable externally), and the database name as `activity_sessions`. Do not change anything else.

Edit also `playbook.yml` (line 32) to point it to your GitHub fork.

Edit the `test_parameters.py` file to set the parameters of the benchmarks (e.g., number of clients, number of iterations, etc.). This file should be edited for each benchmark - configurations for the evaluation in the paper is provided below.
Push the changes to your fork of the repository.

### Setting up - server
The requirements for the server are:
* PostgreSQL 16
* Python 3.8 or higher

Create the user `sidra` and the database `sidra` as superuser with password `test`:
```bash
sudo -u postgres createuser --superuser sidra
sudo -u postgres createdb --owner=sidra sidra
sudo -u postgres psql -c "ALTER USER sidra WITH PASSWORD 'test';"
PG_HBA="/etc/postgresql/16/main/pg_hba.conf" # Adjust the version and path if necessary
sudo sed -i 's/^\(local\s\+all\s\+all\s\+\)peer/\1md5/' "$PG_HBA"
```
Then change the PostgreSQL configuration to allow remote connections and tune the performance:
```bash
PG_DIR="/etc/postgresql/16/main"
sudo sed -i "s/^#listen_addresses =.*/listen_addresses = '*'/" "$PG_DIR/postgresql.conf"
sudo sed -i "s/^#*max_connections = .*/max_connections = 1000/" "$PG_DIR/postgresql.conf"
sudo sed -i "s/^#*shared_buffers = .*/shared_buffers = 4GB/" "$PG_DIR/postgresql.conf"
sudo bash -c "echo 'host    all             all             0.0.0.0/0               md5' >> $PG_DIR/pg_hba.conf"
sudo systemctl restart postgresql
```
Download and compile this repository:
```bash
git clone your-fork-url
cd duckdb
git checkout sidra-stable
make Release -BUILD_EXTENSIONS='icu;compiler;server;openivm' -OVERRIDE_GIT_DESCRIBE=v1.3.1-0-g2063dda3e6
cd build/release
```
Note: servers with around 8GB of RAM might struggle building DuckDB. In this case, one solution is to compile it on a larger machine (with the same architecture) and then transfer the binary via `scp` or `rsync` to the server. 
Run `duckdb` to create the database schema (we assume the database file to be in the same location of the `duckdb` build:
```bash
./duckdb activity_sessions.db < ../../extension/client/sql_scripts/create_tables_compiled_decentralized_server.sql
./duckdb activity_sessions.db < ../../extension/client/sql_scripts/create_tables_compiled_centralized_server.sql
```
Copy both files, even when running one single benchmark. The next step would be the compilation of the `flush` script, i. e. the SQL instructions generated by our compiler to perform a refresh, remove tuples out-of-scope and incrementally merge the result with the upstream views. 
We already created a flush script and adapted it to also include the instructions generated by OpenIVM (with some restructuring of the files for simplicity of reproduction and the limitations listed above). We also created a centralized equivalent script, simply refreshing the pipeline. 
Copy the flush scripts into the folder of the `duckdb` build:
```bash
cp -f ../../extension/client/sql_scripts/sidra_staging_view_* .
```
The flush operation will fail if the files are not copied! Then, run the server (and keep it running either in the foreground or in a screen session):
```bash
./duckdb
pragma run_server;
```
The server should be running in order to initialize clients and accept remote refresh calls. In our benchmark, we assume the client to be already initialized, but we provide the `flush.py` script to flush remotely (and update windows). 
```bash
python flush.py
```
Alternatively if you want to flush manually, there is no need to have the server running (but the window needs to be updated as well, so we recommend to run the flush script):
```sql
D pragma flush('activity_sessions', 'postgres'); -- centralized
D pragma flush('daily_steps_user', 'postgres'); -- decentralized
```
Note: switching to a new server requires changing both the `client.config` file and the `test_parameters.py` file to set the new server IP address.

#### CPU Log
To log the CPU, network and storage usage during the benchmarks, you can run the `cpu_log.py` script. This script will log the CPU usage, network traffic and disk I/O every second and save it to a file. You can run it in the background while running the benchmarks:
```bash
python cpu_log.py
```
The metrics are aggregated at the same interval as the `FLUSH_INTERVAL` in the parameters file.

### Running the benchmarks
The benchmark Python scripts implement a distributed client simulation system designed to test our data processing workflow. The system simulates multiple clients that generate activity data locally in SQLite databases, then periodically synchronize this data to a central PostgreSQL database through a network protocol. The testing framework consists of three main components working together:

* Centralized Processing (`test_centralized.py`): Manages the core client lifecycle with centralized data processing. This script handles raw activity session data, tracking user fitness metrics like steps and heartbeat rates across different cities. All data aggregation and view maintenance occurs on the central PostgreSQL server after clients transmit their raw data. It implements sophisticated client management with support for client deaths, late arrivals, and dynamic scaling based on configurable growth rates.
* Decentralized Processing (`test_decentralized.py`): A decentralized version that implements client-side Incremental View Maintenance (IVM) to minimize data movement. The key difference is that aggregation processing occurs locally on each client before transmission - clients process daily step totals from raw activity sessions using IVM techniques, then send only the pre-aggregated results to PostgreSQL. This approach significantly reduces network traffic and central server load by pushing computation to the clients.
* Common Utilities (`test_commons.py`): Provides shared functionality across both test scripts, including client configuration parsing, CSV data generation, SQLite database operations, and network communication utilities. This module handles the low-level details of client folder setup, random data generation with geographic distribution skewing, and socket-based communication with the central server.

The system generates synthetic data with specific patterns to ensure consistent testing conditions. Nicknames and usernames are identical for each client, following the format `user_{random_id}` (we do not have NULL values). Dates auto-increment with each script execution to simulate temporal progression across test runs. Cities are pseudo-randomly distributed with optional geographic skewing (90% concentrated in a primary city like New York). All other data fields including steps, heartbeat rates, and timestamps are fully randomized within realistic ranges. 

There are two ways one can run the benchmark - either in a full automated way using Ansible, or manually by running the scripts on a client.

#### Running the benchmarks on a single client
```bash
git clone your-fork-url
cd duckdb/extension/client
pip install -r requirements.txt
python test_centralized.py # or python test_decentralized.py (you might need to run it with sudo)
```
If the benchmark fails for any reason, the existing metadata might have an impact on the following runs. In this case, one needs to remove the metadata by running:
```bash
sudo rm -rf /home/tmp_duckdb/client_*
```
#### Running the benchmarks using Ansible
You need to edit the `main.tf` file to change:
* The key pair name (e.g., `sidra-key`).
* The public key to use for the instances (e.g., `~/.ssh/sidra-key.pub`).

It is also possible to change the AWS region and instance type, but this might lead to different results. The playbook assumes Ubuntu as the operating system, so if you want to use a different OS, you need to adapt the playbook accordingly.

Then, edit the `playbook.yml` (line 69) file to set the experiment being run (do not change the path, only the file):
* `test_centralized.py` for the centralized architecture.
* `test_decentralized.py` for the decentralized architecture.

If the instances have been previously generated, one needs to refresh the Ansible inventory:
```bash
terraform apply -auto-approve
```
Then, run the playbook:
```bash
 ANSIBLE_FORKS=25 ansible-playbook -i terraform-inventory.py playbook.yml -u ubuntu --key-file /home/ila/server.pem -e 'ansible_ssh_common_args="-o StrictHostKeyChecking=no"' -v
```

### Cleanup
To clean up the database between benchmarks, one can run the cleanup script:
```bash
./duckdb activity_sessions.db < ../../extension/client/sql_scripts/cleanup.sql
```
This does not remove the tables and the metadata, but it removes all the data from the tables, so that the next benchmark can start with a clean slate.
To remove the databases and the compiled files, one can run:
```bash
rm *.db *.sql *.py
```

### Plotting
The plotting scripts can be found in:
* `plot.R`: R script to plot the results of the centralized/decentralized benchmarks.
* `plot_refresh.R`: R script to plot the results of the refresh/window benchmarks.

### Benchmark configurations
Here are the configurations used for the benchmarks in the paper. These can be set in the `test_parameters.py` file. The parameters that are not mentioned explicitly should be left as default.
#### Decentralized, without minimum aggregation
* INITIAL_CLIENTS: 2000
* MAX_CLIENTS: 5500
* NEW_RATE: 0.25
* MAX_RUNS: 5
* FLUSH_NAME: 'daily_steps_user'
* CENTRALIZED = False
* MIN_AGG = False
* FLUSH_INTERVAL: 35
* ROWS_PER_CLIENT: 1
#### Centralized
Same as above, but with:
* FLUSH_NAME: 'activity_sessions'
* CENTRALIZED = True
* For 10 data points generated by the client:
  * ROWS_PER_CLIENT: 10, FLUSH_INTERVAL: 45
* 100 data points generated by the client:
  * ROWS_PER_CLIENT: 100, FLUSH_INTERVAL: 120
#### Refresh
* NEW_RATE = 1
* DEAD_RATE = 1
* REFRESH = True
* MAX_CLIENTS = 2000
* FLUSH_NAME: 'daily_steps_user'
* CENTRALIZED = False
* MIN_AGG = True
* 100 as minimum aggregation (change `sidra_staging_view_daily_steps_user_min_agg_flush.sql`, copy it again)
* 6 times a day:
  * NUM_CHUNKS: 6, MAX_RUNS: 6, INITIAL_CLIENTS: 333, FLUSH_INTERVAL: 2
* 4 times a day:
  * NUM_CHUNKS: 4, MAX_RUNS: 4, INITIAL_CLIENTS: 500, FLUSH_INTERVAL: 3
* 2 times a day:
  * NUM_CHUNKS: 2, MAX_RUNS: 2, INITIAL_CLIENTS: 1000, FLUSH_INTERVAL: 6
* 1 time a day:
  * NUM_CHUNKS: 1, MAX_RUNS: 1, INITIAL_CLIENTS: 2000, FLUSH_INTERVAL: 10
#### Window
Same as above, but with:
* UPDATE_WINDOW_EVERY_REFRESH = True
* 50 as minimum aggregation (change `sidra_staging_view_daily_steps_user_min_agg_flush.sql`)
