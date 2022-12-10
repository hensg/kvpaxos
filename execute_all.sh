#!/bin/bash
#
./compile.sh
./build/bin/replica a_conf_0_ckp.toml > a_results_0_ckp.csv
./build/bin/replica a_conf_1_ckp.toml > a_results_1_ckp.csv
./build/bin/replica a_conf_8_ckp_metis.toml > a_results_8_ckp_metis.csv
./build/bin/replica a_conf_8_ckp.toml > a_results_8_ckp.csv
#
./build/bin/replica d_conf_8_ckp_metis.toml > d_results_8_ckp_metis.csv
./build/bin/replica d_conf_1_ckp.toml > d_results_1_ckp.csv
./build/bin/replica d_conf_8_ckp.toml > d_results_8_ckp.csv
./build/bin/replica d_conf_0_ckp.toml > d_results_0_ckp.csv
#
./build/bin/replica e_conf_0_ckp.toml > e_results_0_ckp.csv
./build/bin/replica e_conf_1_ckp.toml > e_results_1_ckp.csv
./build/bin/replica e_conf_8_ckp_metis.toml > e_results_8_ckp_metis.csv
./build/bin/replica e_conf_8_ckp.toml > e_results_8_ckp.csv
