#!/bin/bash

dataset_dir='/data/dataset/'
query_dir='/data/queries/'
bench_dir='bin/benchmarks/'

queries=('sosd_books_queries.tsv' 'sosd_fb_queries.tsv' 'sosd_osm_queries.tsv')
datasets=('books_200M_uint64.bin' 'fb_200M_uint64.bin' 'osm_cellids_200M_uint64.bin')
cnt=199999400

benchmarks=('irs_bench' 'pgm_bench' 'ts_bench' 'dynamic_pgm_bench' 'btree_bench' 'alex_bench' 'ts_bsm_bench')

for bench in ${benchmarks[@]}; do
	for (( i=0; i<3; i++)); do 
		dataset=${datasets[$i]}
		query=${queries[$i]}
		printf "%20s\t%30s\t" "$bench" "$dataset"
		numactl -C1 -m1 "$bench_dir""$bench" "$cnt" "$dataset_dir""$dataset" "$query_dir""$query"

		if [[ ! $? ]]; then
			printf "ERROR\n"
		fi
	done
done

vector_datasets=('cleaned-vectors.txt')
vector_queries=('sbw_queries.txt')

vector_benchmarks=('vptree_bench' 'mtree_bench' 'vptree_bsm_bench')
for bench in ${vector_benchmarks[@]}; do
	for (( i=0; i<1; i++)); do
		dataset=${vector_datasets[$i]}
		query=${vector_queries[$i]}
		printf "%20s\t%30s\t\t" "$bench" "$dataset"
		cnt=$(wc -l "$dataset_dir""$dataset" | cut -d' ' -f1)
		numactl -C1 -m1 "$bench_dir""$bench" "$cnt" "$dataset_dir""$dataset" "$query_dir""$query"

		if [[ ! $? ]]; then
			printf "ERROR\n"
		fi
	done
done

string_datasets=('ursarc2.0.txt' 'english-words.txt')
string_benchmarks=('fst_bench' 'fst_bsm_bench')
for bench in ${string_benchmarks[@]}; do
	for (( i=0; i<2; i++)); do
		dataset=${string_datasets[$i]}
		printf "%20s\t%30s\t\t" "$bench" "$dataset"
		cnt=$(wc -l "$dataset_dir""$dataset" | cut -d' ' -f1)
		numactl -C1 -m1 "$bench_dir""$bench" "$cnt" "$dataset_dir""$dataset" 

		if [[ ! $? ]]; then
			printf "ERROR\n"
		fi
	done
done

binary_vector_datasets=('bigann.u8bin')
binary_vector_queries=('ann_queries.u8bin')

binary_vector_benchmarks=('vptree_bench_alt' 'mtree_bench_alt' 'vptree_bsm_bench_alt')

for bench in ${binary_vector_benchmarks[@]}; do
	for (( i=0; i<1; i++)); do
		dataset=${binary_vector_datasets[$i]}
		query=${binary_vector_queries[$i]}
		printf "%20s\t%30s\t\t" "$bench" "$dataset"
		cnt=10000000
		numactl -C1 -m1 "$bench_dir""$bench" "$cnt" "$dataset_dir""$dataset" "$query_dir""$query"

		if [[ ! $? ]]; then
			printf "ERROR\n"
		fi
	done
done


