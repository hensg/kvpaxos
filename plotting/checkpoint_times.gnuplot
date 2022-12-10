set datafile separator ','

set key autotitle columnhead
set key above opaque box

set ylabel "Elapsed time (s)"
set xlabel "Partition"

set style data histogram
set style histogram cluster gap 1
set style fill solid

set yrange [0:]

set boxwidth 0.9

set terminal pdf enhanced color dashed font "Times-Roman, 13"

do for [name in "A D E"] {
  set output sprintf('ycsb_%s_ckp_times.pdf', name)
  plot sprintf('ycsb_%s_ckp_times.csv', name) \
     using ($2/1e9):xtic(1) lc rgb "#abdda4", \
  '' using ($3/1e9) lc rgb "#fdae61", \
  '' using ($4/1e9) lc rgb "#d7191c"

  set output sprintf('ycsb_%s_ckp_times_parallel_only.pdf', name)
  plot sprintf('ycsb_%s_ckp_times.csv', name) \
     using ($3/1e9):xtic(1) lc rgb "#fdae61", \
  '' using ($4/1e9) lc rgb "#d7191c"
}

