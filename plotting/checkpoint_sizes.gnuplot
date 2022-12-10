set datafile separator ','

set key autotitle columnhead
set key above opaque box

set ylabel "Size (MB)"
set xlabel "Partition"

set style data histogram
set style histogram cluster gap 1
set style fill solid

set yrange [0:]

set boxwidth 0.9

#set format y "%.0s%c"

set terminal pdf enhanced color dashed font "Times-Roman, 13"

do for [name in "A D E"] {
  set output sprintf('ycsb_%s_ckp_sizes.pdf', name)
  plot sprintf('ycsb_%s_ckp_sizes.csv', name) \
     using ($2/1e6):xtic(1) lc rgb "#abdda4", \
  '' using ($3/1e6) lc rgb "#fdae61", \
  '' using ($4/1e6) lc rgb "#d7191c"

  set output sprintf('ycsb_%s_ckp_sizes_parallel_only.pdf', name)
  plot sprintf('ycsb_%s_ckp_sizes.csv', name) \
     using ($3/1e6):xtic(1) lc rgb "#fdae61", \
  '' using ($4/1e6) lc rgb "#d7191c"
}
