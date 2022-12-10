set datafile separator ','

set key autotitle columnhead
set key above opaque box

set ylabel "Requests"
set xlabel "Partition"

set style data histogram
set style histogram cluster gap 1
set style fill solid

set yrange [0:]

set boxwidth 0.9

set terminal pdf enhanced color dashed font "Times-Roman, 13"
set format y "%.1s%c"

do for [name in "A D E"] {
  set output sprintf('ycsb_%s_distribution.pdf', name)
  plot sprintf('ycsb_%s_distribution.csv', name) \
     using 2:xtic(1) lc rgb '#0000ff', \
  '' using 3 lc rgb "#abdda4", \
  '' using 4 lc rgb "#fdae61", \
  '' using 5 lc rgb "#d7191c"
}

