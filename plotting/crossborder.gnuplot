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

set format y "%.1s%c"

set terminal pdf enhanced color dashed font "Times-Roman, 13"

set output 'ycsb_E_crossborder.pdf'

plot 'ycsb_E_crossborder.csv' \
     using 2:xtic(1) lc rgb '#0000ff', \
  '' using 3 lc rgb "#abdda4", \
  '' using 4 lc rgb "#fdae61", \
  '' using 5 lc rgb "#d7191c"

