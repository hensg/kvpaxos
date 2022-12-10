set datafile separator ','

set autoscale
set grid noxtic ytic
set key above opaque box

set key autotitle columnhead

set terminal pdf enhanced color dashed font "Times-Roman, 13"

# Reference: https://colorbrewer2.org/#type=diverging&scheme=Spectral&n=4
#blue = "rgb '#2b83ba'"
#green = "rgb '#abdda4'"
#orange = "rgb '#fdae61'"
#red = "rgb '#d7191c'"

set xrange [0:120]

set xlabel "Time (s)"
set ylabel "Throughput (Requests/s)"

set format y "%.0s%c"
set output "ycsb_A_throughput.pdf"
plot 'ycsb_A_throughput.csv' \
     using 1:2 with lines lc rgb '#0000ff', \
  '' using 1:3 with lines lc rgb "#abdda4", \
  '' using 1:4 with lines lc rgb "#fdae61", \
  '' using 1:5 with lines lc rgb  "#d7191c"

set output "ycsb_E_throughput.pdf"
plot 'ycsb_E_throughput.csv' \
    using 1:2 with lines ls 1 lc rgb '#0000ff', \
 '' using 1:3 with lines ls 2 lc rgb "#abdda4", \
 '' using 1:4 with lines ls 3 lc rgb "#fdae61", \
 '' using 1:5 with lines ls 4 lc rgb  "#d7191c"

set format y "%.1s%c"
set output "ycsb_D_throughput.pdf"
plot 'ycsb_D_throughput.csv' \
    using 1:2 with lines ls 1 lc rgb '#0000ff', \
 '' using 1:3 with lines ls 2 lc rgb "#abdda4", \
 '' using 1:4 with lines ls 3 lc rgb "#fdae61", \
 '' using 1:5 with lines ls 4 lc rgb  "#d7191c"

set xrange [0:70]
set format y "%.1s%c"
set output "ycsb_D_throughput_70s.pdf"
plot 'ycsb_D_throughput.csv' \
    using 1:2 with lines ls 1 lc rgb '#0000ff', \
 '' using 1:3 with lines ls 2 lc rgb "#abdda4", \
 '' using 1:4 with lines ls 3 lc rgb "#fdae61", \
 '' using 1:5 with lines ls 4 lc rgb  "#d7191c"
