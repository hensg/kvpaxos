set datafile separator ','

set key autotitle columnhead
set key top right opaque box

set ylabel "Elapsed time (s)"

set style data histogram
set style histogram cluster gap 1
set style fill solid

set boxwidth 0.9

set grid ytics

set yrange [0:]

unset xtics

set terminal pdf enhanced color dashed font "Times-Roman, 13"

do for [name in "A D E"] {
  set output sprintf('ycsb_%s_makespan.pdf', name)
  plot sprintf('ycsb_%s_makespan.csv', name) \
       using ($1/1e9) lc rgb '#0000ff', \
    '' using ($2/1e9) lc rgb "#abdda4", \
    '' using ($3/1e9) lc rgb "#fdae61", \
    '' using ($4/1e9) lc rgb "#d7191c"
}

