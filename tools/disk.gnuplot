#! gnuplot

set output "disk.png"
set term png size 14000,900 small
set termoption enhanced
set xrange [0:*]
set xlabel "timestamp"
set ylabel "offset"
set format y "%6.0fMB"
set format x "%6.1fmin"
set key off
plot "disk.log" using ($1/1000/60):($2/1024/1024):($2/1024/1024):($3/1024/1024):($2/1024/1024):5 with candlesticks lc variable

set output "disk-reads.png"
plot "disk.log" using ($4 == 0 ? ($1/1000/60) : 1/0):($2/1024/1024):($2/1024/1024):($3/1024/1024):($2/1024/1024):5 with candlesticks lc variable

set output "disk-writes.png"
plot "disk.log" using ($4 == 1 ? ($1/1000/60) : 1/0):($2/1024/1024):($2/1024/1024):($3/1024/1024):($2/1024/1024):5 with candlesticks lc variable

set output "disk-overview.png"
set term png size 1400,700 small
plot "disk.log" using ($1/1000/60):($2/1024/1024):($2/1024/1024):($3/1024/1024):($2/1024/1024):5 with candlesticks lc variable
