import sys

f = open(sys.argv[1], 'r')
print('opening %s' % sys.argv[1])

stats = {}

filenames = {}

for l in f:
	if l.startswith('#'):
		filenum, filename = l[1:].strip().split(' ')
		filenames[int(filenum)] = filename
		continue

	time, offset, end, rw, f = l.split('\t')

	size = int(end) - int(offset)

	f = int(f.strip())
	name = filenames[f]
	if not name in stats:
		stats[name] = {'total_write':0, 'total_read':0}

	if rw == '1':
		stats[name]['total_write'] += int(size)
	elif rw == '0':
		stats[name]['total_read'] += int(size)

log = sorted(stats.items(), key=lambda x: x[0])

total_read = 0
total_write = 0

for n,s in log:
	print('%40s -  read: %13d write: %13d' % (n, s['total_read'], s['total_write']))
	total_read += s['total_read']
	total_write += s['total_write']

print('total-read:  %16d' % total_read)
print('total-write: %16d' % total_write)
