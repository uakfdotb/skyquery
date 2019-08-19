import subprocess

out_fname = 'jun24-512-1min-60cap-long/ttl-{}drone_predictions.json'
gt_fname = 'gt_counts_long.json'

for n in [1, 2, 3, 4, 5, 6]:
	output = subprocess.check_output(['go', 'run', 'eval-accuracy.go', out_fname.format(n), gt_fname])
	parts = output.strip().split(', ')
	parts = [x.split('=')[1] for x in parts]
	print "\t".join([str(n)] + parts)
