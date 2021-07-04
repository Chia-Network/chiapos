### Run
run plotter

```bash
# phase 1
docker run -v $(pwd):/tmp duolacloud/chiapos:latest /app/ProofOfSpace -k 25 -f "plot.dat" -m "0x1234" -h 1 -e -t /tmp create

# phase 2
docker run -v $(pwd):/tmp duolacloud/chiapos:latest /app/ProofOfSpace -k 25 -f "plot.dat" -m "0x1234" -h 2 -e -t /tmp create
```
