<a href="https://zenodo.org/badge/latestdoi/183218159"><img src="https://zenodo.org/badge/183218159.svg" alt="DOI"></a>

# PALMSpy

PALMSpy detects personal activity patterns of individual participants wearing a GPS data logger and a physical activity monitor.

## Build dependencies

- Conda v4.10.x
- Make v4.2
- OpenJDK v8.0
- Python v3.7

## Installation instructions

#### Create a conda environment based on Python v3.7.x
```bash
conda create -n palmspy python=3.7 openjdk=8.0 make=4.2.1 -y
conda activate palmspy
```

#### Build and install the package
```bash
make
make install
```

#### Run a simulation for testing
```bash
make test
```

#### Remove build artifacts 
```bash
make clean
```
