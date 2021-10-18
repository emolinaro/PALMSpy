<a href="https://zenodo.org/badge/latestdoi/183218159"><img src="https://zenodo.org/badge/183218159.svg" alt="DOI"></a>

# PALMSpy

The program detects personal activity patterns of individual participants wearing
a GPS data logger and a physical activity monitor.

## Installation instructions

#### Create a conda environment based on Python v3.7.x
```bash
conda create -n palmspy python=3.7 openjdk=8.0 -y
conda activate palmspy
```

#### Install dependencies
```bash
pip install -r requirements.txt
```

#### Generate the latest release wheel
```bash
bash gen_package
```
The wheel file is created in the folder `./dist`.

#### Install the program
```bash
pip install ./dist/habitus-x.y.z-py3-none-any.whl
```
