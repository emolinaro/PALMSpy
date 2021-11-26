FROM continuumio/miniconda3:4.10.3

MAINTAINER Emiliano Molinaro <molinaro@imada.sdu.dk>

LABEL software="PALMSpy" \
      author="Emiliano Molinaro <molinaro@imada.sdu.dk>" \
      version="1.9.6" \
      license="MIT" \
      description="PALMSpy is an implementation of the Personal Activity and Location Measurement System (PALMS),\
                   written in Python and integrated with Apache Spark for cluster-computing.\
                   The program detects personal activity patterns of individual participants wearing\
                   a GPS data logger and a physical activity monitor."

ARG VERSION=${VERSION:-1.9.6}

SHELL ["/bin/bash",  "--login", "-c"]

## Install software
###################

RUN apt-get update \
 && apt-get install -y git vim nano emacs build-essential --no-install-recommends \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp

RUN git clone https://github.com/emolinaro/PALMSpy.git

WORKDIR PALMSpy

ENV PATH /opt/conda/envs/palmspy/bin:$PATH
ENV CONDA_PREFIX="/opt/conda/envs/palmspy"

RUN conda create -n palmspy python=3.7 openjdk=8.0 make=4.2.1 -y \
 && conda activate palmspy \
 && echo "source activate palmspy" > ~/.bashrc \
 && make \
 && make install \
 && make check \
 && make clean

WORKDIR /