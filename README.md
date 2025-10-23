# CPG Flow Stripy

A CPG workflow for creating STR reports with STRipy, using the [cpg-flow](https://github.com/populationgenomics/cpg-flow) pipeline framework.

## Purpose

This workflow is designed to process short-read sequencing data and create reports with [STRipy](https://gitlab.com/andreassh/stripy-pipeline).

The short-read data is processed at the sequencing group level, and STRipy reports are generated for each sequencing group. The configuration allows complete customisation as to which STR loci are analysed and which reports are created.

## Requirements

- Clone the production-pipelines-configuration repository to a known location and ensure the `stripy` config files are used in the analysis-runner submission.
