# CPG Flow Stripy

A CPG workflow for creating STR reports with STRipy, using the [cpg-flow](https://github.com/populationgenomics/cpg-flow) pipeline framework.

## Issues

Grabbing the Missing Genes from STRipy can be problematic
Missing genes are currently embedded in the HTML, this can be remedied by modifyying the pipeline to generate a separate text file with the missing genes and then grabbing those.

## Purpose

This workflow is designed to process short-read sequencing data and create reports with [STRipy](https://gitlab.com/andreassh/stripy-pipeline).

The short-read data is processed at the sequencing group level, and STRipy reports are generated for each sequencing group. The configuration allows complete customisation as to which STR loci are analysed and which reports are created.

## Requirements

- Clone the production-pipelines-configuration repository to a known location and ensure the `stripy` config files are used in the analysis-runner submission.

#CPG Flow STRipy Pipeline
## summary
This pipeline performs Short Tandem Repeat (STR) analysis using the STRipy pipeline on short-read sequencing data.
## stages
1. RunStripy:
Call stripy to run STR analysis on all available loci. Produces a JSON with findings
for all loci, which can then be subset to specific loci of interest and used to create
HTML reports.

Job: stripy.run_stripy_pipeline() at src/cpg_flow_stripy/jobs/stripy.py
Script: No script, stripy is called directly from the driver
Outputs:
STR analysis results in JSON format
Analysis log file

2. MakeStripyReports:
Create dataset-specific HTML reports by subsetting the comprehensive JSON results to focus on loci of interest.
Different loci lists are applied based on the dataset type, generating multiple targeted reports per sequencing group.

Job: stripy.make_stripy_reports() src/cpg_flow_stripy/jobs/stripy.py
Script: src/cpg_flow_stripy/scripts/subsetting_jsons.py
Analysis Type: web
Output Keys: global, default, default_with_exclusions, neuro_with_research_inclusions, paediatric, kidney
Dependencies: Requires RunStripy stage
Creates dataset-specific HTML reports by subsetting the comprehensive JSON results to focus on loci of interest. Different loci lists are applied based on the dataset type, generating multiple targeted reports per sequencing group.
Outputs:Multiple HTML reports filtered by loci list type

3. MakeIndexPage
Aggregates all individual STR reports into a centralized HTML index page, providing navigation and summary views across all sequencing groups in the dataset. Creates a unified interface for accessing and comparing STR analysis results.
Job: stripy.make_index_page() at src/cpg_flow_stripy/jobs/stripy.py
Script: src/cpg_flow_stripy/scripts/indexer.py
Outputs:
Consolidated HTML index page
Cross-sample navigation interface
Dataset-level STR analysis summary

