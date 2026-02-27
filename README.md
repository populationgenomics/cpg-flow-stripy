# CPG Flow Stripy
A CPG workflow for creating STR reports with STRipy, using the [cpg-flow](https://github.com/populationgenomics/cpg-flow) pipeline framework.

## Purpose

This workflow is designed to process short-read sequencing data and create reports with [STRipy](https://gitlab.com/andreassh/stripy-pipeline).

The short-read data is processed at the sequencing group level, and STRipy reports are generated for each sequencing group. The configuration allows complete customisation as to which STR loci are analysed and which reports are created.

## Example invocation

```bash
analysis-runner \
    --skip-repo-checkout \
    --image australia-southeast1-docker.pkg.dev/cpg-common/images/cpg-flow-stripy:0.3.0-1 \
    --config src/cpg_flow_stripy/config_template.toml \
    --config stripy_loci.toml \  # containing the inputs_cohorts and sequencing_type
    --dataset seqr \
    --description 'stripy' \
    --access-level standard \
    --output-dir stripy_run_<date> \
  run_workflow
```
# CPG Flow STRipy Pipeline

## Workflow Stages

1. RunStripy

   1. Call stripy to run STR analysis on all available loci. Produces a JSON & HTML with findings
   for all loci, but the HTML is not extracted.

2. MakeStripyReports:
   1. Leverage the Loci presets and the per-dataset subset selection config file (see [prod-pipes-config](github.com/populationgenomics/production-pipelines-configuration)).
   2. Ingest the all-Loci JSON file, and for each requested subset for a Dataset, create a separate HTML, containing only the appropriate Loci
   3. For each report record any instances of a Locus being selected, but not being available in the callset

3. MakeIndexPage
   1. DatasetStage, collects all reports generated for the SGs in a Dataset, and creates one Index page
   2. The index page contains the SG & Family ID, the report sub-type, the link to the report, and any missing loci relative to the documented Loci subset
   3. Two identical index pages are created, one called latest, that links to the fixed collaborator-facing path, and one with a loci version number for archival purposes
