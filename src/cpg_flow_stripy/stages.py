"""
Stage to run STR analysis with STRipy-pipeline.

See https://gitlab.com/andreassh/stripy-pipeline
"""

from typing import Any

from cpg_flow import stage, targets
from cpg_utils import Path, config

from cpg_flow_stripy.jobs import stripy
from cpg_flow_stripy.utils import get_loci_lists


def _update_meta(output_path: str) -> dict[str, Any]:
    """Add the detected outlier loci to the analysis meta."""
    from cloudpathlib.anypath import to_anypath  # noqa: PLC0415

    # Munge JSON path into log path
    log_path = output_path.replace('.json', '.log.txt')

    outlier_loci = {}
    with to_anypath(log_path).open() as f:
        for line in f:
            if not line.strip().rstrip():
                continue
            _path, symbol, score = line.strip().split('\t')
            if not score.isdigit():
                continue
            if int(score) > 0:
                outlier_loci[symbol] = score

    return {
        'outlier_loci': outlier_loci,
        'outliers_detected': bool(outlier_loci),
        'log_path': log_path,
    }


@stage.stage(
    analysis_type='web',
    analysis_keys=['json'],
    update_analysis_meta=_update_meta,
)
class RunStripy(stage.SequencingGroupStage):
    """
    Call stripy to run STR analysis on all available loci. Produces a JSON with findings
    for all loci, which can then be subset to specific loci of interest and used to create
    HTML reports.
    """

    def expected_outputs(self, sequencing_group: targets.SequencingGroup) -> dict[str, Path]:
        web_prefix = sequencing_group.dataset.web_prefix()
        analysis_prefix = sequencing_group.dataset.analysis_prefix()
        return {
            'json': analysis_prefix / 'stripy' / f'{sequencing_group.id}.stripy.json',
            'log': analysis_prefix / 'stripy' / f'{sequencing_group.id}.stripy.log.txt',
            'html': web_prefix / 'stripy' / f'{sequencing_group.id}.stripy.html',
        }

    def queue_jobs(self, sequencing_group: targets.SequencingGroup, inputs: stage.StageInput) -> stage.StageOutput:
        outputs = self.expected_outputs(sequencing_group)
        j = stripy.run_stripy_pipeline(
            sequencing_group=sequencing_group,
            outputs=outputs,
            job_attrs=self.get_job_attrs(sequencing_group),
        )

        return self.make_outputs(sequencing_group, data=outputs, jobs=[j])


@stage.stage(required_stages=RunStripy)
class MakeStripyReports(stage.SequencingGroupStage):
    """
    Create HTML reports for STRipy analysis, subsetting the full JSON results to
    only loci of interest, depending on the dataset of the input sequencing group.
    """

    def expected_outputs(self, sequencing_group: targets.SequencingGroup) -> dict[str, Path]:
        """
        Get the expected output paths for the HTML reports - there can be multiple,
        depending on how many distinct loci lists are in scope for the dataset.
        """
        loci_version = str(config.config_retrieve(['stripy', 'loci_version']))

        std_prefix = sequencing_group.dataset.prefix()
        web_prefix = sequencing_group.dataset.web_prefix()

        return {
            **{
                ll: web_prefix / 'stripy' / loci_version / f'{sequencing_group.id}__{ll}.html'
                for ll in get_loci_lists(sequencing_group.dataset.name)
            },
            'log': std_prefix / 'stripy' / loci_version / f'{sequencing_group.id}.log',
        }

    def queue_jobs(self, sequencing_group: targets.SequencingGroup, inputs: stage.StageInput) -> stage.StageOutput:
        outputs = self.expected_outputs(sequencing_group)
        jobs = stripy.make_stripy_reports(
            sequencing_group=sequencing_group,
            json_path=inputs.as_path(sequencing_group, RunStripy, 'json'),
            outputs=outputs,
            job_attrs=self.get_job_attrs(sequencing_group),
        )

        return self.make_outputs(sequencing_group, data=outputs, jobs=jobs)


@stage.stage(analysis_type='web', analysis_keys=['index'], required_stages=[MakeStripyReports], forced=True)
class MakeIndexPage(stage.DatasetStage):
    """
    Create HTML reports for STRipy analysis, subsetting the full JSON results to
    only loci of interest, depending on the dataset of the input sequencing group.
    """

    def expected_outputs(self, dataset: targets.Dataset) -> dict[str, Path]:
        """
        Get the expected output paths for the HTML reports - there can be multiple,
        depending on how many distinct loci lists are in scope for the dataset.
        """
        loci_version = str(config.config_retrieve(['stripy', 'loci_version']))
        web_prefix = dataset.web_prefix()
        return {
            'index': web_prefix / 'stripy' / loci_version / f'{dataset.name}_index.html',
            'latest': web_prefix / 'stripy' / f'{dataset.name}_index.html',
        }

    def queue_jobs(self, dataset: targets.Dataset, inputs: stage.StageInput) -> stage.StageOutput:
        outputs = self.expected_outputs(dataset)

        # all outputs from previous stage, e.g. {"CPGIII": {'log': ..., 'neuro': ...}}
        all_outputs_previous_stage = inputs.as_dict_by_target(MakeStripyReports)

        # reduce _all_ previous stage outputs to just the ones in this dataset
        dataset_outputs_previous_stage = {
            key: value for key, value in all_outputs_previous_stage.items() if key in dataset.get_sequencing_group_ids()
        }

        job = stripy.make_index_page(
            dataset_name=dataset.name,
            inputs=dataset_outputs_previous_stage,
            output_archive=outputs['index'],
            output_latest=outputs['latest'],
            all_reports=str(dataset.tmp_prefix() / 'stripy' / dataset.get_alignment_inputs_hash() / 'all_reports.txt'),
            job_attrs=self.get_job_attrs(dataset),
        )

        return self.make_outputs(dataset, data=outputs, jobs=job)
