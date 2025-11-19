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
    """
    Add the detected outlier loci to the analysis meta
    """
    from cloudpathlib.anypath import to_anypath  # noqa: PLC0415

    # Munge html path into log path (As far as I can know I can not pass to
    # output paths to one analysis object?)
    log_path = output_path.replace('-web/', '-analysis/').replace('.html', '.log.txt')

    outlier_loci = {}
    with to_anypath(log_path).open() as f:
        for line in f:
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
        analysis_prefix = sequencing_group.dataset.analysis_prefix()
        return {
            'json': analysis_prefix / 'stripy' / f'{sequencing_group.id}.stripy.json',
            'log': analysis_prefix / 'stripy' / f'{sequencing_group.id}.stripy.log.txt',
        }

    def queue_jobs(self, sequencing_group: targets.SequencingGroup, inputs: stage.StageInput) -> stage.StageOutput:
        outputs = self.expected_outputs(sequencing_group)
        j = stripy.run_stripy_pipeline(
            sequencing_group=sequencing_group,
            outputs=outputs,
            job_attrs=self.get_job_attrs(sequencing_group),
        )

        return self.make_outputs(sequencing_group, data=outputs, jobs=[j])


@stage.stage(
#    analysis_type='web',
#    analysis_keys=[
#        'global',
#        'default',
#        'default_with_exclusions',
#        'neuro_with_research_inclusions',
#        'paediatric',
#        'kidney',
#    ],
#    tolerate_missing_output=True,
#    update_analysis_meta=_update_meta,
    required_stages=[RunStripy],
)
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
        outputs = {}
        web_prefix = sequencing_group.dataset.web_prefix()
        for ll in get_loci_lists(sequencing_group.dataset.name):
            outputs[ll] = web_prefix / 'stripy' / f'{loci_version}' / f'{sequencing_group.id}__{sequencing_group.pedigree.}__{ll}.html'

        return outputs

    def queue_jobs(self, sequencing_group: targets.SequencingGroup, inputs: stage.StageInput) -> stage.StageOutput:
        outputs = self.expected_outputs(sequencing_group)
        jobs = stripy.make_stripy_reports(
            sequencing_group=sequencing_group,
            json_path=inputs.as_path(sequencing_group, RunStripy, 'json'),
            outputs=outputs,
            job_attrs=self.get_job_attrs(sequencing_group),
        )

        return self.make_outputs(sequencing_group, data=outputs, jobs=jobs)


@stage.stage(
    analysis_type='web',
    analysis_keys=['index'],
    tolerate_missing_output=True,
    update_analysis_meta=_update_meta,
    required_stages=[MakeStripyReports],
)
class MakeIndexPage(stage.DatasetStage ):
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
        outputs = web_prefix / 'stripy' / f'{loci_version}' / f'{dataset}_index.html'
        return {'index': outputs}

    def queue_jobs(self, dataset: targets.Dataset, inputs: stage.StageInput) -> stage.StageOutput:
        outputs_previous_stage = inputs.as_dict_by_target(MakeStripyReports)
        outputs = self.expected_outputs(dataset)
        jobs = stripy.make_index_page(
            dataset=dataset,
            inputs=outputs_previous_stage,
            outputs=outputs,
            job_attrs=self.get_job_attrs(dataset),
        )

        return self.make_outputs(dataset, data=outputs, jobs=jobs)
