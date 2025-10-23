"""
Stage to run STR analysis with STRipy-pipeline.

See https://gitlab.com/andreassh/stripy-pipeline
"""

from typing import Any

from cpg_flow import stage, targets
from cpg_utils import Path

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
            path, symbol, score = line.strip().split('\t')
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
    # analysis_type='web',
    # analysis_keys=[
    #     'html',
    # ],
    # update_analysis_meta=_update_meta,
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
            # 'html': sequencing_group.dataset.web_prefix() / 'stripy' / f'{sequencing_group.id}.stripy.html',
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


# TODO - the namespacing of these outputs should include the STRipy version, somehow
@stage.stage(
    analysis_type='web',
    analysis_keys=['global', 'default', 'neuro'],
    tolerate_missing_output=True,
    update_analysis_meta=_update_meta,
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
        outputs = {}
        web_prefix = sequencing_group.dataset.web_prefix()
        for ll in get_loci_lists(sequencing_group.dataset.name):
            if ll == 'global':
                outputs['global'] = web_prefix / 'stripy' / f'{sequencing_group.id}.stripy.html'
            else:
                outputs[ll] = web_prefix / 'stripy' / f'{sequencing_group.id}__{ll}.stripy.html'

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
