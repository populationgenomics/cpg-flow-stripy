"""
Stage to run STR analysis with STRipy-pipeline.

See https://gitlab.com/andreassh/stripy-pipeline
"""

from typing import Any

from cpg_flow import stage, targets
from cpg_utils import Path

from cpg_flow_stripy.jobs import stripy


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
    analysis_type='web',
    analysis_keys=[
        'html',
    ],
    update_analysis_meta=_update_meta,
)
class RunStripy(stage.SequencingGroupStage):
    """
    Call stripy to run STR analysis on known pathogenic loci.

    Changed behaviour - this step does as much calling as possible. Instead of taking a subset of Loci, this would
    generate the most expansive set of results for a given version of STRipy

    A next Stage would optionally generate subset reports (e.g. late-onset, incidental-findings), with each of those
    results (JSON, HTML) being saved in a separate namespace so we know the locus list which was used

    The intention would be to do as much calling as possible with each run of STRipy, making most efficient use of the
    'warmed up' CRAM in GCS, then to generate subsets of that data as HTML/JSON, we only have to access a tiny file,
    with negligible cost implications
    """

    def expected_outputs(self, sequencing_group: targets.SequencingGroup) -> dict[str, Path]:
        analysis_prefix = sequencing_group.dataset.analysis_prefix()
        return {
            'html': sequencing_group.dataset.web_prefix() / 'stripy' / f'{sequencing_group.id}.stripy.html',
            'json': analysis_prefix / 'stripy' / f'{sequencing_group.id}.stripy.json',
            'log': analysis_prefix / 'stripy' / f'{sequencing_group.id}.stripy.log.txt',
        }

    def queue_jobs(self, sequencing_group: targets.SequencingGroup, inputs: stage.StageInput) -> stage.StageOutput:
        outputs = self.expected_outputs(sequencing_group)
        jobs = []
        j = stripy.run_stripy_pipeline(
            sequencing_group=sequencing_group,
            outputs=outputs,
            job_attrs=self.get_job_attrs(sequencing_group),
        )
        jobs.append(j)

        return self.make_outputs(sequencing_group, data=outputs, jobs=jobs)
