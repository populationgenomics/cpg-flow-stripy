"""
Job creator - runs the GFF to JSON conversion script.
"""

from typing import TYPE_CHECKING

from cpg_utils import Path, config, hail_batch

if TYPE_CHECKING:
    from hailtop.batch.job import BashJob


def create_gff_conversion_job(output: Path) -> 'BashJob':
    """Run the GFF to JSON conversion script."""
    batch = hail_batch.get_batch()
    job = batch.new_bash_job(name='Run GFF to JSON conversion script')
    job.image(config.config_retrieve(['workflow', 'driver_image']))
    job.storage('10GiB')

    localised_gff = batch.read_input(config.config_retrieve(['references', 'ensembl_gff3']))

    job.command(f'python -m cpg_flow_stripy.scripts.parse_gff_into_json --gff3 {localised_gff} --output {job.output}')

    batch.write_output(job.output, output)

    return job
