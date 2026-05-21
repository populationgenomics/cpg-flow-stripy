"""
Job creator - runs the STRipy JSONs to multisample VCF representation.
"""

from typing import TYPE_CHECKING

from cpg_utils import Path, config, hail_batch

if TYPE_CHECKING:
    from hailtop.batch.job import BashJob


def create_joint_call(json_paths: dict[str, Path], gene_lookup: Path, output: Path) -> 'BashJob':
    """Run the GFF to JSON conversion script."""
    batch = hail_batch.get_batch()
    job = batch.new_bash_job(name='Run JSON to multisample VCF conversion')
    job.image(config.config_retrieve(['workflow', 'driver_image']))
    job.storage(config.config_retrieve(['stripy_vcf', 'storage']))

    localised_jsons = [batch.read_input(jpath) for jpath in json_paths.values()]

    localised_mapping = batch.read_input(gene_lookup)

    job.output.add_extension('.vcf.gz')

    job.command(
        f"""
        python -m cpg_flow_stripy.scripts.stripy_to_vcf \\
            --json {' '.join(localised_jsons)} \\
            --output {job.output} \\
            --mapping {localised_mapping}
        """
    )

    # maybe tabix the file? that requires an extra tool added to the image and CBA right now
    batch.write_output(job.output, output)

    return job
