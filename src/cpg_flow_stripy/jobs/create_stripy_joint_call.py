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

    job.declare_resource_group(
        output={
            'vcf.gz': '{root}.vcf.gz',
            'vcf.gz.tbi': '{root}.vcf.gz.tbi'
        },
    )

    job.command(
        f"""
        python -m cpg_flow_stripy.scripts.stripy_to_vcf \\
            --json {' '.join(localised_jsons)} \\
            --output {job.output['vcf.gz']} \\
            --mapping {localised_mapping}
        tabix {job.output['vcf.gz']}
        """
    )

    # maybe tabix the file? that requires an extra tool added to the image and CBA right now
    batch.write_output(job.output, str(output).removesuffix('.vcf.gz'))

    return job
