"""
Create Hail Batch jobs to run STRipy
"""

import json

import hailtop.batch as hb
from cpg_flow import targets
from cpg_utils import Path, config, hail_batch, to_path
from loguru import logger

from cpg_flow_stripy.scripts import make_stripy_html
from cpg_flow_stripy.utils import get_loci_lists

REPORT_TEMPLATE_PATH = './cpg_flow_stripy/stripy_report_template.html'


def run_stripy_pipeline(
    sequencing_group: targets.SequencingGroup,
    outputs: dict[str, Path],
    job_attrs: dict,
) -> hb.batch.job.Job:
    """
    Run STRipy
    """

    dataset = sequencing_group.dataset.name

    batch_instance = hail_batch.get_batch()

    j = batch_instance.new_job('STRipy', job_attrs | {'tool': 'stripy'})

    j.image(config.config_retrieve(['images', 'stripy']))
    j.cpu(4)

    config_path = 'config.json'
    # use a dataset config if available, else fall back to the standard config
    if stripy_config := config.config_retrieve(
        ['stripy', dataset, 'config'], config.config_retrieve(['stripy', 'config'])
    ):
        j.command('echo original config:')
        j.command(f'cat {config_path}')
        j.command(
            f"echo $(cat {config_path} | jq '. * $p' {config_path} --argjson p '{json.dumps(stripy_config)}') > $BATCH_TMPDIR/config_updated.json",  # noqa: E501
        )
        config_path = '$BATCH_TMPDIR/config_updated.json'

    reference = hail_batch.fasta_res_group(batch_instance)

    # Stripy accesses a relatively small number of discrete regions from each cram
    # accessing the cram via cloudfuse is faster than localising the full cram
    cram_path = sequencing_group.cram
    bucket = cram_path.path.drive
    print(f'bucket = {bucket}')
    bucket_mount_path = to_path('/bucket')
    j.cloudfuse(bucket, str(bucket_mount_path), read_only=True)
    mounted_cram_path = bucket_mount_path / '/'.join(cram_path.path.parts[2:])
    mounted_cram_index_path = f'{mounted_cram_path}.crai'

    sex_argument = ''
    if sequencing_group.pedigree.sex and str(sequencing_group.pedigree.sex).lower() != 'unknown':
        sex_argument = f'--sex {str(sequencing_group.pedigree.sex).lower()}'

    # smaller configuration - we keep a list of expanded loci, and can associate each with multiple datasets
    custom_loci_path = None
    for path, datasets in config.config_retrieve(['stripy', 'expanded_loci']).items():
        if dataset in datasets:
            custom_loci_path = path
            break

    custom_loci_argument = ''
    if custom_loci_path:
        custom_loci_input = batch_instance.read_input(str(custom_loci_path))
        custom_loci_argument = f'--custom {custom_loci_input}'

    cmd = f"""\
    cat {config_path}

    ln -s {mounted_cram_path} {sequencing_group.id}__{sequencing_group.external_id}.cram
    ln -s {mounted_cram_index_path} {sequencing_group.id}__{sequencing_group.external_id}.crai

    python3 stri.py \\
        --genome hg38 \\
        --reference {reference.base} \\
        {sex_argument} \
        --output $BATCH_TMPDIR/ \\
        --input {sequencing_group.id}__{sequencing_group.external_id}.cram  \\
        --logflags {j.log_path} \\
        --config {config_path} \\
        --analysis {config.config_retrieve(['stripy', 'analysis_type'])} {custom_loci_argument} \\
        --locus {config.config_retrieve(['stripy', 'target_loci'])}

    cp $BATCH_TMPDIR/{sequencing_group.id}__{sequencing_group.external_id}.cram.html {j.out_path}

    if [ -f $BATCH_TMPDIR/{sequencing_group.id}__{sequencing_group.external_id}.cram.json ]; then
        cp $BATCH_TMPDIR/{sequencing_group.id}__{sequencing_group.external_id}.cram.json {j.json_path}
    else
        touch {j.json_path}
    fi

    if [ ! -f {j.log_path} ]; then
        touch {j.log_path}
    fi
    """

    j.command(cmd)

    batch_instance.write_output(j.log_path, outputs['log'])
    batch_instance.write_output(j.json_path, outputs['json'])

    return j


def make_stripy_reports(
    sequencing_group: targets.SequencingGroup,
    json_path: Path,
    outputs: dict[str, Path],
    job_attrs: dict,
) -> list[hb.batch.job.Job]:
    """
    Make HTML reports for STRipy results, subsetting the full JSON results to
    only loci of interest, depending on the dataset of the input sequencing group.

    The subset JSONs are then used to populate HTML report templates.
    """
    loci_lists = get_loci_lists(sequencing_group.dataset.name)
    logger.info(f'{sequencing_group.id}: Making STRipy reports for loci lists: {",".join(loci_lists.keys())}')

    batch_instance = hail_batch.get_batch()
    j = batch_instance.new_job('Make STRipy reports', job_attrs | {'tool': 'stripy-report'})

    j.image(config.config_retrieve(['workflow', 'driver_image']))
    j.cpu(2)

    input_json = batch_instance.read_input(str(json_path))

    for loci_list_name, loci in loci_lists.items():
        # make a resource group for each loci list's JSON subset and HTML report
        j.declare_resource_group(
            **{
                loci_list_name: {
                    'json': '{root}.json',
                    'html': '{root}.html',
                },
            },
        )
        loci_str = ','.join(loci)

        # Subset the full loci JSON to just the loci in this list
        # TODO maybe do this with python instead of jq?
        j.command(f"cat {input_json} | jq '.loci |= map(select(.name | IN({loci_str})))' > {j[loci_list_name].json}")

        # Now copy template HTML report and populate with subset JSON
        j.command(
            f'python3 {make_stripy_html.__file__} --results {j[loci_list_name].json} --output {j[loci_list_name].html}'
        )

        batch_instance.write_output(j[loci_list_name].json, outputs['json'][loci_list_name])
        batch_instance.write_output(j[loci_list_name].html, outputs['html'][loci_list_name])

    return [j]
