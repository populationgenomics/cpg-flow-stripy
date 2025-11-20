"""
Create Hail Batch jobs to run STRipy
"""

import json
from typing import TYPE_CHECKING

from cpg_flow_stripy.scripts import indexer, subsetting_jsons
from cpg_flow.workflow import path_walk

if TYPE_CHECKING:
    from hailtop.batch.job import Job

    class Targets:
        class SequencingGroup:
            id: str
            dataset: str
            name: str


import hailtop.batch as hb
from cpg_flow import targets
from cpg_utils import Path, config, hail_batch, to_path
from metamist.graphql import gql, query

from cpg_flow_stripy.utils import get_loci_lists

if TYPE_CHECKING:
    from hailtop.batch.job import BashJob

# not used, needs correction
REPORT_TEMPLATE_PATH = './cpg_flow_stripy/stripy_report_template.html'

PEDIGREE_QUERY = gql(
    """
query Pedigree($project: String!) {
  project(name: $project) {
    sequencingGroups {
      id
      sample {
        participant {
          families {
            externalId
          }
        }
      }
    }
  }
}""",
)


def get_cpg_to_family_mapping(dataset: str) -> dict[str, str]:
    """Get the CPG ID to external Family ID mapping for all members of this dataset, cached per dataset."""
    result = query(PEDIGREE_QUERY, variables={'project': dataset})
    return {
        entry['id']: entry['sample']['participant']['families'][0]['externalId']
        for entry in result['project']['sequencingGroups']
    }


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

    custom_loci_path = config.config_retrieve(['stripy','loci_lists','custom_loci_bed_file'])

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
) -> 'BashJob':
    """
    Makes HTML reports for STRipy results using the all-in-one
    subsetting_jsons.py script inside an Exomiser-configured job.
    """
    loci_lists = get_loci_lists(sequencing_group.dataset.name)
    batch_instance = hail_batch.get_batch()

    j = hail_batch.get_batch().new_bash_job(name=f'Make STRipy reports for {sequencing_group.id}', attributes=job_attrs)
    j.image(config.config_retrieve(['workflow', 'driver_image']))

    input_json = batch_instance.read_input(json_path)

    for loci_list_name, loci in loci_lists.items():
        resource_group = j[loci_list_name]
        loci_str = ' '.join(loci)

        # --- Job Command (SINGLE STEP) ---
        # Runs your script, telling it to write to the local VM path
        j.command(f"""
            python3 -m cpg_flow_stripy.scripts.subsetting_jsons \\
            --input_json {input_json} \\
            --report_type {loci_list_name} \\
            --loci_list {loci_str} \\
            --output {resource_group} \\
            --logfile {j.log_path}
        """)

        # Get the *exact file path* from the flat dictionary
        target_cloud_path = str(outputs[loci_list_name])

        batch_instance.write_output(
            resource_group,  # <-- Write the specific FILE
            target_cloud_path,  # <-- To the specific exact PATH
        )

    # after all commands are executed, extract a log file
    batch_instance.write_output(j.log_path, outputs['log'])

    return j


def make_index_page(
    dataset_name: str,
    inputs: dict[str, dict[str, Path]],
    output: Path,
    all_reports: str,
    job_attrs: dict,
) -> 'BashJob':
    """
    Makes an index HTML page linking to all STRipy reports for a sequencing group.
    """
    batch_instance = hail_batch.get_batch()

    old_suffix = config.config_retrieve(['storage', bucket_name, 'web'])
    new_suffix = config.config_retrieve(['storage', bucket_name, 'web_url'])
    list_of_suffixes = [old_suffix, new_suffix]

    j = batch_instance.new_bash_job(name=f'Make STRipy index page for {dataset.id}', attributes=job_attrs)
    j.image(config.config_retrieve(['workflow', 'driver_image']))

    report_links = path_walk(inputs)
    inputs_files = ' '.join([ f for f in report_links])
    web_report_links = [str(p).replace(old_suffix, new_suffix) for p in report_links]

    # --- Job Command (SINGLE STEP) ---
    # Runs your script, telling it to write to the local VM path
    j.command(
        f'cd $BATCH_TMPDIR'
        f'python3 {indexer.__file__}'
        f'--txt_file_paths {inputs_files} '
        f'--output_root {output} '
        f'--web_report_name {web_report_links} '
    )

    batch_instance.write_output(
        j.out_path,
        str(output),
    )

    return j
