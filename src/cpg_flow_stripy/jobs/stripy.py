"""
Create Hail Batch jobs to run STRipy
"""

import json
from typing import TYPE_CHECKING

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

    # when run in test we need to manually edit the dataset used in this query
    query_dataset = dataset
    if config.config_retrieve(['workflow', 'access_level']) == 'test' and 'test' not in query_dataset:
        query_dataset += '-test'

    result = query(PEDIGREE_QUERY, variables={'project': query_dataset})
    return {
        entry['id']: entry['sample']['participant']['families'][0]['externalId']
        for entry in result['project']['sequencingGroups']
    }


def run_stripy_pipeline(
    sequencing_group: targets.SequencingGroup,
    outputs: dict[str, Path],
    job_attrs: dict,
) -> 'BashJob':
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

    custom_loci_path = config.config_retrieve(['stripy', 'loci_lists', 'custom_loci_bed_file'])

    custom_loci_argument = ''
    if custom_loci_path:
        custom_loci_input = batch_instance.read_input(str(custom_loci_path))
        custom_loci_argument = f'--custom {custom_loci_input}'
    locus_arg = f'--locus {",".join(config.config_retrieve(["stripy", "loci_lists", "default"]))}'
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
        {locus_arg}


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
            --log_file {j.log_path}
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
    """Makes an index HTML page linking to all STRipy reports for a Dataset."""
    batch_instance = hail_batch.get_batch()

    j = batch_instance.new_bash_job(name=f'Make STRipy index page for {dataset_name}', attributes=job_attrs)
    j.image(config.config_retrieve(['workflow', 'driver_image']))

    # separate out all the real file paths from the log file paths - localise the log files
    local_log_files = [hail_batch.get_batch().read_input(output_dict.pop('log')) for output_dict in inputs.values()]

    # concatenate all those separate log files into a single log
    j.command(f'cat {" ".join(local_log_files)} > {j.biglog}')

    # for the remaining files, collect the SG, family ID, report type, and report Path - write to a temp file
    cpg_fam_mapping = get_cpg_to_family_mapping(dataset_name)

    file_prefix = config.config_retrieve(['storage', dataset_name, 'web'])
    html_prefix = config.config_retrieve(['storage', dataset_name, 'web_url'])

    # an object to store all the content we need to write
    collected_lines: list[str] = []
    for cpg_id, output_dict in inputs.items():
        # must find a family ID for this CPG ID
        fam_id = cpg_fam_mapping[cpg_id]

        for report_type, report_path in output_dict.items():
            # substitute the report HTML path for a proxy-rendered path
            corrected_path = str(report_path).replace(file_prefix, html_prefix)
            collected_lines.append(f'{cpg_id}\t{fam_id}\t{report_type}\t{corrected_path}')

    # write all reports to a single temp file, instead of passing an arbitrary number of CLI/script arguments
    with to_path(all_reports).open('w') as f:
        f.write('\n'.join(collected_lines))

    # localise that file
    mega_input_file = hail_batch.get_batch().read_input(all_reports)

    # --- Job Command (SINGLE STEP) ---
    # Runs your script, telling it to write to the local VM path
    j.command(f"""
        python3 -m cpg_flow_stripy.scripts.indexer \\
        --input_txt {mega_input_file} \\
        --output {j.index} \\
        --logfile {j.biglog}
    """)

    batch_instance.write_output(j.index, output)

    return j
