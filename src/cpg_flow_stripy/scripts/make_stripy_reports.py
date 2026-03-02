# ruff: noqa: C901
# ruff: noqa: PLR0912
# ruff: noqa: PLR0915
import copy
import json
from argparse import ArgumentParser

# used to navigate from the installed location of this package to the HTML template file
from importlib import resources
from pathlib import Path

import loguru

DEFAULT_REPORT_SCHEMA = {
    'GenotypingResults': [
        {
            'DefaultGeneTemplate': {  # Use a generic key to represent the dynamic gene name
                'Metadata': {
                    'HighestPathRepeatsInFlanking': {},
                    'TotalOfFlankingReads': [],
                    'TotalOfInrepeatReads': [],
                    'TotalOfSpanningReads': [],
                },
                'TargetedLocus': {
                    'LocusID': 'N/A',
                    'Coordinates': 'N/A',
                    'Motif': 'N/A',
                    'CorrespondingDisease': {
                        # This nested structure provides the necessary keys
                        'DefaultDiseaseEntry': {
                            'DiseaseName': 'N/A',
                            # Using 0 as default for IntermediateRange per your request
                            'IntermediateRange': 0,
                            'NormalRange': {'Max': 0, 'Min': 0},
                            'PathogenicCutoff': 0,
                            'Inheritance': 'NI',
                        }
                    },
                },
            }
        }
    ],
    'JobDetails': {
        'TimeOfAnalysis': 'N/A',
        'InputFile': 'N/A',
        'Reference': 'N/A',
        'TargetedLoci': [],
        'MissingGenes': [],
        'SampleSex': 'Unknown',
    },
}


def deep_merge_defaults(target_dict: dict, default_dict: dict) -> dict:
    """
    Optimized recursive merge of a target dictionary with a default schema.

    Includes specific logic for 'DefaultDiseaseEntry' and 'DefaultGeneTemplate'.
    """
    # Fast failure: if types don't match or aren't dicts, return immediately
    if not isinstance(target_dict, dict) or not isinstance(default_dict, dict):
        return target_dict

    for key, default_val in default_dict.items():
        # Optimization: Fetch target value once to avoid multiple hash lookups
        target_val = target_dict.get(key)

        # --- Case 1: Missing or None ---
        if target_val is None:
            # Optimization: Only deepcopy mutable types (dicts/lists).
            # Primitives (int, str) are immutable and safe to assign directly.
            if isinstance(default_val, dict | list):
                target_dict[key] = copy.deepcopy(default_val)
            else:
                target_dict[key] = default_val
            continue

        # --- Case 2: Both are Dictionaries ---
        if isinstance(default_val, dict) and isinstance(target_val, dict):
            # Special Logic: DefaultDiseaseEntry
            # Check length first (O(1)) to avoid string lookup if unnecessary
            if len(default_val) == 1 and 'DefaultDiseaseEntry' in default_val:
                template_content = default_val['DefaultDiseaseEntry']
                # Apply template to all dynamic sub-keys in the target
                for _sub_key, sub_val in target_val.items():
                    if isinstance(sub_val, dict):
                        # Mutate in place rather than re-assigning to save overhead
                        deep_merge_defaults(sub_val, template_content)
            else:
                # Standard Recursive Merge
                deep_merge_defaults(target_val, default_val)

        # --- Case 3: Both are Lists ---
        elif isinstance(default_val, list) and isinstance(target_val, list) and default_val:
            default_template = default_val[0]

            # Check for Special Logic: DefaultGeneTemplate
            # lifting the extraction of template_content out of the loop
            inner_template = None
            if (
                isinstance(default_template, dict)
                and len(default_template) == 1
                and 'DefaultGeneTemplate' in default_template
            ):
                inner_template = default_template['DefaultGeneTemplate']

            # Iterate target list
            for item in target_val:
                if not isinstance(item, dict):
                    continue

                if inner_template:
                    # Dynamic Key Strategy (Genes)
                    for gene_data in item.values():
                        if isinstance(gene_data, dict):
                            deep_merge_defaults(gene_data, inner_template)
                else:
                    # Standard List Merge
                    deep_merge_defaults(item, default_template)

    return target_dict


def main(
    input_json: str,
    output: str,
    external_id: str,
    report_type: str,
    loci_list: str,
    subset_svg_flag: int,
    logfile: str,
) -> None:
    # Extract sampleID from filename (e.g., "CPG276402.stripy.json" -> "CPG276402")
    filename = Path(input_json).name
    sample_id = filename.split('.')[0]

    loguru.logger.info(f'Sample ID: {sample_id}')
    loguru.logger.info(f'Report type: {report_type}')

    # --- Load Input Data ---
    with open(input_json) as f:
        data = json.load(f)

    loguru.logger.info(f"  Relevant loci for '{report_type}': {loci_list}")

    listofdictionarydata = data['GenotypingResults']
    list_of_available_genes = [list(d.keys())[0] for d in listofdictionarydata]
    missing_genes = [gene for gene in loci_list if gene not in list_of_available_genes]
    stripyanalysis_time = data.get('JobDetails', {}).get('TimeOfAnalysis', 'N/A')

    loguru.logger.info(f'  Available loci in input JSON: {list_of_available_genes}')
    loguru.logger.info(f'  Missing loci for this report: {missing_genes}')

    dict_of_dicts = {list(d.keys())[0]: d for d in listofdictionarydata}
    subset_dict = {k: v for k, v in dict_of_dicts.items() if k in loci_list}
    subset_list = [v for k, v in subset_dict.items()]

    # Create a temporary copy for this report type's results
    temp_data = data.copy()
    temp_data['GenotypingResults'] = subset_list

    genotyping_results = temp_data['GenotypingResults']
    sample_sex = temp_data.get('JobDetails', {}).get('SampleSex', 'Unknown')
    loci_of_interest = {}
    for locus_item in genotyping_results:
        # Each locus_item is a dictionary with one key (the Locus ID)
        # Use .items() to get the (key, value) pair. Since there's only one,
        # the loop will run exactly once per item.
        for locus_id, details_dict in locus_item.items():
            flag_status = details_dict.get('Flag', 0)
            allele_status_list = details_dict.get('Alleles', [])
            coords = details_dict.get('TargetedLocus', {}).get('Coordinates') or ''
            ischromx = coords.startswith('chrX')
            allele_flag = ''
            allele_pop_outlier_counter = 0
            for allele_dict in allele_status_list:
                allele_flag += allele_dict.get('Range', '')
                if allele_dict.get('IsPopulationOutlier') is True:
                    allele_pop_outlier_counter += 1

            # You can add conditional logic here, e.g., to check for non-zero flags
            if flag_status < subset_svg_flag and 'SVG' in details_dict:
                del details_dict['SVG']

            if flag_status == 3:
                loci_of_interest[locus_id] = 'Red'
                continue

            if flag_status == 1 and ('pathogenic' in allele_flag) and sample_sex == 'Male' and ischromx:
                # Edge case for X-linked pathogenic variants in Males, which should be flagged as red, not pink
                loci_of_interest[locus_id] = 'Red'
                continue

            if flag_status == 2:
                loci_of_interest[locus_id] = 'Orange'
                continue

            if flag_status == 1 and ('pathogenic' in allele_flag):
                loci_of_interest[locus_id] = 'Pink'
                continue

            if flag_status == 1 and allele_pop_outlier_counter > 0:
                loci_of_interest[locus_id] = 'Grey'

    # log the missing genes
    with open(logfile, 'a') as handle:
        line_to_write = ','.join(f'{locus}:{color}' for locus, color in loci_of_interest.items())
        if missing_genes:
            handle.write(
                f'{sample_id}\t{report_type}\t{external_id}\t{stripyanalysis_time}\t{", ".join(missing_genes)}'
                f'\t{line_to_write}\n'
            )
        else:
            handle.write(f'{sample_id}\t{report_type}\t{external_id}\t{stripyanalysis_time}\tNone\t{line_to_write}\n')

    temp_data['GenotypingResults'] = genotyping_results
    temp_data['JobDetails'] = temp_data['JobDetails'].copy()
    temp_data['JobDetails']['TargetedLoci'] = loci_list
    temp_data['JobDetails']['MissingGenes'] = missing_genes

    # --- Ensure All Required Fields are Present ---

    temp_data = deep_merge_defaults(temp_data, DEFAULT_REPORT_SCHEMA)

    # --- Generate HTML Output ---
    # This script writes the final HTML to the --output path
    with (
        (resources.files('cpg_flow_stripy') / 'results_template.html').open() as results_html_template,
        open(output, 'w') as output_html_file,
    ):
        for line in results_html_template:
            # double replace, single write
            output_html_file.write(line.replace('/*SampleResultsJSON*/', json.dumps(temp_data, indent=4)))

    loguru.logger.info(f'  HTML file generated: {output}')


if __name__ == '__main__':
    parser = ArgumentParser(description='Generate html reports for STRipy by subsetting full JSON results')
    parser.add_argument('--input_json', help='Path to stripy output json', required=True)
    parser.add_argument('--output', help='Output path for the final HTML file', required=True)
    parser.add_argument('--external_id', help='log external id', required=True)
    parser.add_argument('--report_type', help='report type', required=True)
    parser.add_argument('--loci_list', help='string_list_of_loci', required=True, nargs='+')
    parser.add_argument('--log_file', help='path to log missing loci to', required=True)
    parser.add_argument('--subset_svg_flag', help='default=1 0 for all loci 1 for flagged', required=True, type=int)
    args = parser.parse_args()
    main(
        args.input_json,
        args.output,
        args.external_id,
        args.report_type,
        args.loci_list,
        args.subset_svg_flag,
        logfile=args.log_file,
    )
