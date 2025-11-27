import copy
import json
from argparse import ArgumentParser

# used to navigate from the installed location of this package to the HTML template file
from importlib import resources
from pathlib import Path

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


def deep_merge_defaults(target_dict, default_dict):
    """
    Recursively merges a target dictionary with a default schema.

    If a key is missing, or its value is None, in the target_dict, it is
    filled in from the default_dict. The function handles nested dictionaries
    and iterates over lists of dictionaries to merge their items against
    the first item in the default list (the schema template).

    Args:
        target_dict (dict): The dictionary to be populated (the "populated" dictionary).
        default_dict (dict): The dictionary containing default values (the essential fields).

    Returns:
        dict: The target_dict updated with default values.
    """
    # Ensure we are working with dicts; otherwise, just return the target
    if not isinstance(target_dict, dict) or not isinstance(default_dict, dict):
        return target_dict

    # Iterate over all keys and values in the default dictionary (the schema)
    for key, default_value in default_dict.items():
        # Case 1: Key is missing or explicitly None
        if key not in target_dict or target_dict[key] is None:
            # If missing/None, copy the default value.
            target_dict[key] = copy.deepcopy(default_value)

        # Case 2: Key exists, and both values are dictionaries (recurse)
        elif isinstance(target_dict[key], dict) and isinstance(default_value, dict):
            target_dict[key] = deep_merge_defaults(target_dict[key], default_value)

        # Case 3: Key exists, and both values are lists (handle list items recursively)
        elif isinstance(target_dict[key], list) and isinstance(default_value, list) and default_value:
            # Use the first element of the default list as the merge template
            default_template_item = default_value[0]

            # Iterate over existing items in the target list
            for i, target_item in enumerate(target_dict[key]):
                # Only attempt to merge if the target item is a dictionary
                if isinstance(target_item, dict):
                    # Recursively merge the target list item with the default template
                    target_dict[key][i] = deep_merge_defaults(target_item, default_template_item)

            # If the target list was initially empty, it was handled in Case 1.
            # If both are non-empty lists, we only merge the contents of existing items.

    return target_dict


def main(input_json, output, external_id, report_type, loci_list, subset_svg_flag: int, logfile: str):
    # Extract sampleID from filename (e.g., "CPG276402.stripy.json" -> "CPG276402")
    filename = Path(input_json).name
    sample_id = filename.split('.')[0]

    print(f'Sample ID: {sample_id}')
    print(f'Report type: {report_type}')

    # --- Load Input Data ---
    with open(input_json) as f:
        data = json.load(f)

    print(f"  Relevant loci for '{report_type}': {loci_list}")

    listofdictionarydata = data['GenotypingResults']
    list_of_available_genes = [list(d.keys())[0] for d in listofdictionarydata]
    missing_genes = [gene for gene in loci_list if gene not in list_of_available_genes]

    # log the missing genes
    with open(logfile, 'a') as handle:
        if missing_genes:
            handle.write(f'{sample_id}\t{report_type}\t{external_id}\t{",".join(missing_genes)}\n')
        else:
            handle.write(f'{sample_id}\t{report_type}\t{external_id}\tNone\n')

    print(f'  Available loci in input JSON: {list_of_available_genes}')
    print(f'  Missing loci for this report: {missing_genes}')

    dict_of_dicts = {list(d.keys())[0]: d for d in listofdictionarydata}
    subset_dict = {k: v for k, v in dict_of_dicts.items() if k in loci_list}
    subset_list = [v for k, v in subset_dict.items()]

    # Create a temporary copy for this report type's results
    temp_data = data.copy()
    temp_data['GenotypingResults'] = subset_list

    genotyping_results = temp_data['GenotypingResults']
    for locus_item in genotyping_results:
        # Each locus_item is a dictionary with one key (the Locus ID)
        # Use .items() to get the (key, value) pair. Since there's only one,
        # the loop will run exactly once per item.
        for _locus_id, details_dict in locus_item.items():
            flag_status = details_dict.get('Flag')

            # You can add conditional logic here, e.g., to check for non-zero flags
            if flag_status < subset_svg_flag and 'SVG' in details_dict:
                del details_dict['SVG']

    temp_data['GenotypingResults'] = genotyping_results
    temp_data['JobDetails'] = temp_data['JobDetails'].copy()
    temp_data['JobDetails']['TargetedLoci'] = loci_list
    temp_data['JobDetails']['MissingGenes'] = missing_genes

    # --- Ensure All Required Fields are Present ---

    temp_data = deep_merge_defaults(temp_data, DEFAULT_REPORT_SCHEMA)

    # --- Generate HTML Output ---
    # This script writes the final HTML to the --output path
    with (
        resources.open_text('cpg_flow_stripy', 'results_template.html') as results_html_template,
        open(output, 'w') as output_html_file,
    ):
        for line in results_html_template:
            # double replace, single write
            output_html_file.write(line.replace('/*SampleResultsJSON*/', json.dumps(temp_data, indent=4)))

    print(f'  HTML file generated: {output}')


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
