import json
from argparse import ArgumentParser

# used to navigate from the installed location of this package to the HTML template file
from importlib import resources
from pathlib import Path


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
            if flag_status >= subset_svg_flag and 'SVG' in details_dict:
                del details_dict['SVG']

    temp_data['GenotypingResults'] = genotyping_results
    temp_data['JobDetails'] = temp_data['JobDetails'].copy()
    temp_data['JobDetails']['TargetedLoci'] = loci_list
    temp_data['JobDetails']['MissingGenes'] = missing_genes
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
