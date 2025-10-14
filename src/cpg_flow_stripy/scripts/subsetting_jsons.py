import json
import re
from argparse import ArgumentParser
from pathlib import Path
import toml


def main(input_json, output_root, target_cohort, report_type):
    # --- Configuration ---
    # Use relative paths with pathlib.Path
    report_loci_file = Path('loci_mapping.toml')
    template_html_file = Path('results_template.html')

    # Extract sampleID from filename (e.g., "CPG276402.stripy.json" -> "CPG276402")
    filename = Path(input_json).name
    sample_id = filename.split('.')[0]

    # --- Load Configuration and Loci Mappings ---
    with open(report_loci_file) as f:
        report_type_loci_map = toml.load(f)

    print(f"Processing cohort: {target_cohort}")
    print(f"Sample ID: {sample_id}")
    print(f"Report type: {report_type}")

    # --- Load Input Data ---
    with open(input_json) as f:
        data = json.load(f)

    # 1. Get the relevant loci for the current report type
    if report_type not in report_type_loci_map:
        print(f"  Warning: Report type '{report_type}' not found in loci mapping file '{report_loci_file}'. Skipping.")
        return

    locus_names = report_type_loci_map[report_type]
    print(f"  Relevant loci for '{report_type}': {locus_names}")

    listofdictonarydata = data['GenotypingResults']
    dict_of_dicts = {list(d.keys())[0]: d for d in listofdictonarydata}
    subset_dict = {k:v for k,v in dict_of_dicts.items() if k in locus_names}
    subset_list = [v for k,v in subset_dict.items()]

    # Create a temporary copy for this report type's results
    temp_data = data.copy()
    temp_data['GenotypingResults'] = subset_list
    temp_data['JobDetails'] = temp_data['JobDetails'].copy()
    temp_data['JobDetails']['TargetedLoci'] = locus_names

    # Sanitize software version for filename use
    software_version = temp_data['JobDetails']['SoftwareVersion']
    sanitized_version = re.sub(r'[^\w.-]', '-', software_version)

    # Generate filenames with the required format: {cohort}_{sampleID}_{report_type}_{stripy_version}
    base_filename = f"{target_cohort}_{sample_id}_{report_type.replace(' ', '-')}_{sanitized_version}"
    output_file = Path(output_root) / f"{base_filename}.json"
    output_html = Path(output_root) / f"{base_filename}.html"

    with open(output_file, 'w') as out_f:
        json.dump(temp_data, out_f, indent=4)

    with open(template_html_file) as resultsHTMLtemplate, open(output_html, 'w') as outputHTMLfile:
        for line in resultsHTMLtemplate:
            outputHTMLfile.write(line.replace('/*SampleResultsJSON*/', json.dumps(temp_data, indent=4)))

    print(f"  Files generated: {output_file}, {output_html}")

if __name__ == '__main__':
    parser = ArgumentParser(description='Generate BedGraph tracks for splice site variants')
    parser.add_argument(
        '--input_json', help='Path to stripy output json', required=True
    )
    parser.add_argument(
        '--output_root', help='Root output directory', required=True
    )
    parser.add_argument(
        '--target_cohort', help='Root output directory', required=True
    )
    parser.add_argument(
        '--report_type', help='Root output directory', required=True
    )
    args = parser.parse_args()
    main(args.input_json, args.output_root, args.target_cohort, args.report_type)