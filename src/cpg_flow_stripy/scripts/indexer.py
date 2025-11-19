import json
import re
from argparse import ArgumentParser
from collections import defaultdict

# used to navigate from the installed location of this package to the HTML template file
from importlib import resources
from pathlib import Path

from cpg_utils import config


def create_open_button(file_string, output, dataset_name):
    """
    Return HTML snippet for a button opening the provided file in a new tab
    using the Google Cloud Storage browser URL.
    NOTE: This requires the user to be signed into an authorized Google account.
    """
    # Convert output to string if it's a Path object
    output_root_str = str(output)
    file_str = str(file_string)

    # Extract the bucket and path part after 'gs://'
    if output_root_str.startswith('gs://'):
        try:
            # Create the delimiter string we're looking for.
            # We include the slash to ensure we're splitting at the end of the bucket name.
            delimiter = f'{dataset_name}/'

            # Split the string by the delimiter, but only perform one split.
            # This divides the string into [stuff_before, stuff_after]
            parts = output_root_str.split(delimiter, 1)

            # If the split was successful, 'parts' will have two elements.
            # The path we want is the second element (at index 1).
            if len(parts) == 2:
                path_after_bucket = parts[1]
                gcs_browser_url = f'https://storage.cloud.google.com/{dataset_name}/{path_after_bucket}/{file_str}'
            else:
                # The delimiter (dataset_name/) wasn't found
                print(f"Warning: Could not find delimiter '{delimiter}' in path '{output_root_str}'.")
                return None
        except (AttributeError, TypeError) as e:
            # Catch specific errors that could occur during string operations
            print(f"An error occurred while parsing GCS path '{output_root_str}': {e}")
            return None
    else:
        raise ValueError(f'output_root does not appear to be a GCS path (starts with gs://): {output_root_str}')

    return f"""
        <button onclick="window.open('{gcs_browser_url}', '_blank')"
                style="background-color: #4CAF50; color: white; padding: 8px 16px; border: none;
                       border-radius: 4px; cursor: pointer; font-family: 'Trebuchet MS', Trebuchet, Arial, sans-serif;"
                onmouseover="this.style.backgroundColor='#45a049'"
                onmouseout="this.style.backgroundColor='#4CAF50'">
            Open
        </button>
    """


def create_index_html(input_rows, output, dataset_name, log: str):
    """
    Build consolidated index HTML referencing all HTML files listed in the txt file.
    Files should follow the naming convention:
    {target_cohort}_{sample_id}_{report_type.replace(' ', '-')}_{sanitized_version}.html
    """
    template_path_obj = Path(index_template_path)
    with template_path_obj.open('r') as f:
        template = f.read()

    # Generate table rows
    table_rows = ''
    for filename in txt_file_paths:
        file_data = extract_file_data_from_path(filename)
        file_data['missing_genes'] = extract_missing_genes(filename)
        if file_data:
            table_rows += f"""
                <tr>
                    <td>{file_data['sequencing_group_id']}</td>
                    <td>{file_data['report_type']}</td>
                    <td>{file_data['missing_genes']}</td>
                    <td>{file_data['loci_version']}</td>
                    <td>{create_open_button(filename, output, dataset_name)}</td>
                </tr>
            """
        else:
            # Optionally log or handle files that don't match the expected format
            print(f"Warning: Filename '{filename}' does not match expected format and will be skipped.")

    # Replace placeholder with actual rows
    return template.replace('<!-- ROWS_WILL_BE_INSERTED_HERE -->', table_rows)


def digest_logging(log_path: str) -> dict[str, dict[str, str]]:
    """
    Takes a path to a TSV containing the logging details (Loci which were not present during locus subsetting)
    Extracts out the missing loci (if any) which were present for each sample / report subset combination

    This parsing could be tidied up a lot
    """
    missing_genes = defaultdict(dict)
    with open(log_path, 'r') as f:
        for line in f:
            line_list = line.rstrip().split('\t')

            # e.g. {
            #          CPGxxx: {
            #              report_type_1: "gene1,gene2,gene3",,
            #              report_type_2: "None",
            #          },
            #      ...
            #      }
            missing_genes[line_list[0]][line_list[1]] = line_list[2]

    return dict(missing_genes)


def read_input_rows(input_path: str, log_data: dict[str, dict[str, str]]) -> list[dict[str, str]]:
    """Reads the input file containing all details to populate into this index, returns as a list of dicts."""
    all_details = []
    with open(input_path, 'r') as f:
        for line in f:
            line_list = line.rstrip().split('\t')
            line_dict = {
                'cpg_id': line_list[0],
                'family_id': line_list[1],
                'subset': line_list[2],
                'html_path': line_list[3],
                'missing_genes': 'None',
            }

            # if there are missing loci for this subset, update from "None"
            if line_dict['cpg_id'] in log_data and line_dict['subset'] in log_data[line_dict['cpg_id']]:
                line_dict['missing_genes'] = log_data[line_dict['cpg_id']][line_dict['subset']]

    return all_details


def main(input_path, output, dataset_name, log: str):
    """Main function to generate the index HTML file."""

    log_content = digest_logging(log)
    input_rows = read_input_rows(input_path, log_content)

    ...

    # Generate the index HTML content
    index_html_content = create_index_html(input_path, dataset_name, output, log)

    # Write to output file
    output_path = Path(output_index_path)
    with output_path.open('w') as f:
        f.write(index_html_content)

    print(f'Index HTML generated successfully at: {output_path}')


if __name__ == '__main__':
    parser = ArgumentParser(description='Generate BedGraph tracks for splice site variants')
    parser.add_argument('--input_txt', help='file containing all inputs to this index', required=True)
    parser.add_argument('--logfile', help='log of failed-to-find loci in this result', required=True)
    parser.add_argument('--output', help='Path to write the index HTML', required=True)
    parser.add_argument('--dataset_name', help='Name of the dataset', required=True)
    args = parser.parse_args()
    main(args.input_txt, args.output, args.dataset_name, log=args.logfile)
