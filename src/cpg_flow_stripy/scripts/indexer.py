import json
import re
from argparse import ArgumentParser
from pathlib import Path


def extract_file_data_from_path(file_path_str):
    """
    Extracts data from a path where 'loci_version' is the folder
    immediately following a 'stripy' folder.

    Format expected: .../stripy/{loci_version}/{sequencing_group_id}__{ll}.html
    """
    path = Path(file_path_str)

    # 1. Basic validation: Check extension
    if path.suffix != '.html':
        return None

    # 2. Extract Loci Version (The folder AFTER 'stripy')
    try:
        # Get all parts of the path (e.g., '/', 'mnt', 'data', 'stripy', 'v1', 'file.html')
        parts = path.parts

        # Find the index of the 'stripy' folder
        stripy_index = parts.index('stripy')

        # The version is the next part in the list
        loci_version = parts[stripy_index + 1]

        # Ensure the version isn't actually the filename (i.e., ensure structure is stripy/ver/file)
        if loci_version == path.name:
            return None

    except (ValueError, IndexError):
        # 'stripy' was not found, or there was no folder after 'stripy'
        return None

    # 3. Extract ID and LL from the filename
    # path.stem gives us "sequencing_group.id__ll" (removes .html)
    filename_parts = path.stem.split('__')

    if len(filename_parts) != 2:
        return None

    sequencing_group_id, ll = filename_parts

    return {'sequencing_group_id': sequencing_group_id, 'report_type': ll, 'loci_version': loci_version}


def create_open_button(file_string, output, bucket_name):
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
            delimiter = f'{bucket_name}/'

            # Split the string by the delimiter, but only perform one split.
            # This divides the string into [stuff_before, stuff_after]
            parts = output_root_str.split(delimiter, 1)

            # If the split was successful, 'parts' will have two elements.
            # The path we want is the second element (at index 1).
            if len(parts) == 2:
                path_after_bucket = parts[1]
                gcs_browser_url = f'https://storage.cloud.google.com/{bucket_name}/{path_after_bucket}/{file_str}'
            else:
                # The delimiter (bucket_name/) wasn't found
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


def extract_missing_genes(filename):
    """
    Loads an HTML file, finds the 'Genes Not Found' section,
    and extracts the JSON list of missing genes from the
    'missing-genes-list-...' <ul> tag that follows it.

    This function finds the *first* occurrence of the section.

    Args:
        filename (str): The path to the HTML file.

    Returns:
        list: A list of missing gene names, or an empty list
              if the file is not found, the section is missing,
              or the JSON content is invalid or not a list.
    """

    # Read file content
    try:
        with open(filename, encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError as e:
        if isinstance(e, FileNotFoundError):
            print(f'Error: File not found at {filename}')
        else:
            print(f'Error reading file {filename}: {e}')
        return []

    # Extract JSON from HTML using regex
    pattern = re.compile(r'<!-- Genes Not Found Section -->.*?<ul id="missing-genes-list-.*?">(.*?)</ul>', re.DOTALL)
    match = pattern.search(content)

    # Return early if no match or empty content
    if not match or not match.group(1).strip():
        return []

    json_string = match.group(1).strip()

    # Parse JSON and validate
    try:
        gene_list = json.loads(json_string)
        if not isinstance(gene_list, list):
            print(f'Warning: Extracted JSON in {filename} is not a list. Found type: {type(gene_list)}')
            return []
        return gene_list
    except json.JSONDecodeError:
        print(f'Error: Failed to decode JSON in {filename}.')
        print('--- Invalid Content Snippet ---')
        print(json_string[:150] + '...')
        print('--- End Snippet ---')
        return []


def create_index_html(txt_file_paths, output, bucket_name, index_template_path='src/index_template.html'):
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
                    <td>{create_open_button(filename, output, bucket_name)}</td>
                </tr>
            """
        else:
            # Optionally log or handle files that don't match the expected format
            print(f"Warning: Filename '{filename}' does not match expected format and will be skipped.")

    # Replace placeholder with actual rows
    return template.replace('<!-- ROWS_WILL_BE_INSERTED_HERE -->', table_rows)


def main(
    txt_file_paths, output, bucket_name, output_index_path='index.html', index_template_path='src/index_template.html'
):
    """
    Main function to generate the index HTML file.

    Args:
        txt_file_paths: Path to text file containing HTML filenames
        output:expected output file write location
        output_index_path: Where to save the generated index.html
        index_template_path: Path to the HTML template
    """
    # Generate the index HTML content
    index_html_content = create_index_html(txt_file_paths, bucket_name, output, index_template_path)

    # Write to output file
    output_path = Path(output_index_path)
    with output_path.open('w') as f:
        f.write(index_html_content)

    print(f'Index HTML generated successfully at: {output_path}')


if __name__ == '__main__':
    parser = ArgumentParser(description='Generate BedGraph tracks for splice site variants')
    parser.add_argument('--txt_file_paths', help='list of html files', required=True)
    parser.add_argument('--output', help='Root output directory', required=True)
    parser.add_argument('--bucket_name', help='name of the dataset', required=True)
    args = parser.parse_args()
    main(args.txt_file_paths, args.output, args.bucket_name)
