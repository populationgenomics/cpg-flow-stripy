import re
from pathlib import Path
from argparse import ArgumentParser

def extract_file_data_from_filename(filename):
    """
    Extract data from filename format: {target_cohort}_{sample_id}_{report_type.replace(' ', '-')}_{sanitized_version}.html
    Splits by underscores and expects exactly 4 parts before the extension.
    Returns dict with extracted components or None if format doesn't match.
    """
    # Check if it ends with .html
    if not filename.endswith('.html'):
        return None

    # Remove the .html extension
    base_name = filename[:-5]

    # Split by underscores, expecting exactly 4 parts now
    parts = base_name.split('_')

    # Check if we have exactly 4 parts after removing extension
    if len(parts) != 4:
        return None # Format doesn't match expected 4 components separated by underscores

    target_cohort, sample_id, report_type_with_hyphens, sanitized_version = parts

    # Replace hyphens back to spaces in report_type
    report_type = report_type_with_hyphens.replace('-', ' ')

    return {
        'sample': sample_id,
        'dataset': target_cohort,
        'report_type': report_type, # Will have spaces instead of hyphens
        'stripy_version': sanitized_version
    }

def create_open_button(file_string, output_root):
    """
    Return HTML snippet for a button opening the provided file in a new tab
    using the Google Cloud Storage browser URL.
    NOTE: This requires the user to be signed into an authorized Google account.
    """
    # Convert output_root to string if it's a Path object
    output_root_str = str(output_root)

    # Extract the bucket and path part after 'gs://'
    if output_root_str.startswith('gs://'):
        match = re.match(r'gs://([^/]+)/(.+)', output_root_str)
        if match:
            bucket_name = match.group(1)
            path_after_bucket = match.group(2)
            gcs_browser_url = f'https://storage.cloud.google.com/{bucket_name}/{path_after_bucket}/{file_string}'
        else:
            raise ValueError(f'Invalid output_root format: {output_root_str}')
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

def read_html_files_from_txt(txt_file_path):
    """Read HTML filenames from a text file, one per line."""
    file_path = Path(txt_file_path)
    with file_path.open('r') as f:
        return [line.strip() for line in f if line.strip()]

def create_index_html(txt_file_path, output_root, index_template_path='src/index_template.html'):
    """
    Build consolidated index HTML referencing all HTML files listed in the txt file.
    Files should follow the naming convention: {target_cohort}_{sample_id}_{report_type.replace(' ', '-')}_{sanitized_version}.html
    """
    template_path_obj = Path(index_template_path)
    with template_path_obj.open('r') as f:
        template = f.read()

    # Read HTML filenames from txt file
    html_files = read_html_files_from_txt(txt_file_path)

    # Generate table rows
    table_rows = ''
    for filename in html_files:
        file_data = extract_file_data_from_filename(filename)
        if file_data:
            table_rows += f"""
                <tr>
                    <td>{file_data['sample']}</td>
                    <td>{file_data['dataset']}</td>
                    <td>{file_data['report_type']}</td>
                    <td>{file_data['stripy_version']}</td>
                    <td>{create_open_button(filename, output_root)}</td>
                </tr>
            """
        else:
             # Optionally log or handle files that don't match the expected format
             print(f"Warning: Filename '{filename}' does not match expected format and will be skipped.")

    # Replace placeholder with actual rows
    return template.replace('<!-- ROWS_WILL_BE_INSERTED_HERE -->', table_rows)

def main(txt_file_path, output_root, output_index_path='index.html', index_template_path='src/index_template.html'):
    """
    Main function to generate the index HTML file.

    Args:
        txt_file_path: Path to text file containing HTML filenames
        output_root: GCS bucket path where files are stored
        output_index_path: Where to save the generated index.html
        index_template_path: Path to the HTML template
    """
    # Generate the index HTML content
    index_html_content = create_index_html(txt_file_path, output_root, index_template_path)

    # Write to output file
    output_path = Path(output_index_path)
    with output_path.open('w') as f:
        f.write(index_html_content)

    print(f"Index HTML generated successfully at: {output_path}")


if __name__ == '__main__':
    parser = ArgumentParser(description='Generate BedGraph tracks for splice site variants')
    parser.add_argument(
        'txt_file_path', help='Path to stripy output json', required=True
    )
    parser.add_argument(
        '--output_root', help='Root output directory', required=True
    )
    args = parser.parse_args()
    main(args.input_json, args.output_root)


