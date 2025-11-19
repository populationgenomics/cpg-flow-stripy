from argparse import ArgumentParser
from collections import defaultdict

# used to navigate from the installed location of this package to the HTML template file
from importlib import resources
from pathlib import Path

from cpg_utils import config


def create_open_button(gcs_browser_url: str) -> str:
    """
    Return HTML snippet for a button opening the provided file in a new tab using the Google Cloud Storage browser URL.
    NOTE: This requires the user to be signed into an authorized Google account.
    """
    return f"""
        <button onclick="window.open('{gcs_browser_url}', '_blank')"
                style="background-color: #4CAF50; color: white; padding: 8px 16px; border: none;
                       border-radius: 4px; cursor: pointer; font-family: 'Trebuchet MS', Trebuchet, Arial, sans-serif;"
                onmouseover="this.style.backgroundColor='#45a049'"
                onmouseout="this.style.backgroundColor='#4CAF50'">
            Open
        </button>
    """


def create_index_html(input_rows: list[dict[str, str]]) -> str:
    """Build consolidated index HTML referencing all HTML files listed in the txt file."""

    # Generate table rows
    table_rows = ''
    for each_dict in input_rows:
        table_rows += f"""
            <tr>
                <td>{each_dict['cpg_id']}</td>
                <td>{each_dict['family_id']}</td>
                <td>{each_dict['subset']}</td>
                <td>{each_dict['missing_genes']}</td>
                <td>{create_open_button(each_dict['html_path'])}</td>
            </tr>
        """

    # Replace placeholder with actual rows
    with resources.open_text('cpg_flow_stripy', 'index_template.html') as template:
        return template.read().replace('<!-- ROWS_WILL_BE_INSERTED_HERE -->', table_rows)


def digest_logging(log_path: str) -> dict[str, dict[str, str]]:
    """
    Takes a path to a TSV containing the logging details (Loci which were not present during locus subsetting)
    Extracts out the missing loci (if any) which were present for each sample / report subset combination

    This parsing could be tidied up a lot
    """
    missing_genes: dict[str, dict[str, str]] = defaultdict(dict)
    with open(log_path) as f:
        for line in f:
            # skip empty lines
            if not line.rstrip():
                continue

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
    all_details: list[dict[str, str]] = []
    with open(input_path) as f:
        for line in f:
            line_list = line.rstrip().split('\t')
            cpg_id = line_list[0]
            subset = line_list[2]

            line_dict = {
                'cpg_id': cpg_id,
                'family_id': line_list[1],
                'subset': subset,
                'html_path': line_list[3],
                'missing_genes': '',
            }

            # if there are missing loci for this subset, update from an empty string
            # absolutely chaotic use of config_retrieve - giving it the dictionary, and traversing using keys
            if (missing := config.config_retrieve(key=[cpg_id, subset], config=log_data)) != 'None':
                line_dict['missing_genes'] = missing

            all_details.append(line_dict)

    return all_details


def main(input_path, output, log: str):
    """Main function to generate the index HTML file."""

    log_content = digest_logging(log)
    input_rows = read_input_rows(input_path, log_content)

    # Generate the index HTML content
    index_html_content = create_index_html(input_rows)

    # Write to output file
    with Path(output).open('w') as f:
        f.write(index_html_content)

    print(f'Index HTML generated successfully at: {output}')


if __name__ == '__main__':
    parser = ArgumentParser(description='Generate BedGraph tracks for splice site variants')
    parser.add_argument('--input_txt', help='file containing all inputs to this index', required=True)
    parser.add_argument('--output', help='Path to write the index HTML', required=True)
    parser.add_argument('--logfile', help='log of failed-to-find loci in this result', required=True)
    args = parser.parse_args()
    main(input_path=args.input_txt, output=args.output, log=args.logfile)
