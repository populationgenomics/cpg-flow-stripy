import re
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


def render_loci_of_interest(loci_dict: dict[str, str]) -> str:
    """
    Render the loci of interest as colored HTML spans wrapped in a blurred container.
    Each locus is displayed with its associated color as the background.
    The container starts blurred and can be clicked to reveal.
    """
    if not loci_dict:
        return ''

    spans = []
    for locus, color in loci_dict.items():
        spans.append(
            f'<span style="background-color: {color}; padding: 2px 6px; border-radius: 3px; '
            f'margin: 2px; display: inline-block;">{locus}</span>'
        )
    content = ' '.join(spans)
    # Wrap in a div with loci-cell class for blur effect
    return f'<div class="loci-cell">{content}</div>'


def create_index_html(input_rows: list[dict], dataset_name: str) -> str:
    """Build consolidated index HTML referencing all HTML files listed in the txt file."""

    # Generate table rows
    table_rows = ''
    dataset_title = re.sub(r'[-_]', ' ', dataset_name).title()
    dataset_title = config.config_retrieve(['stripy', 'stylised_mapping', dataset_name], default=dataset_title)
    for each_dict in input_rows:
        table_rows += f"""
            <tr>
                <td>{each_dict['cpg_id']}</td>
                <td>{each_dict['family_id']}</td>
                <td>{each_dict['external_participant_id']}</td>
                <td>{each_dict['external_id']}</td>
                <td>{each_dict['subset']}</td>
                <td>{each_dict['analysis_time']}</td>
                <td>{each_dict['missing_genes']}</td>
                <td>{render_loci_of_interest(each_dict['loci_of_interest'])}</td>
                <td>{create_open_button(each_dict['html_path'])}</td>
            </tr>
        """

    # Replace placeholder with actual rows
    with resources.open_text('cpg_flow_stripy', 'index_template.html') as template:
        modified_content = template.read().replace('<!-- ROWS_WILL_BE_INSERTED_HERE -->', table_rows)
        return modified_content.replace('---Dataset---', dataset_title)


def digest_logging(log_path: str) -> tuple[dict[str, dict[str, str]], dict[str, str], dict[str, str]]:
    """
    Takes a path to a TSV containing the logging details (Loci which were not present during locus subsetting)
    Extracts out the missing loci (if any) which were present for each sample / report subset combination

    This parsing could be tidied up a lot
    """
    missing_genes: dict[str, dict[str, str]] = defaultdict(dict)
    external_id_dict: dict[str, str] = {}
    stripyanalysis_time_dict: dict[str, str] = {}
    with open(log_path) as f:
        for line in f:
            # skip empty lines
            if not line.rstrip():
                continue

            line_list = line.rstrip().split('\t')
            external_id = line_list[2]
            missing_genes[line_list[0]][line_list[1]] = line_list[4]
            external_id_dict[line_list[0]] = external_id
            stripyanalysis_time_dict[line_list[0]] = line_list[3]

    return dict(missing_genes), dict(external_id_dict), dict(stripyanalysis_time_dict)


def digest_interesting_loci(log_path: str) -> dict[str, dict[str, str]]:
    """
    Takes a path to a TSV containing the logging details of interesting loci (Loci which were flagged during analysis)
    Extracts out the interesting loci (if any) which were present for each sample / report subset combination

    Returns a dict mapping sample_id -> {locus: color, ...}
    """
    interesting_loci: dict[str, dict[str, str]] = defaultdict(dict)
    with open(log_path) as f:
        for line in f:
            # skip empty lines
            if not line.rstrip():
                continue

            line_list = line.rstrip().split('\t')
            sample_id = line_list[0]
            for locus_color in line_list[1:]:
                if ':' in locus_color:
                    locus, color = locus_color.split(':', 1)
                    interesting_loci[sample_id][locus] = color

    return dict(interesting_loci)


def read_input_rows(
    input_path: str,
    log_data: dict[str, dict[str, str]],
    external_id_dict: dict[str, str],
    stripy_analysis_dict: dict[str, str],
    loci_of_interest: dict[str, dict[str, str]],
) -> list[dict]:
    """Reads the input file containing all details to populate into this index, returns as a list of dicts."""
    all_details: list[dict] = []
    with open(input_path) as f:
        for line in f:
            line_list = line.rstrip().split('\t')
            cpg_id = line_list[0]
            subset = line_list[3]
            subset_nice = re.sub(r'[-_]', ' ', line_list[3]).title()
            external_id = external_id_dict[cpg_id]
            analysis_time = stripy_analysis_dict[cpg_id]
            analysis_time_nice = re.sub(r'(\d{2})\.(\d{2})\.(\d{4}).*', r'\1/\2/\3', analysis_time)

            line_dict = {
                'cpg_id': cpg_id,
                'external_id': external_id,
                'external_participant_id': line_list[2],
                'family_id': line_list[1],
                'subset': subset_nice,
                'analysis_time': analysis_time_nice,
                'html_path': line_list[4],
                'missing_genes': '',
                'loci_of_interest': loci_of_interest.get(cpg_id, {}),
            }

            # if there are missing loci for this subset, update from an empty string
            # absolutely chaotic use of config_retrieve - giving it the dictionary, and traversing using keys
            if (missing := config.config_retrieve(key=[cpg_id, subset], config=log_data)) != 'None':
                line_dict['missing_genes'] = missing

            all_details.append(line_dict)

    return all_details


def main(input_path: str, dataset_name: str, output: str, log: str, log_loci: str) -> None:
    """Main function to generate the index HTML file."""

    log_content, external_id_dict, stripy_analysis_dict = digest_logging(log)

    loci_of_interest = digest_interesting_loci(log_loci)

    input_rows = read_input_rows(input_path, log_content, external_id_dict, stripy_analysis_dict, loci_of_interest)

    # Generate the index HTML content
    index_html_content = create_index_html(input_rows, dataset_name)

    # Write to archived output file
    with Path(output).open('w') as f:
        f.write(index_html_content)


if __name__ == '__main__':
    parser = ArgumentParser(description='Generate an Index page for all STRipy reports in a Dataset')
    parser.add_argument('--input_txt', help='file containing all inputs to this index', required=True)
    parser.add_argument('--dataset_name', help='dataset name', required=True)
    parser.add_argument('--output', help='Path to write the index HTML', required=True)
    parser.add_argument('--logfile', help='log of failed-to-find loci in this result', required=True)
    parser.add_argument('--log_loci', help='log of interesting loci in this result', required=True)
    args = parser.parse_args()
    main(
        input_path=args.input_txt,
        dataset_name=args.dataset_name,
        output=args.output,
        log=args.logfile,
        log_loci=args.log_loci,
    )
