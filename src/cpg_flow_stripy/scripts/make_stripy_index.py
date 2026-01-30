import re
from argparse import ArgumentParser
from collections import defaultdict
from dataclasses import dataclass

# used to navigate from the installed location of this package to the HTML template file
from importlib import resources
from pathlib import Path

import jinja2


@dataclass
class Entry:
    """Object for storing details for each row in the index page"""

    sample: str
    report_type: str
    run_date: str
    missing: str
    ext_participant: str
    ext_sample: str
    family: str
    url: str


def digest_logging(log_path: str, index_manifest: dict[str, dict[str, str]]) -> list[Entry]:
    """Digest the per-report STRipy data - dates, subset ID, missing loci, ... pathogenic?"""

    report_objects: list[Entry] = []

    with open(log_path) as f:
        for line in f:
            if not line.rstrip():
                continue

            # break up the logging line, turn it into an index Entry
            line_list = line.rstrip().split('\t')

            cpg_id = line_list[0]
            report_type = line_list[1]

            report_objects.append(
                Entry(
                    sample=cpg_id,
                    report_type=re.sub(r'[-_]', ' ', report_type).title(),
                    ext_sample=index_manifest[cpg_id]['ext_participant'],
                    ext_participant=line_list[2],
                    run_date=re.sub(r'(\d{2})\.(\d{2})\.(\d{4}).*', r'\3/\2/\1', line_list[3]),
                    missing=line_list[4],
                    family=index_manifest[cpg_id]['family'],
                    url=index_manifest[cpg_id][report_type],
                ),
            )
    return report_objects


def digest_index_manifest(manifest_path: str) -> dict[str, dict[str, str]]:
    """Digest the index manifest to get the non-Stripy index details"""
    manifest_details: dict[str, dict[str, str]] = defaultdict(dict)
    with open(manifest_path) as f:
        for line in f:
            line_list = line.rstrip().split('\t')
            # f'{cpg_id}\t{fam_id}\t{external_id}\t{report_type}\t{corrected_path}'
            cpg_id = line_list[0]

            manifest_details[cpg_id] |= {
                'family': line_list[1],
                line_list[3]: line_list[4],
                'ext_participant': line_list[2],
            }

    return dict(manifest_details)


def main(input_path, dataset_name: str, output, log: str):
    """Main function to generate the index HTML file."""

    index_manifest = digest_index_manifest(input_path)

    index_entries = digest_logging(log, index_manifest)

    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(
            str(resources.files('cpg_flow_stripy') / 'templates'),
        ),
        autoescape=True,
    )

    template = env.get_template('index.html.jinja')

    content = template.render(reports=index_entries, dataset=dataset_name)

    # Write to output file
    with Path(output).open('w') as f:
        f.write(content)


if __name__ == '__main__':
    parser = ArgumentParser(description='Generate an Index page for all STRipy reports in a Dataset')
    parser.add_argument('--input_txt', help='file containing all inputs to this index', required=True)
    parser.add_argument('--dataset_name', help='dataset name', required=True)
    parser.add_argument('--output', help='Path to write the index HTML', required=True)
    parser.add_argument('--logfile', help='log of failed-to-find loci in this result', required=True)
    args = parser.parse_args()
    main(input_path=args.input_txt, dataset_name=args.dataset_name, output=args.output, log=args.logfile)
