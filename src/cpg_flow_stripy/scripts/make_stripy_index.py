import re
from argparse import ArgumentParser
from collections import defaultdict
from dataclasses import dataclass
from importlib import resources
from pathlib import Path

import jinja2
from cpg_utils import config


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
    loci_of_interest: dict[str, str]

    def __key(self) -> tuple[str, str, str]:
        return self.sample, self.report_type, self.url

    def __hash__(self) -> int:
        return hash(self.__key())


def digest_logging(log_path: str, index_manifest: dict[str, dict[str, str]]) -> list[Entry]:
    """
    Digest the per-report STRipy data - dates, subset ID, missing loci, and interesting loci.

    Also extracts interesting loci (if any) which were flagged during analysis for each sample.
    """

    report_objects: list[Entry] = []

    with open(log_path) as f:
        for line in f:
            if not line.rstrip():
                continue

            # break up the logging line, turn it into an index Entry
            line_list = line.rstrip().split('\t')

            cpg_id = line_list[0]
            report_type = line_list[1]

            # Extract loci of interest from column 5
            loci_of_interest: dict[str, str] = {}
            if len(line_list) > 5:
                for locus_color in line_list[5]:
                    if ':' in locus_color:
                        locus, color = locus_color.split(':', 1)
                        loci_of_interest[locus] = color

            report_objects.append(
                Entry(
                    sample=cpg_id,
                    report_type=re.sub(r'[-_]', ' ', report_type).title(),
                    ext_sample=index_manifest[cpg_id]['ext_participant'],
                    ext_participant=line_list[2],
                    run_date=re.sub(r'(\d{2})\.(\d{2})\.(\d{4}).*', r'\3/\2/\1', line_list[3]),
                    missing=line_list[4].rstrip(),
                    family=index_manifest[cpg_id]['family'],
                    url=index_manifest[cpg_id][report_type],
                    loci_of_interest=loci_of_interest,
                ),
            )
    return report_objects


def digest_index_manifest(manifest_path: str) -> dict[str, dict[str, str]]:
    """Digest the index manifest to get the non-Stripy index details"""
    manifest_details: dict[str, dict[str, str]] = defaultdict(dict)
    with open(manifest_path) as f:
        for line in f:
            line_list = line.rstrip().split('\t')
            cpg_id = line_list[0]

            manifest_details[cpg_id] |= {
                'family': line_list[1],
                line_list[3]: line_list[4],
                'ext_participant': line_list[2],
            }

    return dict(manifest_details)


def main(manifest: str, dataset_name: str, output: str, log: str) -> None:
    """Main function to generate the index HTML file."""

    digested_manifest = digest_index_manifest(manifest)

    index_entries = digest_logging(log, digested_manifest)

    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(
            str(resources.files('cpg_flow_stripy') / 'templates'),
        ),
        autoescape=True,
    )

    template = env.get_template('index.html.jinja')

    dataset_title = re.sub(r'[-_]', ' ', dataset_name).title()
    dataset_title = config.config_retrieve(['stripy', 'stylised_mapping', dataset_name], default=dataset_title)

    content = template.render(reports=index_entries, dataset=dataset_title)

    # Write to output file
    with Path(output).open('w') as f:
        f.write(content)


if __name__ == '__main__':
    parser = ArgumentParser(description='Generate an Index page for all STRipy reports in a Dataset')
    parser.add_argument('--manifest', help='file containing all inputs to this index', required=True)
    parser.add_argument('--dataset', help='dataset name', required=True)
    parser.add_argument('--output', help='Path to write the index HTML', required=True)
    parser.add_argument('--logfile', help='log of failed-to-find loci in this result', required=True)
    args = parser.parse_args()
    main(manifest=args.manifest, dataset_name=args.dataset, output=args.output, log=args.logfile)
