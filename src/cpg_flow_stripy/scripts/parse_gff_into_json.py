#!/usr/bin/env python3

"""
Parses a GFF3 file, and generates a JSON file mapping Gene Symbols to Gene ID (ENSG)
- chrom
- start
- end
- gene details

This is built for and tested on the ensembl GFF file, though should generally work on valid GFF3 format files
https://ftp.ensembl.org/pub/release-116/vertebrates/gff3/homo_sapiens/Homo_sapiens.GRCh38.116.chr.gff3.gz
"""

import gzip
import json
import logging
import re
from argparse import ArgumentParser

from cpg_utils import to_path

CHROM_INDEX = 0
RESOURCE_INDEX = 1
TYPE_INDEX = 2
START_INDEX = 3
END_INDEX = 4
DETAILS_INDEX = 8

# regular expressions to parse out sections of the GFF3 annotations
GENE_ID_RE = re.compile(r'gene:(ENSG\d+);')
GENE_NAME_RE = re.compile(r'Name=([\w-]+);')

TYPES_TO_KEEP: set[str] = {'gene', 'ncRNA_gene', 'snRNA'}

CANONICAL_CONTIGS = [f'chr{x}' for x in list(range(1, 23))] + ['chrX', 'chrY', 'chrM']


def main(gff3_file: str, output: str) -> None:
    """
    Read the GFF3 file, and generate a BED file of gene regions, plus padding
    Args:
        gff3_file (str): path to the GFF3 file
        output (str): path to the JSON output file
    """

    gene_lookup = generate_gene_lookup(gff3_file)

    with to_path(output).open('w') as output_handle:
        json.dump(gene_lookup, output_handle, indent=4)


def generate_gene_lookup(gff3_file: str) -> dict[str, str]:
    """
    Generate the new BED file, and return the lines as a list of lists for merging.
    """

    output_json: dict[str, str] = {}

    # open and iterate over the GFF3 file
    with gzip.open(gff3_file, 'rt') as handle:
        for line in handle:
            # skip over headers and dividing lines
            if line.startswith('#'):
                continue

            line_as_list = line.rstrip().split('\t')

            # skip over non-genes (e.g. pseudogenes, ncRNA). Only focus on Ensembl genes/transcripts
            if (
                line_as_list[TYPE_INDEX] not in TYPES_TO_KEEP
                or 'ensembl' not in line_as_list[RESOURCE_INDEX]
                or f'chr{line_as_list[CHROM_INDEX]}' not in CANONICAL_CONTIGS
            ):
                continue

            # extract the gene name from the details field
            # allowing for some situations that don't work,
            # e.g. ENSG00000225931 (novel transcript, to be experimentally confirmed)
            # search for ID and transcript separately, ordering not guaranteed
            gene_name_match = GENE_NAME_RE.search(line_as_list[DETAILS_INDEX])
            gene_id_match = GENE_ID_RE.search(line_as_list[DETAILS_INDEX])
            if gene_id_match and gene_name_match:
                output_json[gene_name_match.group(1)] = gene_id_match.group(1)
            else:
                logging.debug(f'Failed to extract gene name from {line_as_list[DETAILS_INDEX]}')
    return output_json


def cli_main() -> None:
    parser = ArgumentParser()
    parser.add_argument('--gff3', help='Path to the compressed GFF3 file', required=True)
    parser.add_argument('--output', help='Path to output JSON.', required=True)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    main(gff3_file=args.gff3, output=args.output)


if __name__ == '__main__':
    cli_main()
