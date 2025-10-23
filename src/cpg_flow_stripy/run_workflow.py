#!/usr/bin/env python3

"""
The main entry point for the workflow.
"""

from argparse import ArgumentParser

from cpg_flow.workflow import run_workflow

from cpg_flow_stripy.stages import RunStripy


def cli_main():
    """
    CLI entrypoint - starts up the workflow
    """
    parser = ArgumentParser()
    parser.add_argument('--dry_run', action='store_true', help='Dry run')
    args = parser.parse_args()

    run_workflow(name='stripy', stages=[RunStripy], dry_run=args.dry_run)


if __name__ == '__main__':
    cli_main()
