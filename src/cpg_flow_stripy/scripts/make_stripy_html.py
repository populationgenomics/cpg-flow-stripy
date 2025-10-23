import json
from argparse import ArgumentParser

REPORT_TEMPLATE_PATH = './cpg_flow_stripy/stripy_report_template.html'


def cli_main():
    parser = ArgumentParser()
    parser.add_argument('--results', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    make_stripy_html(args.results, args.output, REPORT_TEMPLATE_PATH)


def make_stripy_html(results_file: str, output_file: str, template_file: str = REPORT_TEMPLATE_PATH):
    """
    Generate an HTML report from STRipy JSON results using a specified HTML template.
    """
    # Make a copy of the template to populate
    with open(results_file) as rf:
        results_json = json.load(rf)

    with open(template_file) as report_template, open(output_file, 'w') as output_report:
        for line in report_template:
            output_report.write(line.replace('/*SampleResultsJSON*/', json.dumps(results_json, indent=2)))


if __name__ == '__main__':
    # if called as a script, call through to the ArgParse CLI
    cli_main()
