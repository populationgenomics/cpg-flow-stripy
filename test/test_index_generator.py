"""
add relevant tests to this directory
"""

from cpg_flow_stripy.scripts.make_stripy_index import Entry, digest_index_manifest, digest_logging


def test_digest_manifest(tmp_path):
    """Check that the manifest digest is correct."""

    manifest_file = tmp_path / 'manifest.txt'
    manifest_data = '\n'.join(
        [
            'CPG1\tFAM1\tEXT1\tdefault\tblahblahblah.html',
            'CPG2\tFAM2\tEXT2\tdefault\tfoofoofoo.html',
            'CPG2\tFAM2\tEXT2\tspecific\toooooooohhh.html',
        ],
    )

    with open(manifest_file, 'w') as writehandle:
        writehandle.write(manifest_data)

    results = digest_index_manifest(manifest_file)
    assert results == {
        'CPG1': {
            'default': 'blahblahblah.html',
            'ext_participant': 'EXT1',
            'family': 'FAM1',
        },
        'CPG2': {
            'default': 'foofoofoo.html',
            'ext_participant': 'EXT2',
            'family': 'FAM2',
            'specific': 'oooooooohhh.html',
        },
    }


def test_digest_logging(tmp_path):
    """Check that the logging digest is correct."""
    logging_file = tmp_path / 'log.txt'

    with open(logging_file, 'w') as writehandle:
        writehandle.write('CPG1\tdefault\tEXTSAM1\t01.02.2015\t\n')
        writehandle.write('CPG2\tdefault\tEXTSAM2\t22.11.9999\tLOTS,OF,MISSING\n')
        writehandle.write('CPG2\tspecific\tEXTSAM2\t22.11.9999\tNOTHING\n')

    manifest = {
        'CPG1': {
            'default': 'blahblahblah.html',
            'ext_participant': 'EXT1',
            'family': 'FAM1',
        },
        'CPG2': {
            'default': 'foofoofoo.html',
            'ext_participant': 'EXT2',
            'family': 'FAM2',
            'specific': 'oooooooohhh.html',
        },
    }

    entries = digest_logging(log_path=logging_file, index_manifest=manifest)
    for expected in [
        Entry(
            sample='CPG1',
            report_type='Default',
            run_date='2015/02/01',
            missing='',
            ext_participant='EXTSAM1',
            ext_sample='EXT1',
            family='FAM1',
            url='blahblahblah.html',
        ),
        Entry(
            sample='CPG2',
            report_type='Default',
            run_date='9999/11/22',
            missing='LOTS,OF,MISSING',
            ext_participant='EXTSAM2',
            ext_sample='EXT2',
            family='FAM2',
            url='foofoofoo.html',
        ),
        Entry(
            sample='CPG2',
            report_type='Specific',
            run_date='9999/11/22',
            missing='NOTHING',
            ext_participant='EXTSAM2',
            ext_sample='EXT2',
            family='FAM2',
            url='oooooooohhh.html',
        ),
    ]:
        assert expected in entries
