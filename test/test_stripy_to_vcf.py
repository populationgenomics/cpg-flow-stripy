"""Tests for stripy_to_vcf multi-sample VCF generation."""

import math

import pysam
import pytest

from cpg_flow_stripy.scripts.stripy_to_vcf import write_multisample_vcf


def _make_locus(locus_id, chrom, pos, end, motif, a1_rep, a2_rep, coverage=30, filt="PASS"):
    return {
        "id": locus_id,
        "chrom": chrom,
        "pos": pos,
        "end": end,
        "motif": motif,
        "period": len(motif),
        "a1_rep": float(a1_rep),
        "a2_rep": float(a2_rep) if a2_rep is not None else None,
        "a1_ci": (a1_rep, a1_rep),
        "a2_ci": (a2_rep, a2_rep) if a2_rep is not None else (0, 0),
        "a1_out": 0,
        "a2_out": 0,
        "a1_z": 0.5,
        "a2_z": 0.5,
        "coverage": coverage,
        "filter": filt,
        "diseases": "DIS1",
    }


LOCUS_A = _make_locus("LOCUS_A", "chr1", 1000, 1050, "CAG", 10, 12)
LOCUS_B = _make_locus("LOCUS_B", "chr1", 2000, 2050, "GCN", 5, 5)
LOCUS_C = _make_locus("LOCUS_C", "chr2", 3000, 3050, "AT", 20, 22)


@pytest.fixture()
def two_sample_vcf(tmp_path):
    """
    SAMPLE1 has all three loci.
    SAMPLE2 has only LOCUS_A and LOCUS_C — LOCUS_B is absent.
    """
    sample1_loci = {loc["id"]: loc for loc in [LOCUS_A, LOCUS_B, LOCUS_C]}
    sample2_loci = {loc["id"]: loc for loc in [LOCUS_A, LOCUS_C]}

    out = tmp_path / "multi.vcf"
    write_multisample_vcf(
        [("SAMPLE1", sample1_loci), ("SAMPLE2", sample2_loci)],
        str(out),
    )
    return pysam.VariantFile(str(out))


def test_sample_names(two_sample_vcf):
    assert list(two_sample_vcf.header.samples) == ["SAMPLE1", "SAMPLE2"]


def test_all_loci_present(two_sample_vcf):
    ids = [rec.id for rec in two_sample_vcf.fetch()]
    assert set(ids) == {"LOCUS_A", "LOCUS_B", "LOCUS_C"}


def test_present_sample_has_data(two_sample_vcf):
    for rec in two_sample_vcf.fetch():
        if rec.id == "LOCUS_B":
            s = rec.samples["SAMPLE1"]
            assert s["REPCN"] == (5.0, 5.0)
            assert s["DP"] == 30
            assert s["STR_FILTER"] == ("PASS",)


def test_missing_sample_placeholders(two_sample_vcf):
    """SAMPLE2 has no data for LOCUS_B — all FORMAT fields should be missing/null."""
    for rec in two_sample_vcf.fetch():
        if rec.id == "LOCUS_B":
            s = rec.samples["SAMPLE2"]
            # Numeric fields should be None / nan
            assert all(v is None or (isinstance(v, float) and math.isnan(v)) for v in s["REPCN"])
            assert s["DP"] is None
            assert s["STR_FILTER"] == (".",)
