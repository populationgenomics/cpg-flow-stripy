#!/usr/bin/env python3

import argparse
import json
import re
from pathlib import Path

import pysam
from pyfaidx import Fasta


def parse_coords(coord):
    m = re.match(r"(chr)?(?P<chrom>[^:]+):(?P<start>\d+)-(?P<end>\d+)", str(coord))
    if not m:
        return None
    chrom = m.group("chrom")
    if not chrom.startswith("chr"):
        chrom = "chr" + chrom
    return chrom, int(m.group("start")), int(m.group("end"))


def chrom_key(c):
    m = re.match(r"chr(\d+)$", c)
    if m:
        return (0, int(m.group(1)))
    order = {"chrX": (1, 23), "chrY": (1, 24), "chrM": (2, 25), "chrMT": (2, 25)}
    return order.get(c, (9, c))


def _to_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return float("nan")


def _to_outlier(x):
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def _ci_tuple(allele):
    return (allele["CI"].get("Min", 0), allele["CI"].get("Max", 0))


def load_sample(json_path, sample_name_override=None):
    """Load a STRipy JSON and return (sample_name, loci_dict keyed by locus_id)."""
    with open(json_path) as f:
        data = json.load(f)

    if sample_name_override:
        sample_name = sample_name_override
    else:
        input_file = data.get("JobDetails", {}).get("InputFile", "")
        if input_file:
            stem = Path(input_file).stem
            sample_name = stem.split("__")[0]
        else:
            sample_name = Path(json_path).stem.split(".")[0]

    loci = {}
    for entry in data.get("GenotypingResults", []):
        for locus_name, locus in entry.items():
            tl = locus["TargetedLocus"]
            parsed = parse_coords(tl["Coordinates"])
            if not parsed:
                continue
            chrom, start, end = parsed
            alleles = locus["Alleles"]
            a1 = alleles[0]
            a2 = alleles[1] if len(alleles) > 1 else None
            locus_id = str(tl.get("LocusID") or locus_name)
            motif = tl["Motif"]
            loci[locus_id] = {
                "chrom": chrom,
                "pos": start,
                "end": end,
                "id": locus_id,
                "motif": motif,
                "period": len(motif),
                "a1_rep": _to_float(a1["Repeats"]),
                "a2_rep": _to_float(a2["Repeats"]) if a2 is not None else None,
                "a1_ci": _ci_tuple(a1),
                "a2_ci": _ci_tuple(a2) if a2 is not None else (0, 0),
                "a1_out": _to_outlier(a1["IsPopulationOutlier"]),
                "a2_out": _to_outlier(a2["IsPopulationOutlier"]) if a2 is not None else None,
                "a1_z": _to_float(a1["PopulationZscore"]),
                "a2_z": _to_float(a2["PopulationZscore"]) if a2 is not None else None,
                "coverage": locus["Metadata"]["Coverage"],
                "filter": locus["Filter"],
                "diseases": "|".join(sorted({meta["DiseaseSymbol"] for meta in tl["CorrespondingDisease"].values()})),
            }

    return sample_name, loci


VCF_HEADER = {
    "INFO": [
        {"ID": "END", "Number": "1", "Type": "Integer", "Description": "Stop position of the interval"},
        {"ID": "SVTYPE", "Number": "1", "Type": "String", "Description": "Type of structural variant"},
        {"ID": "RU", "Number": "1", "Type": "String", "Description": "Repeat unit in the reference orientation"},
        {"ID": "PERIOD", "Number": "1", "Type": "Integer", "Description": "Length of the repeat unit"},
        {"ID": "DISEASES", "Number": ".", "Type": "String", "Description": "Associated disease symbols for this locus (| separated)"},
        {"ID": "LOCUS", "Number": "1", "Type": "String", "Description": "Gene/locus identifier from STRipy"},
    ],
    "FORMAT": [
        {"ID": "GT", "Number": "1", "Type": "String", "Description": "Unphased genotype"},
        {"ID": "REPCN", "Number": "2", "Type": "Float", "Description": "Number of repeat units spanned by each allele"},
        {"ID": "REPCI1", "Number": "2", "Type": "Integer", "Description": "95% CI min,max on repeat counts of first allele"},
        {"ID": "REPCI2", "Number": "2", "Type": "Integer", "Description": "95% CI min,max on repeat counts of second allele"},
        {"ID": "OUTLIER", "Number": "2", "Type": "Integer", "Description": "Allelic population outlier flags (0/1) assigned by STRipy"},
        {"ID": "ZSCORE", "Number": "2", "Type": "Float", "Description": "Allelic population Z-scores assigned by STRipy"},
        {"ID": "DP", "Number": "1", "Type": "Integer", "Description": "Total Depth"},
        {"ID": "STR_FILTER", "Number": ".", "Type": "String", "Description": "Filter status assigned by STRipy"},
    ],
}


def write_multisample_vcf(samples, out_path, contigs=None):
    """
    samples: list of (sample_name, loci_dict) tuples
    loci_dict: dict keyed by locus_id -> locus data
    """
    header = pysam.VariantHeader()
    header.add_meta("source", value="STRipy2VCF")
    header.add_meta("ALT", items=[("ID", "STR"), ("Description", "Short tandem repeat")])
    for info in VCF_HEADER["INFO"]:
        header.add_meta("INFO", items=list(info.items()))
    for fmt in VCF_HEADER["FORMAT"]:
        header.add_meta("FORMAT", items=list(fmt.items()))

    canonical = {}
    for _, loci in samples:
        for locus_id, loc in loci.items():
            canonical.setdefault(locus_id, loc)

    sorted_loci = sorted(canonical.values(), key=lambda x: (chrom_key(x["chrom"]), x["pos"]))

    if contigs is not None:
        for name, length in contigs:
            header.contigs.add(name, length=length)
    else:
        for chrom_name in dict.fromkeys(loc["chrom"] for loc in sorted_loci):
            header.contigs.add(chrom_name)

    for sample_name, _ in samples:
        header.add_sample(sample_name)

    with pysam.VariantFile(out_path, mode="w", header=header) as vf:
        for loc in sorted_loci:
            rec = header.new_record()
            rec.contig = loc["chrom"]
            rec.start = loc["pos"] - 1
            rec.stop = loc["end"]
            rec.id = str(loc["id"])
            rec.ref = "N"
            rec.alts = ("<STR>",)
            rec.filter.add(loc["filter"] if loc["filter"] else "PASS")
            rec.info["SVTYPE"] = "STR"
            if loc["motif"]:
                rec.info["RU"] = loc["motif"]
            if loc["period"]:
                rec.info["PERIOD"] = int(loc["period"])
            rec.info["DISEASES"] = loc["diseases"]
            rec.info["LOCUS"] = loc["id"]

            for sample_name, loci in samples:
                s_loc = loci.get(loc["id"])
                s = rec.samples[sample_name]
                s["GT"] = (".", ".")
                if s_loc is None:
                    s["REPCN"] = (None, None)
                    s["REPCI1"] = (0, 0)
                    s["REPCI2"] = (0, 0)
                    s["OUTLIER"] = (None, None)
                    s["ZSCORE"] = (None, None)
                    s["DP"] = None
                    s["STR_FILTER"] = ["."]
                else:
                    s["REPCN"] = (s_loc["a1_rep"], s_loc["a2_rep"])
                    s["REPCI1"] = s_loc["a1_ci"]
                    s["REPCI2"] = s_loc["a2_ci"]
                    s["OUTLIER"] = (s_loc["a1_out"], s_loc["a2_out"])
                    s["ZSCORE"] = (s_loc["a1_z"], s_loc["a2_z"])
                    s["DP"] = int(s_loc["coverage"])
                    s["STR_FILTER"] = [str(s_loc["filter"])] if s_loc["filter"] else ["PASS"]

            vf.write(rec)


def main():
    ap = argparse.ArgumentParser(description="Convert STRipy JSON output(s) into a multi-sample VCF with SV-style STR annotations.")
    ap.add_argument("--json", required=True, nargs="+", help="STRipy JSON report(s); one per sample")
    ap.add_argument("-o", "--out", required=True, help="Output VCF")
    ap.add_argument("--sample-names", nargs="+", default=None, help="Sample names (must match number of --json files if provided; defaults to ID extracted from JobDetails.InputFile)")
    ap.add_argument("--reference", default=None, help="Optional reference FASTA for contig lengths")
    args = ap.parse_args()

    if args.sample_names and len(args.sample_names) != len(args.json):
        ap.error(f"--sample-names count ({len(args.sample_names)}) must match --json count ({len(args.json)})")

    overrides = args.sample_names or [None] * len(args.json)
    samples = [load_sample(p, name) for p, name in zip(args.json, overrides)]

    contigs = None
    if args.reference:
        reference = Fasta(args.reference, as_raw=True, read_ahead=1000000)
        contigs = [(name, len(reference[name])) for name in reference.keys()]
        reference.close()

    write_multisample_vcf(samples, args.out, contigs=contigs)


if __name__ == "__main__":
    main()
