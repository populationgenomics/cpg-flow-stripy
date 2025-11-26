from functools import cache

from cpg_utils import config


@cache
def get_loci_lists(dataset: str) -> dict[str, list[str]]:
    """
    Get the loci lists in scope for a given dataset, as a mapping of loci list name to list of loci
    """
    loci_lists = config.config_retrieve(['stripy', 'loci_lists'])
    loci_list_datasets = config.config_retrieve(['stripy', 'loci_lists_datasets'])

    in_scope = [ll_name for ll_name, datasets in loci_list_datasets.items() if dataset in datasets]

    return {ll_name: loci for ll_name, loci in loci_lists.items() if ll_name in in_scope}
