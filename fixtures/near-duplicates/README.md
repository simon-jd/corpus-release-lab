# Human-label evaluation source

Source: Google LLC, **PAWS: Paraphrase Adversaries from Word Scrambling**, Yuan Zhang, Jason Baldridge and Luheng He, NAACL 2019.

- Dataset and annotation explanation: https://github.com/google-research-datasets/paws
- Original permission: https://github.com/google-research-datasets/paws/blob/master/LICENSE
- Public mirror: https://huggingface.co/datasets/google-research-datasets/paws/tree/161ece9501cf0a11f3e48bd356eaa82de46d6a09

The original archive URL returned HTTP 403 on 2026-09-07; the public Hub mirror's pinned `labeled_final` test and validation Parquet files were used. The exact URLs, SHA-256 values, input row counts and deterministic label-independent sampling policy are preserved in `paws-sample.json`. Labels are copied unchanged from the published human-annotated subset. The noisy/unlabeled subset is not used.

400 pairs are included: 200 validation, 200 test. This is a small adversarial paraphrase benchmark, not a representative deduplication corpus. No new human annotation or independent human review by this project is claimed. The benchmark permission applies to these data; the project's MIT license applies to code.
