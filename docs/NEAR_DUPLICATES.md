# Near-duplicate deletion-risk review

Two fixed lexical rules are evaluated: word-set Jaccard and word 3-shingle Jaccard. Text undergoes NFKC normalization, whitespace collapse and case folding for this diagnostic. Threshold 0.8 was fixed before inspecting the sampled labels. The additional thresholds are sensitivity curves, not a test-set search for a publishable score.

| 200-pair split | Rule at 0.8 | TP | FP | FN | Precision | Recall |
|---|---|---:|---:|---:|---:|---:|
| Validation | word set | 90 | 100 | 0 | 47.37% | 100% |
| Validation | 3-shingles | 5 | 0 | 85 | 100% | 5.56% |
| Test | word set | 91 | 98 | 1 | 48.15% | 98.91% |
| Test | 3-shingles | 8 | 2 | 84 | 80% | 8.70% |

The small validation sample's perfect precision for 3-shingles was based on only five predicted positives. The held-out sample includes two false positives. This is evidence against enabling the rule as a general automatic deletion policy. The high-recall word-set rule is especially unsafe for this adversarial distribution: changing word order can change the meaning without changing the token set.

The full sentences, original pair IDs, scores and human labels of representative false positives are in `evidence/near-duplicates.json`. Each candidate can be traced back to `fixtures/near-duplicates/paws-sample.json` and its pinned raw-source hash. Candidate generation returns `review_only` and does not mutate source records or publication output.

PAWS human labels indicate semantic paraphrase equivalence, not the business decision that one document should be deleted. These precision/recall values therefore measure deletion risk under a semantic-equivalence proxy in the sampled adversarial benchmark. They do not estimate production duplicate prevalence, multilingual performance or real training-quality improvement. Use corpus-specific annotation before enabling automatic near-deduplication.
