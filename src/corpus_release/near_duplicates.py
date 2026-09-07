"""Offline lexical near-duplicate evaluation. Never silently deletes records."""
import hashlib
import json
import re
from .release import normalize


def shingles(text, width=3):
    if type(width) is not int or width < 1:
        raise ValueError('shingle width must be a positive integer')
    tokens = re.findall(r'\w+', normalize(text).casefold())
    if not tokens:
        return set()
    if len(tokens) < width:
        return {tuple(tokens)}
    return {tuple(tokens[i:i+width]) for i in range(len(tokens)-width+1)}


def similarity(left, right, width=3):
    a, b = shingles(left, width), shingles(right, width)
    return len(a & b)/len(a | b) if a or b else 0.0


def evaluate(pairs, *, width=3, threshold=.8):
    if not 0 <= threshold <= 1:
        raise ValueError('threshold must be between zero and one')
    seen, predictions = set(), []
    counts = dict(tp=0, fp=0, tn=0, fn=0)
    for pair in pairs:
        if pair['id'] in seen or type(pair['label']) is not int or pair['label'] not in (0, 1):
            raise ValueError('pair IDs must be unique and labels binary integers')
        if not all(isinstance(pair[k], str) for k in ('sentence1', 'sentence2')):
            raise ValueError('pair sentences must be strings')
        seen.add(pair['id'])
        score = similarity(pair['sentence1'], pair['sentence2'], width)
        predicted = int(score >= threshold)
        outcome = {(1,1): 'tp', (1,0): 'fp', (0,0): 'tn', (0,1): 'fn'}[(predicted,pair['label'])]
        counts[outcome] += 1
        predictions.append({'id':pair['id'], 'score':score, 'label':pair['label'], 'predicted':predicted, 'outcome':outcome})
    return {'shingle_width':width, 'threshold':threshold, 'pairs':len(pairs), **counts,
            'precision':counts['tp']/(counts['tp']+counts['fp']) if counts['tp']+counts['fp'] else None,
            'recall':counts['tp']/(counts['tp']+counts['fn']) if counts['tp']+counts['fn'] else None,
            'false_positive_rate':counts['fp']/(counts['fp']+counts['tn']) if counts['fp']+counts['tn'] else None,
            'predictions':predictions}


def cross_split_candidates(records, *, width=3, threshold=.8, max_records=2000):
    """Bounded all-pairs diagnostic, explicitly unsuitable for large-scale use."""
    if len(records) > max_records:
        raise ValueError('bounded near-duplicate review exceeded max_records')
    if len({r['id'] for r in records}) != len(records):
        raise ValueError('record IDs must be unique')
    if not 0 <= threshold <= 1:
        raise ValueError('invalid threshold')
    result = []
    for i, left in enumerate(records):
        for right in records[i+1:]:
            if left['split'] == right['split']:
                continue
            score = similarity(left['text'], right['text'], width)
            if score >= threshold:
                result.append({'left':left['id'], 'right':right['id'], 'score':score,
                               'action':'review_only', 'left_split':left['split'], 'right_split':right['split']})
    return result
