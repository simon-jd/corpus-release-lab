import pytest
from corpus_release.near_duplicates import evaluate, similarity, cross_split_candidates


def test_order_sensitive_shingles_and_empty_values():
    assert similarity('alpha beta gamma delta', 'alpha beta gamma delta') == 1
    assert similarity('', '') == 0
    assert similarity('alpha beta gamma delta', 'gamma delta alpha beta', 1) == 1
    assert similarity('alpha beta gamma delta', 'gamma delta alpha beta', 3) == 0


def test_precision_recall_and_false_deletion():
    data=[{'id':'true','sentence1':'alpha beta gamma','sentence2':'alpha beta gamma','label':1},
          {'id':'false','sentence1':'New York to Florida','sentence2':'Florida to New York','label':0},
          {'id':'miss','sentence1':'a short car','sentence2':'a small automobile','label':1},
          {'id':'negative','sentence1':'red apples','sentence2':'blue oceans','label':0}]
    result=evaluate(data,width=1,threshold=.8)
    assert [result[k] for k in ('tp','fp','fn','tn')]==[1,1,1,1]
    assert result['precision']==result['recall']==.5
    assert next(r for r in result['predictions'] if r['id']=='false')['outcome']=='fp'
    assert evaluate([],threshold=1)['precision'] is None
    with pytest.raises(ValueError): evaluate(data+data)


def test_cross_split_diagnostic_never_mutates_source():
    records=[{'id':'a','split':'train','text':'alpha beta gamma'}, {'id':'b','split':'test','text':'alpha beta gamma'}]
    assert cross_split_candidates(records)[0]['action']=='review_only'
    assert len(records)==2
    with pytest.raises(ValueError): cross_split_candidates(records,max_records=1)
