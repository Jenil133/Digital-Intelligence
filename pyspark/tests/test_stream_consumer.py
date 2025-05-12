from di_features.stream_consumer import parse_and_split, to_dataframe


def test_parse_and_split_quarantines_bad_json(spark):
    batch = {
        "di:signals:t1": [
            (b"1-0", {b"data": b'{"event_id":"e1","tenant_id":"t1","session_id":"s1","ts":"2026-05-01T00:00:00Z"}'}),
            (b"1-1", {b"data": b"not-json"}),
        ]
    }
    df = to_dataframe(spark, batch)
    good, bad = parse_and_split(df)
    good_rows = good.collect()
    bad_rows = bad.collect()
    assert len(good_rows) == 1
    assert good_rows[0].event_id == "e1"
    assert len(bad_rows) == 1
    assert bad_rows[0].raw == "not-json"
