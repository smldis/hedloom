from pathlib import Path


def test_the_docs_do_not_claim_chmod_prevents_modification():
    root = Path(__file__).parents[1]
    text = (root / "ONTOLOME.md").read_text() + (root / "README.md").read_text()
    assert "not operating-system enforcement" in text
    assert "refuse, detect, and record" in text
