# MIP introduction presentation

Generated deck (repo-grounded narrative; slides 15–16 labeled target architecture):

| File | Purpose |
|------|---------|
| `mip_deck_repo_findings.md` | Evidence inventory — implemented vs partial vs planned; object names cited in slides. |
| `build_mip_introduction_deck.py` | Rebuild **`MIP_Introduction_Deck.pptx`** (20 slides). |
| `MIP_Introduction_Deck.pptx` | Output slides (commit after regeneration). |

## Regenerate

```bash
pip install python-pptx
python MIP/docs/presentations/build_mip_introduction_deck.py
```

Screenshots referenced in **`mip_deck_repo_findings.md`** (`UI routes worth capturing`) can be added manually or by extending `build_mip_introduction_deck.py` with `slide.shapes.add_picture(...)`.
