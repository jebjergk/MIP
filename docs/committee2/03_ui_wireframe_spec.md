# Committee 2.0 — UI

## Route

- `/structural-committee` — empty state
- `/structural-committee/:hearingId` — hearing room
- Optional `?proposal_id=` opens then navigates to hearing id

## Sections

1. **Header** — symbol, side, ids, stance badge, confidence, Refresh, Commit final  
2. **Two columns** — proposal snapshot vs live evidence  
3. **Delta strip** — category chips (tooltips via `title`)  
4. **Specialist board** — grid cards (6 roles) + mini artifacts  
5. **Chair** — supports, tensions, execution shaping JSON, what changed  
6. **Session compare** — after refresh, previous vs current stance  

**No** chat bubbles. Evidence-first, compact premium styling (`StructuralCommitteeHearing.css`).

## Entry

Structural Timeline: **Committee 2.0** strip lists proposals → opens hearing.
