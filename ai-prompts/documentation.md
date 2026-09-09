# AI Prompts — Documentation

Prompt history for assessment documentation deliverables.

---

## Prompt 1: Initial planning documents

**PROMPT SENT:**

> Analyze the assessment PDF and create requirements-analysis.md, design-notes.md,
> and data-model.md without implementing code yet.

**AI RESPONSE SUMMARY:**

- Produced structured docs covering functional requirements, architecture, ER diagram,
  layer designs, DQ strategy outline, and intentional defect catalog

**YOUR EVALUATION:**

- Accepted as foundation for implementation
- Minor adjustments: Gold table column names aligned with dashboard needs

---

## Prompt 2: Submission documentation batch

**PROMPT SENT:**

> Draft the docs folder:
> README, candidate-info, tool-workflow, data-quality-strategy,
> debugging-notes, reflection, final-ai-usage-summary;
> data/seed-data-notes.md;
> ai-prompts/debugging.md and documentation.md

**AI RESPONSE SUMMARY:**

- Generated docs from codebase, conversation history, and validated metrics
- Cross-referenced existing `requirements-analysis.md`, `design-notes.md`, `data-model.md`
- Documented real debugging story (1640.0 casting) and dashboard completion

**YOUR EVALUATION:**

- Review candidate-info for dashboard URL and GitHub link before submission
- Customize reflection voice if needed for personal tone

---

## Documentation principles applied

| Principle | Example |
|-----------|---------|
| Link related docs | README index table |
| Include run commands | Bronze/Silver/Gold notebook paths |
| Document real bugs | `debugging-notes.md` casting issue |
| Preserve AI transparency | `final-ai-usage-summary.md`, `ai-prompts/` |
| Assessment mapping | DQ expected counts, deliverables checklist |

---

## Files produced in this batch

```text
docs/
├── README.md
├── candidate-info.md
├── tool-workflow.md
├── data-quality-strategy.md
├── debugging-notes.md
├── reflection.md
└── final-ai-usage-summary.md

data/
└── seed-data-notes.md

ai-prompts/
├── debugging.md
└── documentation.md
```

**Pre-existing (unchanged):**

- `docs/requirements-analysis.md`
- `docs/design-notes.md`
- `docs/data-model.md`

---

## FINAL DECISION

Use `docs/README.md` as the primary entry point for reviewers. Root-level README optional — link to `docs/` if added later.
