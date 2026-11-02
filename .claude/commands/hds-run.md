---
description: Run one Spec Kit stage for an HDS feature using its stored prompt
argument-hint: <feature-number 0-5> <stage: constitution|specify|plan>
---

Arguments: $ARGUMENTS

1. Parse the arguments as `<feature-number> <stage>`. Feature 0 is the constitution; features 1 to 5 are the folders in `.claude/hds/prompts/` (for example `01-foundation`, `02-availability`, `03-scheduling`, `04-pay-reports`, `05-deploy`).
2. Read the prompt file `.claude/hds/prompts/<NN>-*/<stage>.md`. If it does not exist, say so and stop.
3. Run the matching Spec Kit workflow (`speckit-constitution`, `speckit-specify` or `speckit-plan`; its definition is under `.claude/skills/` or `.claude/commands/`) and pass the **exact contents of the prompt file** as its input. Do not paraphrase, shorten or add to the prompt, and do not add principles, requirements or technology of your own.
4. For `specify`, the spec must name no technology. For `plan`, follow the constitution and `docs/hds-spec.md`.
5. When the stage finishes, show where the output was written and the next step (`/hds-status`). Do not start the next stage yourself.
