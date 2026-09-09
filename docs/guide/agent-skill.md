# Using the Hedloom study skill

The checkout includes `.agents/skills/hedloom-study/SKILL.md` for authoring or
modifying studies, submitting authorized runs, reading exported results, and
finding saved runs. It provides a compact common workflow through Hedloom's
public facade and points to the relevant guide when more detail is needed.
Engine development is outside its scope.

Start Codex in the Hedloom checkout or one of its directories. Codex discovers
repository skills from `.agents/skills` between its working directory and the
repository root. Explicitly select this skill in a prompt:

```text
$hedloom-study Add a new sweep point to this study, submit it using site.toml,
and compare its exported verdict and reused work with the previous run.
```

Include the study path, Site profile, and intended execution scope in your
request. For discovery alone, ask it to inspect an existing run without
submitting another.

When working in another project, optionally copy the entire `hedloom-study`
folder into your configured user skills directory (the documented user location
is `~/.agents/skills`) or that project's `.agents/skills`. Keep the Hedloom
checkout and its Python environment available. If the skill does not appear,
restart Codex; `/skills` lists available skills. See
[OpenAI's skill discovery documentation](https://learn.chatgpt.com/docs/build-skills#where-codex-loads-local-skills)
for locations and invocation behavior.

Evaluation so far covers local sequential authoring, modification, submission,
result use, and discovery. It does not establish farm coverage. For farm work,
read [Sites and placements](sites.md) and the
[first-farm-run guide](first-farm-run.md).
