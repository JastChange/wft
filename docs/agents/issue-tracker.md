# Issue tracker: GitHub

Issues and specs for this repo live as GitHub issues. Use the `gh` CLI for all operations.

## Conventions

- Create: `gh issue create --title "..." --body "..."`
- Read: `gh issue view <number> --comments`
- List: `gh issue list --state open`
- Comment: `gh issue comment <number> --body "..."`
- Label: `gh issue edit <number> --add-label "..."`
- Close: `gh issue close <number> --comment "..."`

Infer the repository from `git remote -v`.

## Pull requests as a triage surface

**PRs as a request surface: no.**

GitHub issues are the primary request and triage surface. Resolve ambiguous
references such as `#42` by checking whether they refer to a PR or issue.

## Skill operations

- “Publish to the issue tracker” means creating a GitHub issue.
- “Fetch the relevant ticket” means reading the GitHub issue and its comments.
- Use GitHub sub-issues or task lists for parent/child issue relationships.
- Use native issue dependencies where available.
- Claim work by assigning the issue to the current GitHub user.
