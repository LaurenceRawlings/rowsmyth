## PR title

The PR title becomes the squash merge commit message and must follow
[Conventional Commits](https://www.conventionalcommits.org/):

```
type(optional-scope): short description
```

Choose **one** type - keep each PR focused on a single kind of change so
`cz bump` can determine the correct version bump from the commit history.

| Type | Version bump | When to use |
|------|-------------|-------------|
| `feat` | minor | New user-visible feature |
| `fix` | patch | Bug fix |
| `feat!` / `fix!` | major | Breaking change (append `!`) |
| `docs` | none | Documentation only |
| `style` | none | Formatting, whitespace - no logic change |
| `refactor` | none | No functional change |
| `test` | none | Tests only |
| `perf` | patch | Performance improvement |
| `build` | none | Build system or dependency changes |
| `ci` | none | CI/CD configuration changes |
| `chore` | none | Maintenance not covered by other types |
| `revert` | patch | Reverts a previous commit |

## Description

<!-- What does this PR change and why? -->

## Checklist

- [ ] README updated if public API changed
- [ ] `docs/usage.md` updated if public API changed
- [ ] Docstring examples added or updated for any new or changed public API
