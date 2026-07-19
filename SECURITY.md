# Security Guidelines & Secret Management Best Practices

This document outlines key rules, best practices, and emergency instructions to prevent API key leakage and maintain a secure development environment.

---

## 1. Secret Scanning Alert: Push Blocked
If Git blocks your push with a **"Secret Detected"** alert (e.g., GitHub Secret Scanning), it means you have accidentally committed hardcoded credentials to your history.

### How to Fix Git History (Emergency Protocol)
Simply changing the code and creating a new commit **does not remove the key from Git history**. The key is still present in previous commits and can be extracted. You must rewrite the history:

#### Scenario A: The commit has NOT been pushed to GitHub (Push was blocked)
If you attempted to push but it was blocked, the commits are only on your local machine. You can safely undo the commit and remove the secret:
1. **Undo the last commit** while keeping your changes in the workspace:
   ```bash
   git reset HEAD~1
   ```
2. **Move the secrets** out of code files and into ignored files (e.g., `.env` or `data/keys.json`).
3. **Stage and commit the clean files**:
   ```bash
   git add analyzer.py
   git commit -m "Refactor: remove hardcoded API keys and load from keys.json"
   ```
4. **Push again**:
   ```bash
   git push
   ```

#### Scenario B: The commit has already been pushed to GitHub
If a secret was successfully pushed to a public repository, **assume the key is compromised** and revoke it immediately at the provider's console. Then, use git history cleaner tools:
1. Revoke the key immediately!
2. Install `git-filter-repo` (recommended) or use `git filter-branch` to purge the secret from all branches, tags, and commits.

---

## 2. Best Practices for Secret Management

### Rule 1: Always Use Environment Variables or Ignored Files
- **Environment Variables**: Store single keys in a `.env` file (e.g., `OPENAI_API_KEY`, `SERPER_API_KEY`). Use `python-dotenv` to load them.
- **Config Files for Bulk Secrets**: For rotating keys, store them in a JSON config file inside a directory that is completely ignored by Git (e.g., `data/keys.json`).
- **Access Control**: Never commit `.env` or `data/keys.json` to version control.

### Rule 2: Maintain a Strict `.gitignore` File
Ensure your `.gitignore` is populated before starting a project. For this repository, the following are ignored:
```gitignore
# Local environment variables
.env

# Data & Config files
data/
```
Because `data/` is ignored, placing `keys.json` or output leads inside `data/` prevents accidental exposure.

### Rule 3: Use Template Files
Always commit a `.env.example` or `data/keys.json.example` template containing mock placeholders so that other developers know what configurations are required without exposing actual credentials:
```env
# .env.example
SERPER_API_KEY=your_serper_key_here
APOLLO_API_KEY=your_apollo_key_here
INSTANTLY_API_KEY=your_instantly_key_here
INSTANTLY_CAMPAIGN_ID=your_campaign_id_here
```

### Rule 4: Run Pre-Commit Checks
Integrate tools like [gitleaks](https://github.com/gitleaks/gitleaks) or [TruffleHog](https://github.com/trufflesecurity/trufflehog) in your local pre-commit hook to scan files for secrets *before* committing:
```bash
# Example: Install gitleaks locally
brew install gitleaks
gitleaks detect --verbose
```
