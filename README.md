# TechMap – Fall 2025 Group 4

*A small-group data analytics project*

> Maintainers: the repo details below are pre-filled for this class group.

---

## Table of Contents
- [Overview](#overview)
- [What You Need](#what-you-need)
- [Get Access](#get-access)
- [Connect to This Repo](#connect-to-this-repo)
  - [Option A — HTTPS (simplest)](#option-a--https-simplest)
  - [Option B — SSH (recommended once)](#option-b--ssh-recommended-once)
- [Keep Your Copy Up to Date](#keep-your-copy-up-to-date)
- [Add or Update Files](#add-or-update-files)
  - [Web Browser (no installs)](#web-browser-no-installs)
  - [GitHub Desktop (easy app)](#github-desktop-easy-app)
  - [Command Line (power users)](#command-line-power-users)
- [Our Collaboration Workflow](#our-collaboration-workflow)
- [Resolve Merge Conflicts (Quick Tips)](#resolve-merge-conflicts-quick-tips)
- [VS Code Setup (Advanced)](#vs-code-setup-advanced)
- [Large Files / Data](#large-files--data)
- [Repo Structure](#repo-structure)
- [FAQ](#faq)
- [License](#license)

---

## Overview
Welcome! This repository hosts our small‑group data analytics project. Use this README to:
- Connect to the repo
- Pull the latest changes
- Add or retrieve files
- (Optionally) work from VS Code like a pro

If anything here is confusing, tag a teammate in a GitHub issue.

## What You Need
- A **GitHub account** and to be **added as a collaborator** to this repository.
- **Git** installed (macOS/Linux usually have it; Windows: [git-scm.com](https://git-scm.com/)).
- Optional but helpful: **GitHub Desktop** or **VS Code**.

## Get Access
1. Share your GitHub username with the maintainer.
2. Watch for an email/notification that you’ve been added.
3. Sign in at GitHub and open the repo: `https://github.com/rpc180/techmap-fall-2025-group4`.

## Connect to This Repo
Pick **HTTPS** (fastest to start) or **SSH** (one‑time setup, no passwords later).

### Option A — HTTPS (simplest)
```bash
# 1) Clone the repo to your computer (choose any folder)
git clone https://github.com/rpc180/techmap-fall-2025-group4.git

# 2) Go into the repo folder
cd techmap-fall-2025-group4

# 3) Set your name/email (first time only on a machine)
git config user.name "Your Name"
git config user.email "you@example.com"
```
When you push changes, GitHub may prompt for login or a **Personal Access Token** (recommended).

### Option B — SSH (recommended once)
1. Generate a key (press Enter for defaults):
   ```bash
   ssh-keygen -t ed25519 -C "you@example.com"
   ```
2. Copy your **public key** (macOS/Linux):
   ```bash
   cat ~/.ssh/id_ed25519.pub
   ```
   Windows (PowerShell):
   ```powershell
   type $env:USERPROFILE\.ssh\id_ed25519.pub
   ```
3. In GitHub → **Settings** → **SSH and GPG keys** → **New SSH key** → paste it.
4. Test:
   ```bash
   ssh -T git@github.com
   ```
5. Clone with SSH:
   ```bash
   git clone git@github.com:rpc180/techmap-fall-2025-group4.git
   cd techmap-fall-2025-group4
   ```

## Keep Your Copy Up to Date
Always pull the latest work **before** starting and **before** you push:
```bash
git pull origin main
```
(If your default branch is `master`, use `master` instead of `main`.)

## Add or Update Files
Choose the approach you like best.

### Web Browser (no installs)
1. Open the repo on GitHub.
2. Click **Add file ▾ → Upload files** to add, or open a file and click the ✏️ pencil to edit.
3. Add a clear commit message and click **Commit changes**.

### GitHub Desktop (easy app)
1. Install **GitHub Desktop** → **File → Clone repository** → pick this repo.
2. Add/edit files in your local folder.
3. In GitHub Desktop: **Commit to main** → **Push origin**.
4. **Fetch origin** regularly to stay up to date.

### Command Line (power users)
```bash
# 1) Create a new branch for your task (recommended)
git checkout -b feature/<short-task-name>

# 2) Stage and commit your changes
git add <file-or-folder>
git commit -m "feat: add data cleaning script for week 2"

# 3) Push your branch
git push -u origin feature/<short-task-name>

# 4) Open a Pull Request (PR)
# Go to GitHub → You’ll see a prompt to open a PR for your new branch
```
For tiny edits, you may commit directly to `main`, but **branches + PRs** are safer for teamwork.

## Our Collaboration Workflow
1. **Create an issue** describing your task (bug, data import, chart, etc.).
2. **Branch** from `main` using `feature/<task>` naming.
3. Commit often with clear messages following this style: `type(scope): short summary`.
   - Common types: `feat`, `fix`, `docs`, `chore`, `refactor`.
4. Open a **Pull Request** and tag reviewers.
5. After approval, **Squash & Merge** to keep history tidy.
6. **Delete your branch** after merge.

## Resolve Merge Conflicts (Quick Tips)
- Pull the latest: `git pull origin main`.
- If Git reports conflicts, open the files shown and keep the correct lines between the conflict markers `<<<<<<<`, `=======`, `>>>>>>>`.
- When fixed:
  ```bash
  git add <conflicted-files>
  git commit
  git push
  ```
- If stuck, ask in the PR—don’t fight it alone.

## VS Code Setup (Advanced)
1. Install **VS Code** and these extensions:
   - **GitHub Pull Requests and Issues**
   - **GitLens – Git supercharged**
2. In VS Code: **View → Command Palette → Git: Clone** → paste repo URL.
3. Open the folder when prompted.
4. **Source Control** view lets you stage, commit, push, pull.
5. Use the **GitHub** extension pane to review & manage Pull Requests inside VS Code.
6. Optional: add a `.vscode/extensions.json` with recommended extensions for the team.

## Large Files / Data
- Keep the repo **fast**: avoid committing raw datasets over ~100 MB.
- If needed, set up **Git LFS**:
  ```bash
  git lfs install
  git lfs track "data/*.csv"
  git add .gitattributes
  git commit -m "chore: track large data files with Git LFS"
  ```
- Prefer storing bulky data in cloud storage, then reference it in a `/data/README.md`.

## Repo Structure
```
techmap-fall-2025-group4/
├─ data/              # small sample data, schemas, README for external links
├─ notebooks/         # Jupyter/Colab notebooks
├─ src/               # scripts and modules
├─ reports/           # figures, exports
├─ docs/              # project docs, meeting notes
├─ .gitignore
├─ README.md
└─ LICENSE
```

## FAQ
**Q: I get “permission denied” on push.**  
A: You probably need to be added as a collaborator, or you’re using HTTPS without a token. Check repo access; for HTTPS create a **Personal Access Token** with `repo` scope and use it as your password.

**Q: My local `main` is behind the remote.**  
A: Run `git pull origin main`. If you have local changes, commit/stash first.

**Q: I created the wrong branch name.**  
A: `git branch -m old-name new-name` then `git push -u origin new-name` and delete the old remote branch in GitHub.

## License
Choose a license and add it as `LICENSE` (MIT is common for class projects). You can create one via **Add file → Create new file → Choose a license template** in GitHub.

