## About My Business
I work in enterprise software marketing. My company **Bhavsar Growth Consulting** helps B2B SaaS companies with their GTM operations.  I have various projects across different clients that I'd like your help with.  

# WAT Framework — Agent Instructions

You operate inside the **WAT framework** (Workflows, Agents, Tools). Probabilistic AI handles reasoning; deterministic code handles execution. This separation is what makes the system reliable.

## Architecture

| Layer | Role | Location |
|-------|------|----------|
| **Workflows** | Markdown SOPs defining objectives, inputs, tools, outputs, and edge cases | `workflows/` |
| **Agents** | You — intelligent orchestration, sequencing, error recovery | (this role) |
| **Tools** | Python scripts for API calls, data transforms, file ops, DB queries | `tools/` |

**Why it matters:** When AI handles every step directly, accuracy compounds downward (~59% after five 90%-accurate steps). Offload execution to deterministic scripts; stay focused on orchestration.

## Common Commands

```bash
# Run a tool
python tools/<script_name>.py

# Check available tools
ls tools/

# Check available workflows
ls workflows/

# Environment variables
cat .env  # NEVER store secrets anywhere else
```

## Directory Layout

```
.tmp/            # Disposable intermediates (scraped data, temp exports)
tools/           # Python scripts for deterministic execution
workflows/       # Markdown SOPs — what to do and how
.env             # API keys and env vars (ONLY secrets location)
credentials.json # Google OAuth (gitignored)
token.json       # Google OAuth (gitignored)
```

**Core rule:** Local files are for processing. Final deliverables go to cloud services (Google Sheets, Slides, etc.). Everything in `.tmp/` is regenerable and disposable.

## How to Operate

### 1. Always check existing tools first

Before building anything, check `tools/` for what your workflow requires. Only create new scripts when nothing exists for the task.

### 2. Follow the workflow

For any task, find and read the matching workflow in `workflows/` before acting. Workflows define the inputs, tool sequence, expected outputs, and edge cases. Follow them step by step.

### 3. Handle failures systematically

When you hit an error:
1. Read the full error message and traceback
2. Fix the script and retest
3. **IMPORTANT:** If the fix involves paid API calls or credits, check with me before re-running
4. Document what you learned in the workflow (rate limits, timing quirks, unexpected API behavior)

### 4. Keep workflows current

Update workflows as you discover better methods, constraints, or recurring issues. **Do NOT create or overwrite workflows without asking** unless explicitly told to. These are living instructions — preserve and refine them.

## Self-Improvement Loop

Every failure makes the system stronger:
1. Identify what broke
2. Fix the tool
3. Verify the fix works
4. Update the workflow with the new approach
5. Move on with a more robust system

## Key Constraints

- **Secrets:** `.env` only. Never hardcode keys or store them elsewhere.
- **Deliverables:** Always push final outputs to cloud services where I can access them.
- **Workflow authority:** Workflows are the source of truth for execution steps. Don't improvise when a workflow exists.
- **Ask when uncertain:** If a task is ambiguous or a workflow is missing, ask a clarifying question before proceeding.

## When Compacting

When compacting conversation context, always preserve: the current task objective, which workflow is active, any tool errors encountered and their fixes, and the list of files modified in this session.