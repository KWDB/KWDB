---
name: kwdb-document-writing
description: |
  Use when the user requests writing functional specifications, design documents,
  PRDs, technical design documents, or needs to analyze KWDB database requirements.
---

# KWDB Document Writing Skill

This skill assists users in writing functional specifications and design documents for the KWDB database.

## Workflow

```
Requirement Understanding → Clarify Questions → Codebase Analysis → Functional Spec Generation → Design Doc Generation → Output Files
```

**Important**: This skill is located within the KWDB code repository. Before generating design documents,
you must analyze existing implementations in the codebase, referencing actual code paths, function names,
and module structures.

## Phase 1: Requirement Understanding and Analysis

When the user proposes a requirement, first perform **requirement clarification** to ensure complete understanding.

### 1.1 Basic Information Extraction

First, read the template files to understand the document structure:
- Functional spec template: `references/functional-spec-template.md`
- Design doc template: `references/design-template.md`

### 1.2 Multi-Round Clarification Questions

If the user's requirements are not clear enough, use the following question framework for clarification:

**Requirement Background:**
- What problem does this feature solve? What is the user's pain point?
- Is there a related requirements document or issue link?
- Who is the target user group? (DBA, application developers, operations, etc.)

**Technical Scope:**
- Which modules does this feature involve? (SQL layer, storage layer, cluster management...)
- Does it require modifying existing modules? Which ones?
- Does it depend on third-party libraries or external systems?

**Constraints:**
- Are there performance indicator requirements?
- Are there compatibility requirements? (e.g., backward compatibility with older versions)
- Does it need to consider high-availability scenarios?

**Output Expectations:**
- Do you need both a functional spec and a design document?
- Is there a specific output directory requirement?

### 1.3 Requirement Classification Framework

Classify the requirement based on the information provided by the user:

| Category          | Characteristics                              | Document Focus                              |
|-------------------|----------------------------------------------|---------------------------------------------|
| New Feature       | Brand new feature, no existing dependencies  | Requirement background, user scenarios, scope |
| Feature Enhancement | Extends existing functionality             | Existing feature analysis, incremental changes |
| Performance Optimization | Optimizes existing implementation      | Performance metrics, benchmark tests         |
| Bug Fix           | Fixes a known issue                          | Root cause, solution                        |
| Architecture Adjustment | Adjusts system architecture             | Module partitioning, call relationships      |

### 1.4 AI-Friendly Documentation

If necessary, you can read the following documents to learn more about the KWDB code repository:
- `AGENTS.md` — AI agent working guide
- `docs/agents/architecture-index.md` — KWDB module and code index
- `docs/agents/resource-index.md` — AI-available resource index

## Phase 2: Codebase Analysis

The module design and interface design in the design document MUST reference actual code structures.

**Steps**:
1. Read `docs/agents/architecture-index.md` to understand KWDB architecture and module partitioning
2. Based on the modules involved in the requirement, search for relevant code files and implementation locations
3. Analyze the directory structure and confirm core file locations
4. Record call relationships and existing implementations

**Requirement**: Code references must be confirmed through actual searches. Mark as `[TBD]` if unverifiable.

## Phase 3: Functional Specification Generation

Fill in based on `references/functional-spec-template.md` using Chinese, strictly following the chapters and format in the template.

**Required chapters (must have substantive content):**
- 2.1 Summary — brief description of the requirement and solution
- 2.4 Detailed problem statement and solution outline — core problem analysis
- 3.x Functional specification details — detailed functional specs
- 5.x Scenario demonstrations — at least one complete use case

**KWDB-Specific Considerations:**
- SQL syntax changes must include explicit syntax examples
- Command-line tool (kwbase) changes must list all affected commands
- Configuration parameter changes must state default values and value ranges
- Version compatibility must explain which versions support rolling upgrades

**Terminology**: In section 1.1, automatically add KWDB-specific terms (node, cluster, time-series table, relational table, kwbase). Depending on the feature type, more terms may be added.

For KWDB-specific terms, automatically add the following terms in section 1.1:

| Abbreviation/Term | Explanation                                                              |
|-------------------|--------------------------------------------------------------------------|
| Node              | A KaiwuDB node, a physical server running database services. Multiple nodes form a cluster |
| Cluster           | A topological form of multiple KaiwuDB database services, consisting of at least 3 nodes |
| Time-series Table  | A table supporting timestamp indexes, suitable for IoT and other time-series scenarios |
| Relational Table  | A standard SQL relational table                                          |
| kwbase            | KaiwuDB command-line management tool                                     |

Additional relevant terms may be added based on the feature type.

## Phase 4: Design Document Generation

Fill in based on `references/design-template.md` using Chinese, strictly following the chapters and format in the template.

**Technical Depth Requirements:**
- 3.x Design Overview — describe at the architectural level
- 4.x Detailed Design — down to class/function level, including call relationships
- 5.x External Interfaces — explicit API signatures
- 6.x Key Algorithms — time/space complexity

**KWDB Architecture Reference:** See `docs/agents/architecture-index.md`

**Related Functional Spec:** In section 1.2 related documents, automatically add a reference link to the functional spec.

## Phase 5: Output and Save

### 5.1 File Naming Convention

```
Functional Spec: {feature_name}_功能规格书_{date}.md
Design Doc:      {feature_name}_设计文档_{date}.md
```

### 5.2 Output Confirmation

After generating documents, confirm with the user:
1. Whether the document content needs adjustment
2. Whether specific chapters need to be supplemented
3. Confirm the save path

### 5.3 Cross-Document Linking

Ensure the design document includes:
- Section 1.2 references the corresponding functional spec
- DCR number is consistent with the functional spec

## Example Dialog

**User**: "I want to add a time_bucket_gapfill function to KaiwuDB for filling missing values in time-series data"

**Assistant**:
1. Read templates to understand document structure
2. Clarify questions: fill strategy, aggregation functions, dependencies, etc.
3. Codebase analysis: read architecture-index.md, search for related code implementations
4. Generate functional spec
5. Generate design doc (referencing actual code paths)
6. Save and confirm

## Quality Checklist

- [ ] Glossary is populated (section 1.1)
- [ ] Requirement background is clear (sections 2.1/2.4)
- [ ] At least one complete scenario demonstration (section 5.x)
- [ ] SQL syntax has concrete examples
- [ ] Configuration parameters have default values and ranges
- [ ] Version compatibility is stated
- [ ] Design doc references the functional spec
- [ ] Design doc includes code analysis (referencing actual file paths and function names)
- [ ] Code references have been confirmed through actual searches
- [ ] Documents use Chinese, terminology is consistent
