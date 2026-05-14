# Go and C++ coding style (KWDB)

This file is the **repository-maintained** coding guide for Go and C++ work in
KWDB. It is suitable for an open-source checkout: it carries no links to
vendor-specific document systems.

Agents and contributors should treat this document as the normative summary. If
project code or tooling clearly disagrees with a rule here, prefer **matching
the surrounding module** and propose a doc or code alignment through review.

Further public references (not exhaustive):

- Go: [Effective Go](https://go.dev/doc/effective_go), [Go Doc Comments](https://go.dev/doc/comment)
- C++ layout and naming: [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html)

---

## Go

### Formatting

- Use `gofmt`; prefer `goimports` so imports stay grouped and unused imports are
  removed.
- When a function signature does not fit comfortably on one line, split the
  name, parameters, and return types across lines. Place the closing `)` for the
  parameter list at the same indentation as `func`. If the parameter list is very
  long, use one parameter per line. Apply the same idea to long return type
  lists.

### Declaration grouping

- Group related `const`, `var`, and `type` declarations.
- Do not group unrelated declarations together.
- Grouping may appear at package level or inside functions.

### Line length

- Assume a ~100-column editing width for code and ~80 for comments (2-space tab
  stops), unless wrapping hurts readability.

### Comments

- Write comments alongside exports as you go; exported identifiers need Go doc
  comments (`godoc`). Comments should be full sentences, starting with the name
  being documented and ending with a period.
- Prefer English in comments to reduce encoding and tooling friction.
- Package comments live in a block or line comment immediately before
  `package`; one file per package is enough.
- Optional markers: `TODO:`, `FIXME:`, `NOTE:` for maintainers (keep them
  actionable).

### Naming

- If a name needs a comment to make sense, rename it.
- Prefer searchable names over single letters except for very small local
  scopes. Scale name length to scope and importance.
- Avoid meaningless duplicates such as `Product` vs `ProductInfo` vs
  `ProductData` without a real distinction.
- **Functions:** camelCase; verbs or verb phrases (`postPayment`, `deletePage`).
  Use `get` / `set` / `is` prefixes only when they match common Go patterns and
  readability.
- **Structs:** nouns or noun phrases (`Customer`, `WikiPage`, `Account`,
  `AddressParser`). Avoid vague words like `Manager`, `Processor`, `Data`, `Info`
  as type names.
- **Packages:** lower case, single word when possible, no underscores or
  `MixedCaps`. Not plural (`url` not `urls`). Avoid names with no information
  (`common`, `util`, `shared`, `lib`).
- **Constants:** ALL_CAPS with underscores between words. For enumerations,
  introduce a named type when it helps clarity; use consistent prefixes when
  many constants coexist in one area.
- **Variables:** follow normal English or accepted abbreviations (`u` for
  `user` only in tight local scope, `uid` for `userID`, etc.). Booleans:
  `Has…`, `Is…`, `Can…`, `Allow…`. Initialisms: respect Go conventions (`API`,
  `URL`, `ID`; not `Url`).
- Prefer `:=` when the value is clearly assigned; use `var` when the zero value
  or type is the point (for example empty slices). Keep scope as small as
  practical without harming clarity.

### Functions and files

- Order functions roughly by call flow; group methods by receiver type.

### Structs

- Prefer multi-line declaration and literal initialization when clarity improves.
- When taking the address of an empty struct, prefer `&T{}` over `new(T)` if it
  matches prevalent local style.

### Control flow

- Use `if` short declarations for scoped locals.
- Loop variables: prefer short declarations scoped to the loop.
- Return early on errors and special cases; keep nesting shallow.
- Avoid `panic` except for unrecoverable programmer errors or unavoidable
  situations called out by maintainers.

### Imports

- Separate standard library imports from other imports with a blank line.
- In non-test code, do not use dot imports.
- Do not use relative imports such as `./subpackage`; paths must work with the
  module layout and `go get`.
- Use `goimports` (or equivalent) to keep grouping stable.

### Parameter passing

- Pass small values by value; use pointers for larger structs when copying cost
  matters.
- Do not pass pointers to maps, slices, or channels; they are already reference
  semantics.

### SQL-facing code

- Use one vocabulary for the same concept across statements (`variable` vs
  `value`, etc.).
- Prefer snake_case identifiers for SQL-facing names; avoid unnecessary quoting;
  avoid naming result columns or fields `user` due to grammar hazards; prefer
  `user_name`-style clarity.
- Disambiguate generic words: `zone_id`, `job_id`, `table_id` instead of naked
  `id`; prefer `*_name` patterns when “name” would be overloaded.
- If an object might be keyed by multiple handle styles (by id vs by name),
  encode the handle flavor in identifiers (`table_name`, future `table_id`).

### Tests

- Test files end with `_*_test.go` for external tests (`example_test.go` pattern
  when exporting examples).
- Test functions begin with `Test`.
- Prefer table-driven tests with subtests; name table fields to separate input
  from expected (`give`/`want` style).

### Static analysis

- Run `go vet` on touched packages; follow project CI for additional linters.

---

## C++

High-level layout and naming follow the public
[Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html)
unless this file or local code sets a narrower team rule.

### Constants and macros

- Prefer `const` / `constexpr` and typed enums instead of preprocessor macros:
  macros lack types, scopes, and good diagnostics.
- Group related integers as enums; let the compiler police valid sets of values.
- Assign explicit values only when a stable numeric mapping is required;
  otherwise prefer implicit progression to reduce churn.
- Avoid duplicate enumerator values unless an existing enumerator is reused on
  purpose.
- Separate constants that carry different meanings even if numeric values match.
- Apply `const` to parameters (especially references), methods that do not
  mutate state (`const` member functions), and data members frozen after
  construction.

### Initialization and object lifetime

- Do not initialize non-POD class objects with `memcpy`/`memset` on `this`; it
  breaks invariants such as vtables.
- Declare variables close to first use and initialize immediately.
- Keep constructors trivial when possible; use an `Init()`-style helper when you
  need error returns, exceptions from member setup, long-running work, reliance
  on incomplete `this`, or calls that must behave virtually.
- Initializer lists must follow **declaration order** of members—list order alone
  is misleading when dependencies exist.
- Be deliberate about initialization order among translation units; prefer
  singleton factories, confined globals, `main`, or OS primitives like
  `pthread_once` rather than brittle cross-file static dependencies.

### Type conversions and polymorphism

- Prefer C++-style casts; pick the narrowest (`static_cast`, `const_cast`,
  `reinterpret_cast`, `dynamic_cast`).
- Restrict `reinterpret_cast` and document why when unavoidable.
- Avoid `const_cast` to mutate through `const` unless semantics are airtight.
- Avoid type-switching hierarchies duplicated everywhere; polymorphism beats
  scattershot `dynamic_cast` for routine behavior dispatch.

### Functions

- `inline` tiny helpers (roughly 1–10 lines) without complex control flow;
  virtuals, recursion, and large bodies are not inline candidates.
- Prefer inline functions over function-like macros except for well-established
  patterns (for example some `new`/`delete` helpers).
- Declare non-trivial `inline` bodies in separate `*.inl` headers included from
  the primary header.
- Prefer `const&` parameters over pointers when null is not part of the API;
  remove or comment unused parameter names in overrides.
- Use default arguments sparingly—they complicate refactors and API clarity.
- Minimize function pointers at module boundaries; prefer interfaces,
  templates, or small strategy objects when maintainability matters.

### Classes and OOP

- Prefer small, single-purpose classes; suspect classes with very many data
  members.
- Encapsulate aggressively: avoid returning non-const pointers or references to
  internal state; keep data members `private` (structs may differ by local
  policy).
- Expose narrow, orthogonal public APIs; keep interface classes free of
  protected/private leakage that forces consumers to recompile on internal
  changes.
- Use PIMPL when you need stable ABI or faster rebuilds.
- Provide or delete copy/move constructors, assignment operators, and destructors
  when RAII resources are involved; guard self-assignment in `operator=` and
  return `*this`.
- Make single-argument constructors `explicit` unless deliberate implicit
  conversion is required (rare).
- Use `virtual` destructor on polymorphic bases; avoid virtual calls inside
  constructors/destructors unless the design mandates well-defined bases-only
  behavior.
- Prefer composition over deep implementation inheritance; keep inheritance
  depth small; minimize multiple inheritance of implementation (multiple pure
  interfaces are comparatively safer).
- Public inheritance expresses “is-a”; private inheritance is rare—prefer
  composition unless you must expose protected symbols.
- Do not introduce default arguments on overrides of virtual functions; defaults
  are statically bound and confuse readers.
- Do not hide overload intent by differing parameter lists across the hierarchy.

### Operators and overloads

- Overload operators only when the meaning mirrors built-in expectations.
- Prefer distinct function names (`AppendInt`, `AppendName`) instead of piles
  of subtly different overloads.
- Prefer overload sets that defeat accidental implicit conversions where that
  matters.
- Avoid overloads at C linkage boundaries consumed from C callers.

### Namespaces and scope

- Use namespaces to group symbols; lowercase names anchored to component paths
  reduces clashes.
- Do not blanket `using namespace` before includes in headers.
- Minimize nested classes unless they clarify implementation details tightly
  coupled to their owner.
- Avoid large local classes; prefer namespace-level helpers or lambdas suited to
  the toolchain.
- Replace global free functions with namespace-scoped helpers; anonymous
  namespace or `static` file-local helpers are acceptable in `.cpp` files.

### Globals and statics

- Non-POD globals/statics invite order bugs; constrain static lifetimes or make
  them POD with explicit bootstrap.

### Templates

- Use templates deliberately; complexity explodes compilation time and diagnostics.
- Prefer passing complicated template parameters by `const` reference or pointer.
- Document constraints on template parameters near the declaration.
- Avoid exporting unstable template-heavy interfaces across shared libraries if
  your platform cannot guarantee coherent ABIs/instantiations.

### Linkage with C

- Wrap C APIs in `extern "C"` with only declarations/functions/types—do not bury
  arbitrary includes inside the linkage guard in ways that recurse oddly.

### Friends and RTTI

- Avoid `friend` unless it is narrower than widening everything to `public`.
- Prefer virtual dispatch or visitor-style patterns instead of pervasive RTTI
  toggles unless the platform mandates otherwise.

### `sizeof`

- Prefer `sizeof variable` over `sizeof(type)` so refactors remain accurate.

### Resource management

- Match allocation style (`new` vs `new[]`) to the correct deletion operator.
- Who allocates should usually free unless a documented ownership transfer applies.
- Prefer RAII wrappers and smart pointers; document cross-module ownership.
- After unconditional `delete`, set raw pointers to `nullptr` where local policy
  uses that guard.
- Free nested allocations from the inside out; free each element before freeing
  array storage when managing manual pointer arrays.

### Threads and concurrency

- Guard shared mutable state consistently; minimize critical sections.
- Keep lock ordering deterministic when multiple mutexes coexist; avoid
  reentrant locking patterns that deadlock.
- Do not invoke unknown code while holding locks when it might re-enter the same
  locks.
- Avoid `return`/branching out of a critical section without releasing locks;
  encapsulate guards (RAII lock types).
- Use message queues, sockets, transactional stores, or shared memory with explicit
  mutual exclusion rather than casually sharing files unless the platform guarantees
  safe coordination.
- Write reentrant-friendly helpers relying on locals/parameters instead of hidden
  globals wherever possible.

### Exceptions (where enabled)

- Use exceptions sparingly and only with clear ownership of failure paths,
  preferring deterministic cleanup (RAII) when exception safety is promised.
- Do not throw from constructors/destructors for types that participate in fragile
  invariants unless the module standard explicitly allows it.
- Throw by value, catch by `const` reference; never let exceptions traverse
  module boundaries lacking a binary contract for them.

### Error handling posture

- Know each module’s error contract (exceptions, error codes). Translate at the
  boundary instead of leaking mixed models.
- Keep basic/strong/no-throw guarantees in mind around transactional sequences.

### Standard library habits

- Do not persist pointers from `std::string::c_str()`; call `c_str()` at the site
  of use.
- Shared ownership: prefer `make_shared`; once an object adopts `shared_ptr`,
  stick to shared ownership semantics—do not casually alias raw owning pointers.
- Understand `weak_ptr` for cycles.
- Prefer `std::unique_ptr`, `scoped_ptr`, or equivalents over `auto_ptr`; avoid
  smart pointers wrapping arrays—use vectors or dedicated container abstractions.

### Containers and iterators

- Be aware iterator invalidation rules and algorithmic complexities.
- Prefer `empty()` checks over questionable `size()` idioms tied to concrete
  library implementations where history shows surprises.

### Performance

- Measure before rewriting hot paths.
- Prefer appropriate containers and algorithms (`vector` locality vs list node
  overhead, map complexity vs hash containers, etc.).
- Construction prefer member initializer lists over default-construct-plus-assign for
  members that support it.
- Watch hidden costs: unnecessary temporaries (`operator+` loops), postfix
  increment on iterators, synchronous iostream contention, etc.

### Naming and commentary (KWDB conventions)

- Class names start upper camel case; functions upper camel case; data members:
  lowercase with underscores plus trailing underscore (`member_one_`).
- Class declaration order suggestion: access sections `public`, `protected`,
  `private`; within each, typedefs/enums/constants, constructors, destructor,
  methods including static helpers, then data including static fields.
- Use `//` comments with a space after the slashes; reserve block comments where
  local style requires boilerplate banners.
- TODO comments in shipped code must carry owner/date context per team policy.

### Portability

- Do not rely on widths of primitive types; wrap them with `<cstdint>` aliases
  or project typedefs tuned per platform matrix.
- Watch pointer ↔ integer casts; prefer `intptr_t`/`uintptr_t` when you must move
  addresses through integers.
- Handle alignment and endian issues for on-disk structs shared across CPUs.
- Normalize network payloads with explicit endian conversions even when localhost
  happens to match.
- Minimize casts between signed and unsigned widths—when conversions are vital,
  make intent explicit and safe.
- Mark 64-bit literals with `LL`/`ULL`; remember `sizeof(void*)` may diverge from
  `sizeof(int)` on LP64 hosts.
- Quarantine `#pragma`, `attribute`, compiler intrinsics behind macros guarded
  by detected toolchains.

### Internationalization

- Decide explicitly between UTF-8 `char` + `std::string` versus wide APIs; size
  buffers for maximal UTF-8 expansion when using byte arrays.

### Measurement and portability references

Consult public ISO technical reports such as ISO/IEC TR 18015 (C++ performance
report) alongside project profiling when arguing about micro-optimizations—do
not treat language lore as a substitute for data.

---

## Document maintenance

- Update this guide through reviewed pull requests; note the rationale in commit
  or PR summaries when semantics change materially.
- If automated formatters disagree with prose here, converge tooling and wording
  together rather than silently drifting.
