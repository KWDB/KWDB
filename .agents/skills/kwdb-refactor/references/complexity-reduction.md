# Complexity Reduction

Reduce the cognitive complexity of a specific method by extracting logic into focused helper methods.

Use this when:
- A function is too long (>50 lines for Go, >80 lines for C++)
- Nesting depth exceeds 3 levels
- A function has multiple distinct responsibilities
- Cognitive complexity metrics are above acceptable thresholds

## Analysis

Identify sources of cognitive complexity:
- Nested conditional statements (if/else inside loops inside if)
- Multiple if-else or switch chains
- Repeated code blocks
- Multiple loops with conditions
- Complex boolean expressions
- Deeply nested error handling

## Extraction Strategy

### 1. Identify Extraction Opportunities
- **Validation logic**: Extract into separate `Validate*` functions
- **Type-specific/case-specific processing**: Extract into handler functions
- **Complex transformations or calculations**: Extract into well-named helpers
- **Common patterns**: Extract repeated code into shared utility functions

### 2. Extract Focused Helper Functions
- Each helper should have a single, clear responsibility
- Use descriptive names that explain *what* the helper does
- Extract validation into separate `validate*` functions
- Extract type-specific logic into handler functions
- Create utility functions for common operations

### 3. Simplify the Main Function
- Reduce nesting depth with early returns / guard clauses
- Replace massive if-else chains with smaller orchestrated calls
- Use switch/type switch where appropriate for cleaner dispatch
- Ensure the main function reads as a high-level flow

### Example: Before

```go
func (s *Store) processRows(ctx context.Context, rows []Row, opts Options) error {
    for i, row := range rows {
        if row.IsValid() {
            if opts.Deduplicate {
                exists, err := s.exists(ctx, row.Key)
                if err != nil {
                    return fmt.Errorf("checking existence: %w", err)
                }
                if exists {
                    continue
                }
            }
            if opts.ValidateSchema {
                if err := s.validateSchema(ctx, row.Schema); err != nil {
                    s.logger.Warnf("schema validation failed for row %d: %v", i, err)
                    if opts.StrictMode {
                        return fmt.Errorf("schema validation failed: %w", err)
                    }
                    continue
                }
            }
            if err := s.insert(ctx, row); err != nil {
                return fmt.Errorf("inserting row %d: %w", i, err)
            }
        }
    }
    return nil
}
```

### Example: After

```go
func (s *Store) processRows(ctx context.Context, rows []Row, opts Options) error {
    for i, row := range rows {
        if !row.IsValid() {
            continue
        }
        if err := s.handleDeduplication(ctx, row, opts); err != nil {
            return err
        }
        if err := s.handleSchemaValidation(ctx, row, i, opts); err != nil {
            return err
        }
        if err := s.insert(ctx, row); err != nil {
            return fmt.Errorf("inserting row %d: %w", i, err)
        }
    }
    return nil
}

func (s *Store) handleDeduplication(ctx context.Context, row Row, opts Options) error {
    if !opts.Deduplicate {
        return nil
    }
    exists, err := s.exists(ctx, row.Key)
    if err != nil {
        return fmt.Errorf("checking existence: %w", err)
    }
    if exists {
        return errSkipRow // sentinel to skip without error
    }
    return nil
}

func (s *Store) handleSchemaValidation(ctx context.Context, row Row, idx int, opts Options) error {
    if !opts.ValidateSchema {
        return nil
    }
    if err := s.validateSchema(ctx, row.Schema); err != nil {
        s.logger.Warnf("schema validation failed for row %d: %v", idx, err)
        if opts.StrictMode {
            return fmt.Errorf("schema validation failed: %w", err)
        }
        return errSkipRow
    }
    return nil
}
```

## Best Practices

- Make helper functions unexported (lowercase) when they're internal to a package
- Use guard clauses and early returns to reduce nesting
- Avoid creating unnecessary local variables
- Use meaningful names that describe the extracted responsibility
- Group related helper functions together in the source file
- Keep extracted functions close to where they're used
- Consider making repeated code patterns into generic utility functions

## Result Expectations

The refactored method should:
- Have significantly reduced cognitive complexity
- Be more readable and maintainable
- Have clear separation of concerns
- Be easier to test and debug
- Retain all original functionality
