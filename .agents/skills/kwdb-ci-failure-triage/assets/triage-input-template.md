# Triage Input

Command: <failing command or target>

Narrowed repro: <smallest known repro command, or `unknown`>

Test/package/script: <test name, package, suite, or script>

Decisive line: <first decisive assertion, panic, compile error, link error, or mismatch>

File:line: <file path and line, or `unknown`>

Failure shape: <assertion | panic | stack | compile error | link error | result mismatch | harness failure | unknown>

Environment clues: <toolchain, path, OOM, disk, permission, shell, arch, or `none seen`>

Nearby passing signals: <peer tests passed, build succeeded, isolated case, or `none seen`>
