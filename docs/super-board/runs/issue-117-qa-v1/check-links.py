"""AC1 + AC2 check for docs/super-board/PROJECT.md: line cap and link targets exist."""
import os, re, sys
p = "docs/super-board/PROJECT.md"
t = open(p).read()
lines = len(t.splitlines())
links = [m.group(1) for m in re.finditer(r"\]\(([^)#]+)(?:#[^)]*)?\)", t)]
missing = [l for l in links if not os.path.exists(os.path.normpath(os.path.join(os.path.dirname(p), l)))]
print(f"lines={lines} (cap 150) links={len(links)} missing={len(missing)}")
for m in missing:
    print("MISSING", m)
sys.exit(1 if lines > 150 or missing else 0)
