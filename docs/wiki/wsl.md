# WSL

### Useful Commands
```bash
# Check WSL version
wsl -l -v

# Access Windows files from WSL
ls /mnt/c/Users/sagar/

# Keep projects on WSL filesystem for performance (not /mnt/c/)
# Good:  ~/chicago-data-pipeline/
# Bad:   /mnt/c/Users/sagar/chicago-data-pipeline/
```

### Why WSL filesystem is faster
Cross-filesystem mounts (`/mnt/c/...`) go through the 9P protocol between WSL and Windows. File-heavy operations (Spark, Parquet I/O, git) are significantly slower. Keep the repo inside `~/` (WSL ext4 filesystem).

### Devin IDE + OMP sync
Devin IDE caches the file tree on open and doesn't watch for external changes. If you edit files via OMP, close and reopen Devin (or the affected file tabs) to see updates.

---

---

**← Previous:** None | **Next:** [uv](uv.md) →
