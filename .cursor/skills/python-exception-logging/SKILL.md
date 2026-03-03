---
name: python-exception-logging
description: When writing or reviewing Python try/except blocks, always log or print the exception so failures are debuggable. Use when writing retry loops, error handling, or any code that catches Exception.
---

# Always print or log the exception

When catching exceptions in Python, **do not swallow the error**. Always include the exception in the log or print output so that job logs and debugging show why the operation failed.

## Do

```python
try:
    spark.sql(f"SELECT 1 FROM `{table_name}` LIMIT 0").collect()
    exists = True
    break
except Exception as e:
    elapsed = int(time.time() - start)
    print(f"  {datetime.now().isoformat()} Waiting for table {table_name} ({elapsed}s)... Last error: {e}")
    time.sleep(poll_sec)
```

Or with traceback when you need full context:

```python
except Exception as e:
    import traceback
    traceback.print_exc()
    print(f"Last error: {e}")
```

## Don't

```python
except Exception:
    print("Waiting...")
    time.sleep(poll_sec)
```

Swallowing the exception (catching without capturing or printing it) makes failures impossible to diagnose from logs.

## When applying

- In retry/wait loops (e.g. polling for table or pipeline state).
- In any `except Exception` or broad `except` block where the error message is useful for debugging.
- When reviewing or refactoring existing try/except code, add exception printing if it is missing.
