"""Entry point for dlt_meta package (compatibility wrapper).

This redirects to databricks.labs.sdpmeta.__main__ with a deprecation warning.
"""
import warnings

warnings.warn(
    "Running 'python -m dlt_meta' is deprecated. "
    "Please use 'python -m databricks.labs.sdpmeta' or migrate to databricks-labs-sdpmeta package.",
    DeprecationWarning,
    stacklevel=2
)

# Import and run the main module from databricks.labs.sdpmeta
from databricks.labs.sdpmeta.__main__ import main

if __name__ == "__main__":
    main()
