"""DLT-META Compatibility Package.

DEPRECATED: This package is a compatibility wrapper for databricks-labs-sdpmeta.
Please migrate to using databricks.labs.sdpmeta directly.

All imports from this package are re-exported from databricks.labs.sdpmeta with deprecation warnings.
"""
import warnings

# Issue deprecation warning on import
warnings.warn(
    "The 'dlt_meta' package is deprecated and will be removed in a future version. "
    "Please migrate to 'databricks.labs.sdpmeta' (pip install databricks-labs-sdpmeta). "
    "See https://databrickslabs.github.io/sdp-meta/ for migration guide.",
    DeprecationWarning,
    stacklevel=2
)

# Re-export everything from databricks.labs.sdpmeta
# This maintains full backwards compatibility
try:
    from databricks.labs.sdpmeta import *
    from databricks.labs.sdpmeta.cli import (
        SDPMeta as DLTMeta,  # Alias for backwards compatibility
        OnboardCommand,
        DeployCommand,
        SDP_META_RUNNER_NOTEBOOK as DLT_META_RUNNER_NOTEBOOK,
        onboard,
        deploy,
        main,
    )
    from databricks.labs.sdpmeta.dataflow_pipeline import DataflowPipeline
    from databricks.labs.sdpmeta.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec
    from databricks.labs.sdpmeta.onboard_dataflowspec import OnboardDataflowspec
    from databricks.labs.sdpmeta.pipeline_readers import PipelineReaders
    from databricks.labs.sdpmeta.pipeline_writers import AppendFlowWriter, DLTSinkWriter
    from databricks.labs.sdpmeta.install import WorkspaceInstaller
    from databricks.labs.sdpmeta.config import WorkspaceConfig
except ImportError:
    # If databricks.labs.sdpmeta module not available, this is being run standalone
    pass


def _deprecated_wrapper(func, old_name, new_name):
    """Wrapper that adds deprecation warning to functions."""
    def wrapper(*args, **kwargs):
        warnings.warn(
            f"'{old_name}' is deprecated, use '{new_name}' instead.",
            DeprecationWarning,
            stacklevel=2
        )
        return func(*args, **kwargs)
    return wrapper
