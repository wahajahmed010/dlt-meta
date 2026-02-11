"""Tests for the dlt-meta backward compatibility layer.

These tests verify that the old dlt_meta package (compat/) properly
re-exports symbols from databricks.labs.sdpmeta with deprecation warnings.
"""
import os
import sys
import unittest
import warnings
from unittest.mock import MagicMock

# Mock the dlt module before importing runtime modules
sys.modules['dlt'] = MagicMock()

# Ensure the compat directory is on the Python path so `import dlt_meta` works
_compat_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "compat")
if _compat_dir not in sys.path:
    sys.path.insert(0, _compat_dir)


class TestCompatDeprecationWarning(unittest.TestCase):
    """Test that importing dlt_meta emits a DeprecationWarning."""

    def test_import_dlt_meta_emits_deprecation_warning(self):
        """Importing dlt_meta should emit a DeprecationWarning."""
        # Remove from cache to trigger fresh import
        modules_to_remove = [k for k in sys.modules if k.startswith('dlt_meta')]
        for mod in modules_to_remove:
            del sys.modules[mod]

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            import dlt_meta  # noqa: F401
            deprecation_warnings = [
                x for x in w if issubclass(x.category, DeprecationWarning)
            ]
            self.assertTrue(
                len(deprecation_warnings) > 0,
                "Importing dlt_meta should emit at least one DeprecationWarning"
            )


class TestCompatReExports(unittest.TestCase):
    """Test that old dlt_meta imports correctly re-export from sdpmeta."""

    def test_dltmeta_alias_is_sdpmeta(self):
        """DLTMeta class should be an alias for SDPMeta."""
        from dlt_meta import DLTMeta
        from databricks.labs.sdpmeta.cli import SDPMeta
        self.assertIs(DLTMeta, SDPMeta)

    def test_dataflow_pipeline_reexport(self):
        """DataflowPipeline should be re-exported from dlt_meta."""
        from dlt_meta import DataflowPipeline
        from databricks.labs.sdpmeta.dataflow_pipeline import DataflowPipeline as Original
        self.assertIs(DataflowPipeline, Original)

    def test_dataflow_spec_reexport(self):
        """BronzeDataflowSpec and SilverDataflowSpec should be re-exported."""
        from dlt_meta import BronzeDataflowSpec, SilverDataflowSpec
        from databricks.labs.sdpmeta.dataflow_spec import (
            BronzeDataflowSpec as OrigBronze,
            SilverDataflowSpec as OrigSilver,
        )
        self.assertIs(BronzeDataflowSpec, OrigBronze)
        self.assertIs(SilverDataflowSpec, OrigSilver)

    def test_onboard_command_reexport(self):
        """OnboardCommand should be re-exported from dlt_meta."""
        from dlt_meta import OnboardCommand
        from databricks.labs.sdpmeta.cli import OnboardCommand as Original
        self.assertIs(OnboardCommand, Original)

    def test_deploy_command_reexport(self):
        """DeployCommand should be re-exported from dlt_meta."""
        from dlt_meta import DeployCommand
        from databricks.labs.sdpmeta.cli import DeployCommand as Original
        self.assertIs(DeployCommand, Original)

    def test_runner_notebook_alias(self):
        """DLT_META_RUNNER_NOTEBOOK should alias SDP_META_RUNNER_NOTEBOOK."""
        from dlt_meta import DLT_META_RUNNER_NOTEBOOK
        from databricks.labs.sdpmeta.cli import SDP_META_RUNNER_NOTEBOOK
        self.assertEqual(DLT_META_RUNNER_NOTEBOOK, SDP_META_RUNNER_NOTEBOOK)

    def test_pipeline_readers_reexport(self):
        """PipelineReaders should be re-exported from dlt_meta."""
        from dlt_meta import PipelineReaders
        from databricks.labs.sdpmeta.pipeline_readers import PipelineReaders as Original
        self.assertIs(PipelineReaders, Original)

    def test_pipeline_writers_reexport(self):
        """AppendFlowWriter and DLTSinkWriter should be re-exported from dlt_meta."""
        from dlt_meta import AppendFlowWriter, DLTSinkWriter
        from databricks.labs.sdpmeta.pipeline_writers import (
            AppendFlowWriter as OrigAppend,
            DLTSinkWriter as OrigSink,
        )
        self.assertIs(AppendFlowWriter, OrigAppend)
        self.assertIs(DLTSinkWriter, OrigSink)


if __name__ == '__main__':
    unittest.main()
