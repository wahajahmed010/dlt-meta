import copy
import sys
from unittest.mock import MagicMock, patch

# Mock the dlt module before importing modules that depend on it
sys.modules["dlt"] = MagicMock()

from databricks.labs.sdpmeta.dataflow_pipeline import DataflowPipeline  # noqa: E402
from databricks.labs.sdpmeta.dataflow_spec import BronzeDataflowSpec, DataflowSpecUtils  # noqa: E402
from databricks.labs.sdpmeta.onboard_dataflowspec import OnboardDataflowspec  # noqa: E402
from databricks.labs.sdpmeta.pipeline_writers import AppendFlowWriter, DLTSinkWriter  # noqa: E402
from databricks.labs.sdpmeta.dataflow_spec import DLTSink  # noqa: E402
from tests.utils import SDPFrameworkTestCase  # noqa: E402


class TestAppendFlowWriter(SDPFrameworkTestCase):

    @patch('databricks.labs.sdpmeta.pipeline_writers.dlt')
    def test_read_af_view(self, mock_dlt):
        appendflow_writer = AppendFlowWriter(
            self.spark, MagicMock(), "test_target", "test_schema",
            {"property": "value"}, ["col1"], ["col2"]
        )
        appendflow_writer.read_af_view()
        mock_dlt.read_stream.assert_called_once()

    @patch('databricks.labs.sdpmeta.pipeline_writers.dlt')
    def test_write_flow(self, mock_dlt):
        appendflow_writer = AppendFlowWriter(
            self.spark, MagicMock(), "test_target", "test_schema",
            {"property": "value"}, ["col1"], ["col2"]
        )
        appendflow_writer.write_flow()
        mock_dlt.create_streaming_table.assert_called_once()
        mock_dlt.append_flow.assert_called_once()


class TestSDPSinkWriter(SDPFrameworkTestCase):

    @patch('databricks.labs.sdpmeta.pipeline_writers.dlt')
    def test_read_input_view(self, mock_dlt):
        dlt_sink = DLTSink(
            name="test_sink",
            format="kafka",
            options={},
            select_exp=["col1", "col2"],
            where_clause="col1 > 0"
        )
        sink_writer = DLTSinkWriter(dlt_sink, "test_view")
        sink_writer.read_input_view()
        mock_dlt.read_stream.assert_called_once_with("test_view")

    @patch('databricks.labs.sdpmeta.pipeline_writers.dlt')
    def test_write_to_sink(self, mock_dlt):
        dlt_sink = DLTSink(
            name="test_sink",
            format="kafka",
            options={},
            select_exp=["col1", "col2"],
            where_clause="col1 > 0"
        )
        sink_writer = DLTSinkWriter(dlt_sink, "test_view")
        sink_writer.write_to_sink()
        mock_dlt.create_sink.assert_called_once_with(name='test_sink', format='kafka', options={})
        mock_dlt.append_flow.assert_called_once()

    @patch('databricks.labs.sdpmeta.pipeline_writers.dlt')
    @patch('databricks.labs.sdpmeta.dataflow_pipeline.dlt')
    def test_dataflowpipeline_bronze_sink_write(self, mock_dlt_dp, mock_dlt_pw):
        mock_dlt_dp.table = MagicMock(return_value=lambda func: func)
        mock_dlt_pw.append_flow = MagicMock(return_value=lambda func: func)
        mock_dlt_pw.create_sink = MagicMock()
        mock_dlt_pw.read_stream = MagicMock(return_value=None)

        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["onboarding_file_path"] = self.onboarding_sink_json_file
        local_params["bronze_dataflowspec_table"] = "bronze_dataflowspec_sink"
        del local_params["silver_dataflowspec_table"]
        del local_params["silver_dataflowspec_path"]
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_bronze_dataflow_spec()
        bronze_dataflowSpec_df = self.spark.read.table(
            f"{self.onboarding_bronze_silver_params_map['database']}.bronze_dataflowspec_sink")
        bronze_dataflowSpec_df.show(truncate=False)
        self.assertEqual(bronze_dataflowSpec_df.count(), 1)
        bronze_dataflow_spec = DataflowSpecUtils._get_dataflow_spec(
            spark=self.spark,
            dataflow_spec_df=bronze_dataflowSpec_df,
            layer="bronze"
        ).collect()[0]
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, BronzeDataflowSpec(**bronze_dataflow_spec.asDict()), view_name, None)
        pipeline.write()
        # Verify that create_sink was called (may be called multiple times for multiple sinks)
        self.assertGreater(mock_dlt_pw.create_sink.call_count, 0, "create_sink should have been called")
        # Verify all calls have the required parameters
        for call in mock_dlt_pw.create_sink.call_args_list:
            _, kwargs = call
            self.assertIn('name', kwargs)
            self.assertIn('format', kwargs)
            self.assertIn('options', kwargs)
        # Check that append_flow and dlt.table were called
        self.assertGreater(mock_dlt_pw.append_flow.call_count, 0)
        self.assertGreater(mock_dlt_dp.table.call_count, 0)
