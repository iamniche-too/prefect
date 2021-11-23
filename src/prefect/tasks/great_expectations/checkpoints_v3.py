from typing import Optional

import great_expectations as ge
from great_expectations.render.renderer import ValidationResultsPageRenderer

from prefect import Task
from prefect.artifacts import create_markdown
from prefect.engine import signals
from prefect.utilities.tasks import defaults_from_attrs


class GreatExpectationsValidationTaskV3(Task):
    """
    Great Expectations: v3 API

    V3 documentation can be found at https://docs.greatexpectations.io/docs/

    """
    def __init__(self,
                 checkpoint_name: str = None,
                 context_root_dir: str = None,
                 runtime_environment: Optional[dict] = None,
                 **kwargs):
        self.checkpoint_name = checkpoint_name
        self.context_root_dir = context_root_dir
        self.runtime_environment = runtime_environment or dict()
        super().__init__(**kwargs)

    @defaults_from_attrs(
        "checkpoint_name",
        "context_root_dir",
        "runtime_environment",
    )
    def run(self,
            checkpoint_name: str = None,
            context_root_dir: str = None,
            runtime_environment: Optional[dict] = None,
            ):
        context = ge.DataContext(
            context_root_dir=context_root_dir,
            runtime_environment=runtime_environment,
        )

        # Run Checkpoint (not Validation Operator, as in V2)
        checkpoint_result = context.run_checkpoint(checkpoint_name)

        validation_results_page_renderer = ValidationResultsPageRenderer()

        # render each ValidationResult
        rendered_document_content_list = []
        for validation_result in checkpoint_result.list_validation_results():
            rendered_document_content_list.append(validation_results_page_renderer.render(validation_result))

        markdown_artifact = " ".join(
            ge.render.view.DefaultMarkdownPageView().render(
                rendered_document_content_list
            )
        )

        create_markdown(markdown_artifact)

        if checkpoint_result.success is False:
            raise signals.FAIL(result=checkpoint_result)

        return checkpoint_result
