"""
Example plugin demonstrating CREATE_CLAIMX_TASK action usage.

This is a reference implementation showing both ClaimX and Verisk domain usage.
"""

from pipeline.plugins.shared.base import (
    ActionType,
    Domain,
    PipelineStage,
    Plugin,
    PluginAction,
    PluginContext,
    PluginResult,
)


class ExampleClaimXTaskPlugin(Plugin):
    """
    Example plugin for ClaimX domain - creates task using project_id.
    """

    name = "example_claimx_task_plugin"
    description = "Example: Create ClaimX task from ClaimX event"
    domains = [Domain.CLAIMX]
    stages = [PipelineStage.ENRICHMENT_COMPLETE]

    async def execute(self, context: PluginContext) -> PluginResult:
        """Create a ClaimX task when enrichment is complete."""

        # Skip if no project_id
        if not context.project_id:
            return PluginResult.skip("No project ID available")

        # Build task data based on event
        task_data = {
            "customTaskName": "Review Enriched Data",
            "customTaskId": 456,  # This would come from config
            "notificationType": "COPY_EXTERNAL_LINK_URL",
        }

        # Create the task using project_id (directly available in ClaimX domain)
        return PluginResult.create_claimx_task(
            project_id=int(context.project_id),
            task_type="CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
            task_data=task_data,
            use_primary_contact_as_sender=True,
        )


class ExampleXACTTaskPlugin(Plugin):
    """
    Example plugin for Verisk domain - creates ClaimX task using claim_number.
    """

    name = "example_xact_task_plugin"
    description = "Example: Create ClaimX task from XACT event"
    domains = [Domain.VERISK]
    stages = [PipelineStage.UPLOAD_COMPLETE]

    async def execute(self, context: PluginContext) -> PluginResult:
        """Create a ClaimX task when XACT upload is complete."""

        # Extract claim number from XACT message
        claim_number = getattr(context.message, "claim_number", None)

        if not claim_number:
            return PluginResult.skip("No claim number available")

        # Build task data
        task_data = {
            "customTaskName": "Review XACT Estimate",
            "customTaskId": 789,  # This would come from config
            "notificationType": "COPY_EXTERNAL_LINK_URL",
        }

        # Create the task using claim_number (automatically resolves to project_id)
        return PluginResult.create_claimx_task(
            claim_number=claim_number,
            task_type="CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
            task_data=task_data,
            sender_username="XACT System",
        )


class ExampleConditionalTaskPlugin(Plugin):
    """
    Example showing conditional task creation with multiple actions.
    """

    name = "example_conditional_task_plugin"
    description = "Example: Conditional task creation with logging"
    domains = [Domain.CLAIMX, Domain.VERISK]

    async def execute(self, context: PluginContext) -> PluginResult:
        """Create task only under certain conditions."""

        # Example condition: only create task if specific event type
        if context.event_type not in ["PROJECT_FILE_ADDED", "ESTIMATE_APPROVED"]:
            return PluginResult.skip(f"Event type {context.event_type} not applicable")

        # Determine project identifier based on domain
        if context.domain == Domain.CLAIMX:
            if not context.project_id:
                return PluginResult.skip("No project ID in ClaimX context")

            # Multiple actions: log + create task
            return PluginResult(
                success=True,
                actions=[
                    # Log the action
                    PluginAction(
                        action_type=ActionType.LOG,
                        params={
                            "level": "info",
                            "message": f"Creating task for ClaimX project {context.project_id}",
                        },
                    ),
                    # Create the task
                    PluginAction(
                        action_type=ActionType.CREATE_CLAIMX_TASK,
                        params={
                            "project_id": int(context.project_id),
                            "task_type": "CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
                            "task_data": {
                                "customTaskName": "Conditional Task",
                                "customTaskId": 999,
                                "notificationType": "COPY_EXTERNAL_LINK_URL",
                            },
                        },
                    ),
                ],
            )

        else:  # Verisk domain
            claim_number = getattr(context.message, "claim_number", None)
            if not claim_number:
                return PluginResult.skip("No claim number in XACT context")

            return PluginResult.create_claimx_task(
                claim_number=claim_number,
                task_type="CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
                task_data={
                    "customTaskName": "XACT Conditional Task",
                    "customTaskId": 999,
                    "notificationType": "COPY_EXTERNAL_LINK_URL",
                },
            )


# Configuration example for these plugins
EXAMPLE_CONFIG = {
    "plugins": [
        {
            "name": "example_claimx_task_plugin",
            "module": "pipeline.plugins.shared.example_claimx_task_plugin",
            "class": "ExampleClaimXTaskPlugin",
            "enabled": True,
            "priority": 100,
            "config": {
                "task_id": 456,  # Custom task ID to use
                "task_name": "Review Enriched Data",
            },
        },
        {
            "name": "example_xact_task_plugin",
            "module": "pipeline.plugins.shared.example_claimx_task_plugin",
            "class": "ExampleXACTTaskPlugin",
            "enabled": True,
            "priority": 100,
            "config": {
                "task_id": 789,
                "task_name": "Review XACT Estimate",
            },
        },
    ]
}
