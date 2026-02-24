# Metaflow plugin registration.
# Metaflow discovers these descriptors at import time via the
# metaflow_extensions namespace package convention.
#
# Layer: Plugin Registration (top-level entry point)
# May only import from: .sandbox_decorator, .sandbox_cli

STEP_DECORATORS_DESC = [
    ("sandbox", ".sandbox_decorator.SandboxDecorator"),
    ("daytona", ".sandbox_decorator.DaytonaDecorator"),
    ("e2b", ".sandbox_decorator.E2BDecorator"),
]

CLIS_DESC = [
    ("sandbox", ".sandbox_cli.cli"),
]
