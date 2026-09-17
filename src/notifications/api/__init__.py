"""No HTTP surface — notifications is a worker-only module.

The package exists to keep the uniform ``api/application/domain`` module shape
(and the import-linter layers contract); nothing routes from here.
"""
