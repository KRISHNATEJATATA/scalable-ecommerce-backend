"""Notifications — order-confirmation emails driven by the event bus.

A worker-only module (no HTTP surface): the ``notifications`` SQS consumer
drains ``OrderPlaced`` (+ ``UserCreated``) events and sends the confirmation
via a sender port. See ``adapters/notification_worker.py``.
"""
