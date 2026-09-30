"""Composition root — the one package allowed to import every module.

Everything that has to *know* more than one module lives here: the DI container
that wires ports to adapters, the ``service``-role worker entrypoints that
compose several modules (saga recovery, payment reconciler, outbox relay), the
payment-gateway factory, and the outbox-schema list that mirrors which modules
own an ``outbox`` table.

The dependency direction is one-way and enforced by import-linter
(``.importlinter``): ``bootstrap`` may import modules and ``src.shared``;
``src.shared`` (the kernel) imports neither. Modules reach in only via
``api.routes`` -> ``bootstrap.container`` for ``Depends`` providers.
"""
