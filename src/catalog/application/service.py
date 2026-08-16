"""Catalog use-cases (read + write).

Maps repo rows through the domain to Pydantic response schemas so ORM/``ProductRow``
never crosses the service boundary. Missing single row -> ``None`` (the route maps
``None`` -> 404; services never raise HTTP).

Writes own two invariants:
- **Ownership**: a merchant may only mutate their own product;
  a cross-merchant mutation raises ``AuthorizationError`` (->403). ``admin`` bypasses
  ownership (never the role gate - that stays in the route).
- **Product events**: every create/update/delete builds the matching versioned
  event and hands it to the repo, which persists it to the transactional outbox in
  the same transaction as the state change (the relay ships it to the bus).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from dataclasses import dataclass

from pydantic import ValidationError

from src.catalog.application.dto import ProductCreate, ProductResponse, ProductUpdate, public_image_url
from src.catalog.application.image_processing import ALLOWED_MIME
from src.catalog.application.mappers import to_domain
from src.catalog.application.outbox import product_updated_outbox
from src.catalog.ports.cache import MISS, ProductCachePort
from src.catalog.ports.repository import CatalogRepositoryPort
from src.catalog.ports.storage import ImageStorePort
from src.events.models import ProductCreated, ProductDeleted, ProductDeletedData, ProductWriteData
from src.shared.config.logging import current_trace_id
from src.shared.config.setting import AppSettings
from src.shared.db.outbox import OutboxMessage
from src.shared.db.pagination import PageParams, PageResponse
from src.shared.errors.exceptions import AuthorizationError, InvalidUploadError

log = logging.getLogger(__name__)


class _RepositoryFailure(Exception):
    """Internal marker: a repository call failed (never a cache fault).

    The cache-aside read swallows cache errors to degrade to the DB. Repository
    errors must NOT be swallowed by that boundary — they are real 5xx and
    re-reading would just fail again. Wrapping them here keeps the distinction
    port-neutral (no ``SQLAlchemyError`` import in the application layer); the
    public entrypoint unwraps and re-raises the original.
    """


@dataclass(frozen=True, slots=True)
class ImageUploadTicket:
    """Application-layer result of a presign use-case (the route maps it to the wire schema).

    Kept free of the API layer so the service never imports ``api.schemas`` for it.
    """

    url: str
    fields: dict[str, str]
    key: str
    expires_in: int


class CatalogService:
    """Read- and write-side use-cases over the product catalog."""

    # Single-filler spin-wait: a cache-miss loser waits for the lock holder's fill
    # instead of hitting the DB. It waits *while the lock is actively held* (the
    # holder renews it during a slow read), so there is no arbitrary time ceiling
    # that could let a waiter race the DB during a legitimately slow fill. The wait
    # is still bounded: a crashed holder stops renewing, the lock lapses, and one
    # waiter promotes itself to filler.
    _FILL_WAIT_SECONDS = 0.05

    def __init__(
        self,
        repo: CatalogRepositoryPort,
        image_store: ImageStorePort | None = None,
        cache: ProductCachePort | None = None,
        *,
        lock_ttl_seconds: int | None = None,
        image_base_url: str | None = None,
        image_max_upload_bytes: int | None = None,
        image_upload_ttl_seconds: int | None = None,
    ) -> None:
        self._repo = repo
        self._image_store = image_store
        self._cache = cache
        # Catalog's slice of the config, injected by the container from
        # ``app.state.settings`` — never re-read from the global ``get_settings()``,
        # so ``create_app(custom_settings)`` can't end up enforcing one upload limit
        # / CDN base at the edge while the service uses another.
        self._image_base_url = image_base_url
        self._image_max_upload_bytes = (
            image_max_upload_bytes or AppSettings.model_fields["image_max_upload_bytes"].default
        )
        self._image_upload_ttl_seconds = (
            image_upload_ttl_seconds or AppSettings.model_fields["image_upload_ttl_seconds"].default
        )
        # Renew the fill lock well inside its TTL so a slow DB read never lets it
        # lapse (which would let a second filler start). Half the lock TTL, floored
        # so a tiny TTL still yields a sane cadence. The TTL is taken from the SAME
        # injected settings the cache adapter uses (passed by the container), not a
        # second read of the global settings, so the renew cadence can't drift from
        # the actual lock TTL under an injected/overridden config.
        if lock_ttl_seconds is None:
            # The field's declared default, not ``get_settings()``: every real call
            # site injects the configured TTL, so building a service must not require
            # a fully-configured environment (and can't drift from the config default).
            lock_ttl_seconds = AppSettings.model_fields["product_cache_lock_ttl_seconds"].default
        self._lock_renew_seconds = max(0.5, lock_ttl_seconds / 2)

    def _to_response(self, product: object) -> ProductResponse:
        """Map a domain product (or decoded cache entry) to the response schema.

        The single place ``image_url`` is resolved, from the injected public base —
        so a cached entry serialized under an older config is re-resolved on read.
        """
        response = product if isinstance(product, ProductResponse) else ProductResponse.model_validate(product)
        response.image_url = public_image_url(response.image_key, response.image_status, self._image_base_url)
        return response

    async def get_product(self, product_id: uuid.UUID) -> ProductResponse | None:
        """Resolve a product by id, or ``None`` if absent (route maps to 404).

        Cache-aside: serve from Valkey on a hit; on a miss, a single caller fills
        the cache under a token-owned ``SET NX`` lock while concurrent callers wait
        for that fill (see :meth:`_read_through`) — so a hot key never stampedes the
        DB. Invalidation is event-driven (``ProductUpdated``/``ProductDeleted`` →
        ``catalog-cache`` consumer), not written here.

        The cache is **best-effort**: any Valkey fault degrades to a direct DB read
        rather than surfacing a 5xx (the graceful-degradation contract in
        ``docs/RUNBOOK.md``). A *repository* failure is not a cache fault — it is
        wrapped as :class:`_RepositoryFailure` at the one place the repo is called
        and unwrapped here, so it surfaces as the real error instead of being
        mislabelled and re-queried.
        """
        try:
            return await self._get_product_cached(product_id)
        except _RepositoryFailure as wrapper:
            raise wrapper.__cause__ from None  # type: ignore[misc]

    async def _get_product_cached(self, product_id: uuid.UUID) -> ProductResponse | None:
        """Cache-aside read; cache faults degrade to the DB, repo faults propagate."""
        if self._cache is None:
            return await self._load_product(product_id)

        try:
            cached = await self._cache.get(product_id)
            if cached is not None:
                hit, value = await self._decode_or_evict(product_id, cached)
                if hit:
                    return value  # a real hit, or a cached 404 (negative hit) → answer is None
            return await self._read_through(product_id)
        except _RepositoryFailure:
            raise
        except Exception:  # cache boundary: Valkey down / bad reply → serve from DB
            log.warning("product read-cache unavailable; serving %s from DB", product_id, exc_info=True)
            return await self._load_product(product_id)

    async def _decode_or_evict(self, product_id: uuid.UUID, cached: str) -> tuple[bool, ProductResponse | None]:
        """Decode one cache hit. Returns ``(usable, value)``.

        ``(True, response)`` for a real hit; ``(True, None)`` for the negative-cache
        :data:`MISS` tombstone (a cached 404 — the answer *is* ``None``). A corrupt
        entry (bad JSON that no longer validates, e.g. after a schema change) is
        **evicted** and reported ``(False, None)`` so the caller refills from the DB
        instead of silently degrading on every read while the poison entry lingers.
        """
        assert self._cache is not None
        if cached == MISS:
            return True, None
        try:
            return True, self._to_response(ProductResponse.model_validate_json(cached))
        except ValidationError:
            log.warning("evicting corrupt product-cache entry for %s", product_id, exc_info=True)
            await self._cache.evict_value(product_id, cached)  # value only, and only this exact poison payload
            return False, None

    async def _load_product(self, product_id: uuid.UUID) -> ProductResponse | None:
        """Fetch one product straight from the repository (no cache).

        Any repository error is wrapped in :class:`_RepositoryFailure` so the
        cache-fault boundary in :meth:`_get_product_cached` can't swallow it.
        """
        try:
            row = await self._repo.get_product(product_id)
        except Exception as exc:
            raise _RepositoryFailure(str(exc)) from exc
        if row is None:
            return None
        return self._to_response(to_domain(row))

    async def _read_through(self, product_id: uuid.UUID) -> ProductResponse | None:
        """Fill the cache on a miss with exactly one DB read across concurrent callers.

        The caller that wins ``acquire_fill_lock`` is the single filler. It
        **re-checks the cache** first (another caller may have filled between our
        miss and the lock — double-checked locking), then fills while **renewing the
        lock** so even a slow DB read never lets a second filler start, and stores
        the value only while it still owns the lock (``store_if_owner``) — an
        invalidation that raced the fill deleted the lock, so the stale read it made
        is dropped. Losers wait while the lock is actively held and serve the fill;
        they promote themselves to filler only once the lock lapses without a value
        (a crashed holder). A confirmed 404 is negative-cached, so losers serve that
        tombstone rather than each re-querying the DB.
        """
        assert self._cache is not None  # guarded by get_product
        token = uuid.uuid4().hex
        if await self._cache.acquire_fill_lock(product_id, token):
            try:
                cached = await self._cache.get(product_id)  # double-check under the lock
                if cached is not None:
                    hit, value = await self._decode_or_evict(product_id, cached)
                    if hit:
                        return value
                return await self._fill_holding_lock(product_id, token)
            finally:
                # A release failure must never mask the body's exception (a DB error
                # or cancellation): log it and move on. ``Exception`` (not
                # ``BaseException``) so a cancellation mid-release still propagates.
                try:
                    await self._cache.release_fill_lock(product_id, token)
                except Exception:
                    log.warning("failed to release product fill lock for %s", product_id, exc_info=True)
        return await self._await_fill(product_id)

    async def _fill_holding_lock(self, product_id: uuid.UUID, token: str) -> ProductResponse | None:
        """Load from the DB while keeping the fill lock alive, then store-if-owner.

        The DB read runs as a task so we can renew the lock every
        ``_lock_renew_seconds`` until it completes — a fill slower than the base
        lock TTL still holds the lock, so no second filler ever starts (the
        anti-stampede guarantee). The store no-ops if an invalidation deleted the
        lock meanwhile, so a value read before that update is never written back.
        A confirmed 404 is **negative-cached** (short-TTL tombstone) so a burst of
        misses on an absent id doesn't re-query the DB one waiter at a time.
        """
        assert self._cache is not None
        load = asyncio.ensure_future(self._load_product(product_id))
        try:
            while True:
                done, _ = await asyncio.wait({load}, timeout=self._lock_renew_seconds)
                if load in done:
                    break
                await self._cache.renew_fill_lock(product_id, token)  # keep-alive during a slow fill
            response = load.result()
        except BaseException:
            load.cancel()
            # Await the cancelled load so its DB session/connection is fully
            # unwound before the caller releases the fill lock — otherwise the task
            # could still be mid-query when the lock is dropped (use-after-release).
            with contextlib.suppress(BaseException):
                await load
            raise
        if response is not None:
            await self._cache.store_if_owner(product_id, response.model_dump_json(), token)
        else:
            await self._cache.store_miss_if_owner(product_id, token)  # negative-cache the 404
        return response

    async def _await_fill(self, product_id: uuid.UUID) -> ProductResponse | None:
        """Wait for the lock holder's fill rather than piling onto the DB.

        Waits *while the lock is actively held* — the holder renews it during a slow
        read, so there is no fixed timeout that could let us race the DB mid-fill.
        Once the lock is gone we serve the freshly-cached value (a real hit or a
        negative-cached 404), or promote to filler if there is none (holder crashed).
        """
        assert self._cache is not None
        while await self._cache.fill_lock_held(product_id):
            cached = await self._cache.get(product_id)
            if cached is not None:
                hit, value = await self._decode_or_evict(product_id, cached)
                if hit:
                    return value
                break  # corrupt entry evicted → stop waiting, promote to filler
            await asyncio.sleep(self._FILL_WAIT_SECONDS)
        cached = await self._cache.get(product_id)
        if cached is not None:
            hit, value = await self._decode_or_evict(product_id, cached)
            if hit:
                return value
        return await self._read_through(product_id)  # lock lapsed without a value → promote

    async def list_products(
        self, params: PageParams, filters: dict[str, object] | None = None
    ) -> PageResponse[ProductResponse]:
        """Return a keyset page of products, optionally filtered."""
        page = await self._repo.list_products(params, filters)
        items = [self._to_response(to_domain(row)) for row in page.items]
        return PageResponse(items=items, next_cursor=page.next_cursor)

    async def create_product(self, *, merchant_id: uuid.UUID, data: ProductCreate) -> ProductResponse:
        """Create a product owned by ``merchant_id`` and emit ``ProductCreated``."""
        product_id = uuid.uuid4()
        event = ProductCreated.new(
            trace_id=current_trace_id(),
            data=ProductWriteData(
                product_id=product_id,
                merchant_id=merchant_id,
                name=data.name,
                price=data.price,
                category=data.category,
            ),
        )
        row = await self._repo.create_product(
            product_id=product_id,
            merchant_id=merchant_id,
            name=data.name,
            description=data.description,
            category=data.category,
            price=data.price,
            image_key=None,  # images are attached later, only after the worker passes them
            outbox=OutboxMessage(event.type, event.model_dump_json()),
        )
        return self._to_response(to_domain(row))

    async def update_product(
        self, *, product_id: uuid.UUID, merchant_id: uuid.UUID, is_admin: bool, patch: ProductUpdate
    ) -> ProductResponse | None:
        """Update an owned product and emit ``ProductUpdated``; ``None`` if absent."""
        product = await self._repo.get_product(product_id)
        if product is None:
            return None
        self._assert_owner(product.merchant_id, merchant_id, is_admin)

        changes = patch.model_dump(exclude_unset=True)
        outbox = product_updated_outbox(
            product_id=product_id,
            merchant_id=product.merchant_id,
            name=changes.get("name", product.name),
            price=changes.get("price", product.price),
            category=changes.get("category", product.category),
        )
        row = await self._repo.update_product(product, changes, outbox=outbox)
        return self._to_response(to_domain(row))

    async def delete_product(self, *, product_id: uuid.UUID, merchant_id: uuid.UUID, is_admin: bool) -> bool:
        """Soft-delete an owned product and emit ``ProductDeleted``; ``False`` if absent."""
        product = await self._repo.get_product(product_id)
        if product is None:
            return False
        self._assert_owner(product.merchant_id, merchant_id, is_admin)

        event = ProductDeleted.new(
            trace_id=current_trace_id(),
            data=ProductDeletedData(product_id=product_id, merchant_id=product.merchant_id),
        )
        await self._repo.soft_delete_product(product, outbox=OutboxMessage(event.type, event.model_dump_json()))
        return True

    async def presign_image_upload(
        self, *, product_id: uuid.UUID, merchant_id: uuid.UUID, is_admin: bool, content_type: str, content_length: int
    ) -> ImageUploadTicket | None:
        """Validate the requested upload, then issue a short-TTL presigned POST.

        Ownership + content-type + size are all checked BEFORE any URL is minted,
        so this is never an open uploader. The presigned POST pins the claimed
        ``content_type`` onto the object (the worker later re-sniffs and rejects a
        mismatch). The product is flipped to ``pending`` and the new upload's token
        recorded, so a late event for a superseded upload can't win. ``None`` if
        the product is absent (route → 404).
        """
        product = await self._repo.get_product(product_id)
        if product is None:
            return None
        self._assert_owner(product.merchant_id, merchant_id, is_admin)

        if content_type not in ALLOWED_MIME:
            raise InvalidUploadError(f"content_type {content_type!r} is not an allowed image type")
        if content_length > self._image_max_upload_bytes:
            raise InvalidUploadError(f"content_length exceeds {self._image_max_upload_bytes} bytes")
        if self._image_store is None:  # pragma: no cover - misconfiguration guard
            raise RuntimeError("image store is not configured")

        presigned = await self._image_store.presign_upload(
            product_id,
            content_type=content_type,
            max_bytes=self._image_max_upload_bytes,
            ttl_seconds=self._image_upload_ttl_seconds,
        )
        # Flipping to ``pending`` drops the public image; emit ProductUpdated in the
        # same txn so a cached ready-image response is invalidated as the re-upload starts.
        outbox = product_updated_outbox(
            product_id=product_id,
            merchant_id=product.merchant_id,
            name=product.name,
            price=product.price,
            category=product.category,
        )
        await self._repo.set_image_pending(product, presigned["token"], outbox=outbox)
        return ImageUploadTicket(
            url=presigned["url"],
            fields=presigned["fields"],
            key=presigned["key"],
            expires_in=self._image_upload_ttl_seconds,
        )

    @staticmethod
    def _assert_owner(owner_id: uuid.UUID, caller_id: uuid.UUID, is_admin: bool) -> None:
        """Merchant may only mutate their own products; ``admin`` bypasses ownership."""
        if owner_id != caller_id and not is_admin:
            raise AuthorizationError("not the owner of this product")
