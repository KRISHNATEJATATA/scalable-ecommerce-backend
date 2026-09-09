"""Backend-owned demo seeding — ``make seed`` (dev-only, explicit opt-in).

One-shot bootstrap in the ``bus_bootstrap``/``s3_bootstrap`` mold (``python -m
scripts.catalog_seed``), exposed as a compose one-shot under ``profiles: ["seed"]``
so a normal ``compose up`` never runs it. Replaces the frontend's one-off seed
scripts: 4 Keycloak demo users, 11 products split across two merchants (5 + 6),
each with a real image through the pipeline, and stock declared per product
(9 X 25, one sold-out, one low).

Every mechanism here is first-class — the same ones the app itself uses:

* **Users** — the :class:`KeycloakIdentityAdmin` adapter (service account
  ``ecommerce-admin``) with Ensure-User semantics: exact-username lookup →
  create with password → grant role if absent. The realm export stays untouched.
* **Local identity anchor** — products are owned by the *local*
  ``identity.users.id`` of each merchant, exactly as JIT provisioning anchors
  them on first token (otherwise the merchant console's ``merchant_id`` filter
  finds nothing): :meth:`IdentityRepository.get_or_create`, the same statement
  the app runs.
* **Products** — the catalog application service (never SQL): lookup-by-name
  for idempotency, :meth:`CatalogService.create_product` for missing ones, so
  every seeded product emits a real ``ProductCreated`` event through the outbox.
* **Images** — the presign flow's mint-and-pend (:meth:`CatalogService.presign_image_upload`,
  which flips the product to ``pending`` and records the upload token exactly
  like the HTTP flow), then a **direct S3 PUT** of the JPEG into the same
  ``uploads/`` key a browser would POST to. The ``ObjectCreated`` event →
  image worker → ``ready`` path is unchanged, so the storefront's image states
  stay truthful (only the presign HTTP dance is skipped — auth plumbing, not
  pipeline). Skipped when the product is already ``ready``.
* **Stock** — the inventory application service's :meth:`InventoryService.upsert_stock`
  (the service behind ``PUT /v1/admin/inventory/{sku}``), never SQL.

Ordering: users → anchors → products → images → stock. On ``--reset`` every
live product is soft-deleted through the domain service first (each emits
``ProductDeleted`` so the cache/cart consumers invalidate downstream state),
then the four Keycloak users are deleted — DB-side wipe before user deletion
is the crash-convergence guarantee: an interrupted reset leaves at worst
orphaned Keycloak accounts without local products, which are harmless, and the
next seed re-creates users (fresh subs → fresh anchors) and products keyed off
them. Local identity rows are
never hard-deleted (orders may reference them) and runtime orders are left
untouched — per ticket decision 12, reset tolerates the orphans.

Demo seeding is enabled by default; set ``SEED_DEMO_DATA=0`` to refuse it — the
kill switch keeps hardcoded demo credentials and a fictional brand catalog out
of shared/staging databases.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import httpx

from src.catalog.adapters.db.repository import CatalogRepository
from src.catalog.adapters.s3_images import ImageStore
from src.catalog.application.dto import ProductCreate
from src.catalog.application.service import CatalogService
from src.catalog.domain.image_status import ImageStatus
from src.catalog.ports.repository import CatalogRepositoryPort
from src.identity.adapters.db.repository import IdentityRepository
from src.identity.adapters.keycloak.admin_client import KeycloakIdentityAdmin
from src.identity.application.outbox import user_created_outbox
from src.inventory.adapters.db.repository import InventoryRepository
from src.inventory.application.service import InventoryService
from src.shared.clients.postgres_client import create_engine, create_sessionmaker
from src.shared.clients.s3_client import s3_client
from src.shared.config.setting import AppSettings, get_settings
from src.shared.db.pagination import PageParams

log = logging.getLogger("catalog_seed")

SEED_ASSETS_DIR = Path(__file__).parent / "seed_assets" / "products"

# (username, email, role, password, first name, last name)
# First/last names are load-bearing: the realm's user profile requires them, and
# a user failing profile validation gets VERIFY_PROFILE resolved at login — every
# direct grant would answer "Account is not fully set up".
USERS: list[tuple[str, str, str, str, str, str]] = [
    ("demo.consumer", "demo.consumer@example.com", "consumer", "DemoConsumer123!", "Demo", "Consumer"),
    ("demo.merchant", "demo.merchant@example.com", "merchant", "DemoMerchant123!", "Mona", "Tailor"),
    ("demo.merchant2", "demo.merchant2@example.com", "merchant", "DemoMerchant123!", "Iris", "Knit"),
    ("demo.admin", "demo.admin@example.com", "admin", "DemoAdmin123!", "Ada", "Admin"),
]

# (name, category, price, description, image filename, stock)
PRODUCTS: list[tuple[str, str, Decimal, str, str, int]] = [
    # demo.merchant — the tailored house (5)
    (
        "Camel Double-Breasted Coat",
        "Outerwear",
        Decimal("1290.00"),
        "Peak-shouldered double-breasted coat in camel wool, tailored to a long, clean line.",
        "product-coat-camel.jpg",
        0,  # sold out — demos the storefront sold-out badge / add-to-cart rejection
    ),
    (
        "Sand Cotton Trench",
        "Outerwear",
        Decimal("980.00"),
        "Water-resistant cotton gabardine trench in sand, with storm flap and belted waist.",
        "trench.jpg",
        25,
    ),
    (
        "Tailored Black Blazer",
        "Tailoring",
        Decimal("720.00"),
        "Sharp-shouldered blazer in black wool twill with horn buttons.",
        "blazer.jpg",
        25,
    ),
    (
        "Pleated Wool Trouser",
        "Tailoring",
        Decimal("380.00"),
        "High-waisted trouser in charcoal wool with a single forward pleat.",
        "trousers.jpg",
        25,
    ),
    (
        "Poplin Column Shirt",
        "Shirting",
        Decimal("210.00"),
        "Crisp cotton poplin shirt with a concealed placket and mother-of-pearl buttons.",
        "shirt.jpg",
        25,
    ),
    # demo.merchant2 — dresses, knitwear & accessories (6)
    (
        "Silk Slip Dress",
        "Dresses",
        Decimal("640.00"),
        "Bias-cut silk charmeuse in black, with adjustable straps and a fluid, mid-calf "
        "line. Cut to move with the wearer.",
        "product-dress-silk.jpg",
        25,
    ),
    (
        "Ivory Cable-Knit Dress",
        "Knitwear",
        Decimal("480.00"),
        "Heavyweight cable knit in undyed ivory wool, fully fashioned and finished by hand.",
        "product-knit-ivory.jpg",
        25,
    ),
    (
        "Cream Crewneck Sweater",
        "Knitwear",
        Decimal("290.00"),
        "Lambswool crewneck in cream, garment-washed for softness.",
        "sweater.jpg",
        25,
    ),
    (
        "Camel Silk Scarf",
        "Accessories",
        Decimal("150.00"),
        "Featherweight silk scarf in camel, hand-rolled edges.",
        "scarf.jpg",
        25,
    ),
    (
        "Structured Leather Tote",
        "Accessories",
        Decimal("920.00"),
        "Full-grain leather tote with a structured base and suede lining.",
        "handbag.jpg",
        25,
    ),
    (
        "Leather Ankle Boots",
        "Footwear",
        Decimal("640.00"),
        "Black leather ankle boots on a low stacked heel, Blake-stitched.",
        "boots.jpg",
        3,  # low stock — demos the low-stock hint
    ),
]

# First 5 products → demo.merchant, last 6 → demo.merchant2.
MERCHANT_SPLIT = 5

# Wait bounds for the image worker to flip each upload to ready/failed.
IMAGE_WAIT_TIMEOUT_SECONDS = 120.0
IMAGE_WAIT_POLL_SECONDS = 1.0

# Keycloak's start-dev boot can outpace this one-shot; probe before Admin calls.
KEYCLOAK_WAIT_TIMEOUT_SECONDS = 120.0


def _catalog_service(session: Any, settings: AppSettings, store: ImageStore | None) -> CatalogService:
    """Build the catalog service over one session, exactly as the container does."""
    return CatalogService(
        cast(CatalogRepositoryPort, CatalogRepository(session)),
        store,
        cache=None,  # no Valkey reads here; the app's cache is event-invalidated
        image_base_url=settings.image_public_base_url,
        image_max_upload_bytes=settings.image_max_upload_bytes,
        image_upload_ttl_seconds=settings.image_upload_ttl_seconds,
    )


# --- Users: Ensure-User via the Keycloak Admin API adapter ----------------------


async def ensure_user(
    admin: KeycloakIdentityAdmin, username: str, email: str, role: str, password: str, first_name: str, last_name: str
) -> str:
    """Idempotently ensure one demo account; returns its ``sub``.

    Usernames are the stable contract across runs; the ``sub`` is minted by
    Keycloak and changes only when ``--reset`` recreated the account.
    """
    sub = await admin.find_sub_by_username(username)
    if sub is None:
        sub = await admin.create_user_with_password(
            username, email, password, first_name=first_name, last_name=last_name
        )
        log.info("created Keycloak user %s", username)
    else:
        log.info("Keycloak user %s already exists", username)
    if not await admin.has_realm_role(sub, role):
        await admin.grant_realm_role(sub, role)
        log.info("granted role %s to %s", role, username)
    return sub


async def wait_for_keycloak(settings: AppSettings) -> None:
    """Wait for Keycloak's realm to answer (a just-started stack boots slowly).

    Probes the *server* URL (the container-reachable hostname, e.g.
    ``http://keycloak:8080/`` inside compose) — the canonical issuer host is the
    browser-facing one and is deliberately not container-reachable.
    """
    base = settings.keycloak_server_url or settings.keycloak_issuer
    assert base is not None  # the admin adapter requires issuer anyway
    url = base.rstrip("/") + f"/realms/{settings.keycloak_realm}/.well-known/openid-configuration"
    async with httpx.AsyncClient(timeout=5.0) as http:
        try:
            async with asyncio.timeout(KEYCLOAK_WAIT_TIMEOUT_SECONDS):
                while True:
                    try:
                        if (await http.get(url)).status_code == 200:
                            return
                    except httpx.HTTPError:
                        pass  # not up yet — retry until the deadline
                    await asyncio.sleep(2.0)
        except TimeoutError as exc:
            raise RuntimeError(
                f"Keycloak did not become ready within {KEYCLOAK_WAIT_TIMEOUT_SECONDS:.0f}s ({url})"
            ) from exc


# --- Local anchors: the same JIT-provisioning statement the app runs ------------


async def ensure_anchor(sessionmaker: Any, sub: str, email: str) -> Any:
    """Provision (or fetch) the local ``identity.users`` mirror for a demo account.

    Products anchor ``merchant_id`` to this row's id — the anchor JIT provisioning
    would give the account on its first authenticated request. The row insert (on
    first sight) carries a real ``UserCreated`` outbox event, like any JIT row.
    """
    async with sessionmaker() as session:
        repo = IdentityRepository(session)
        row = await repo.get_or_create(sub, email, user_created_outbox)
        return row.id


# --- Products: through the catalog domain layer ---------------------------------


async def wipe_products(sessionmaker: Any, settings: AppSettings) -> int:
    """``--reset``: soft-delete every live product through the domain service.

    Each delete emits ``ProductDeleted`` (so the cache/cart consumers invalidate),
    and the listing excludes soft-deleted rows — re-seeding after a wipe lands on
    the known demo state. Orders are untouched (runtime data, ticket decision 12).
    """
    wiped = 0
    while True:
        async with sessionmaker() as session:
            service = _catalog_service(session, settings, None)
            page = await service.list_products(PageParams(limit=100))
            if not page.items:
                return wiped
            for product in page.items:
                await service.delete_product(product_id=product.id, merchant_id=product.merchant_id, is_admin=True)
                wiped += 1


async def ensure_product(
    sessionmaker: Any, settings: AppSettings, merchant_anchor: Any, spec: tuple[str, str, Decimal, str, str, int]
) -> tuple[Any, bool]:
    """Ensure one product exists for ``merchant_anchor``; returns ``(product, created)``.

    Idempotency key is the owning merchant + exact product name (the console's
    own scoping dimension): a re-run finds the merchant's live products and
    skips. Missing products go through :meth:`CatalogService.create_product` so
    the outbox carries a real ``ProductCreated`` event.
    """
    name, category, price, description, _image, _stock = spec
    async with sessionmaker() as session:
        service = _catalog_service(session, settings, None)
        page = await service.list_products(PageParams(limit=100), filters={"merchant_id": merchant_anchor}, search=name)
        for product in page.items:
            if product.name == name:
                return product, False
        created = await service.create_product(
            merchant_id=merchant_anchor,
            data=ProductCreate(name=name, description=description, category=category, price=price),
        )
        return created, True


# --- Images: presign-flow mint-and-pend, then direct S3 PUT ---------------------


async def ensure_image(
    store: ImageStore,
    sessionmaker: Any,
    settings: AppSettings,
    product_id: Any,
    image_path: Path,
) -> str:
    """Idempotently run one product's image through the real pipeline.

    Skips when the product is already ``ready`` (a re-run re-uploads nothing).
    Otherwise mints the presign ticket (flipping the product to ``pending`` and
    recording the upload token exactly like the HTTP flow), PUTs the JPEG bytes
    to the minted ``uploads/`` key, and waits for the image worker to flip it to
    ``ready``. A previous run's stranded ``pending``/``failed`` product is
    repaired by re-minting (the new token supersedes the old one by design).
    """
    current = await _product_status(sessionmaker, product_id)
    if current == ImageStatus.READY:
        return "already-ready"

    data = image_path.read_bytes()
    key = await _presign_and_pend(store, sessionmaker, settings, product_id, len(data))
    # Direct PUT into the same key the presigned POST targets: the object's
    # ObjectCreated event is what drives the worker, and the worker's guarded
    # flip (token + pending) doesn't care who uploaded the bytes.
    await store.put_bytes(key, data, content_type="image/jpeg")
    log.info("uploaded %s for product %s; waiting for the image worker", image_path.name, product_id)

    try:
        async with asyncio.timeout(IMAGE_WAIT_TIMEOUT_SECONDS):
            while True:
                status = await _product_status(sessionmaker, product_id)
                if status in (ImageStatus.READY, ImageStatus.FAILED):
                    # ``failed`` here means the bytes didn't pass sniff/re-encode —
                    # a broken seed asset, not a transient worker condition.
                    if status == ImageStatus.FAILED:
                        raise RuntimeError(f"image worker rejected the seed asset for product {product_id}")
                    return str(status)
                await asyncio.sleep(IMAGE_WAIT_POLL_SECONDS)
    except TimeoutError as exc:
        raise RuntimeError(f"image for product {product_id} not ready after {IMAGE_WAIT_TIMEOUT_SECONDS:.0f}s") from exc


async def _product_status(sessionmaker: Any, product_id: Any) -> str | None:
    """The product's current ``image_status`` (``None`` when the row is gone)."""
    async with sessionmaker() as session:
        repo = CatalogRepository(session)
        product = await repo.get_product(product_id)
        return None if product is None else product.image_status


async def _presign_and_pend(
    store: ImageStore, sessionmaker: Any, settings: AppSettings, product_id: Any, content_length: int
) -> str:
    """Mint a presigned ticket and flip the product to ``pending`` — presign-flow style.

    Calls the service's :meth:`presign_image_upload` (ownership assert → MIME/size
    validation → mint → pending flip → expiry record, one service transaction).
    ``is_admin=True`` is honest: the seeder provisions on behalf of the stack,
    and the caller-id argument is then unused by the ownership assert.
    """
    async with sessionmaker() as session:
        service = _catalog_service(session, settings, store)
        ticket = await service.presign_image_upload(
            product_id=product_id,
            merchant_id=product_id,  # bypassed: is_admin=True short-circuits the ownership assert
            is_admin=True,
            content_type="image/jpeg",
            content_length=content_length,
        )
        if ticket is None:  # pragma: no cover - the product was just ensured above
            raise RuntimeError(f"product {product_id} vanished mid-seed")
        return ticket.key


# --- Stock: the inventory application service -----------------------------------


async def ensure_stock(sessionmaker: Any, settings: AppSettings, product_id: Any, on_hand: int) -> None:
    """Declare stock through the admin PUT's own application service (never SQL)."""
    async with sessionmaker() as session:
        service = InventoryService(
            InventoryRepository(session), reservation_ttl_seconds=settings.reservation_ttl_seconds
        )
        await service.upsert_stock(str(product_id), on_hand)


# --- Orchestration --------------------------------------------------------------


async def run(reset: bool) -> None:
    settings = get_settings()
    admin = KeycloakIdentityAdmin(settings)
    engine = create_engine(settings, worker=True)
    sessionmaker = create_sessionmaker(engine)
    try:
        await wait_for_keycloak(settings)

        if reset:
            # DB-side wipe BEFORE Keycloak user deletion — this ordering is the
            # crash-convergence guarantee: a mid-reset crash leaves at worst
            # orphaned Keycloak users + no local products, so a plain re-seed
            # recreates users (fresh subs → fresh anchors) and products keyed
            # off them, converging with no duplicates.
            wiped = await wipe_products(sessionmaker, settings)
            log.info("reset: soft-deleted %d product(s)", wiped)
            for username, _email, _role, _password, _first, _last in USERS:
                sub = await admin.find_sub_by_username(username)
                if sub is not None:
                    await admin.delete_user(sub)
                    log.info("reset: deleted Keycloak user %s", username)

        subs: dict[str, str] = {}
        anchors: dict[str, Any] = {}
        for username, email, role, password, first_name, last_name in USERS:
            subs[username] = await ensure_user(admin, username, email, role, password, first_name, last_name)
            anchors[username] = await ensure_anchor(sessionmaker, subs[username], email)

        merchant1 = anchors["demo.merchant"]
        merchant2 = anchors["demo.merchant2"]
        if merchant1 == merchant2:
            raise RuntimeError("demo merchants resolved to the same local identity row")

        async with s3_client(settings) as s3:
            if not settings.s3_bucket:  # pragma: no cover - compose always sets it
                raise RuntimeError("S3_BUCKET must be configured for demo seeding")
            store = ImageStore(s3, settings.s3_bucket)

            counts = {"created": 0, "existing": 0, "uploaded": 0, "already-ready": 0}
            for specs, anchor in ((PRODUCTS[:MERCHANT_SPLIT], merchant1), (PRODUCTS[MERCHANT_SPLIT:], merchant2)):
                for spec in specs:
                    name = spec[0]
                    product, created = await ensure_product(sessionmaker, settings, anchor, spec)
                    counts["created" if created else "existing"] += 1
                    if created:
                        log.info("created product %r", name)

                    image_file = SEED_ASSETS_DIR / spec[4]
                    if not image_file.exists():  # pragma: no cover - assets ship with the repo
                        raise FileNotFoundError(f"seed asset missing: {image_file}")
                    status = await ensure_image(store, sessionmaker, settings, product.id, image_file)
                    counts["uploaded" if status != "already-ready" else "already-ready"] += 1

                    await ensure_stock(sessionmaker, settings, product.id, spec[5])
                    log.info("product %r: image=%s stock=%d", name, status, spec[5])

        log.info(
            "seed complete: %d users, %d products (%d created / %d existing), images (%d uploaded / %d already ready)",
            len(USERS),
            len(PRODUCTS),
            counts["created"],
            counts["existing"],
            counts["uploaded"],
            counts["already-ready"],
        )
    finally:
        await engine.dispose()


def main() -> None:
    """``python -m scripts.catalog_seed [--reset]`` — the ``make seed`` entrypoint."""
    parser = argparse.ArgumentParser(description="Seed demo users, products, images and stock (dev-only).")
    parser.add_argument(
        "--reset", action="store_true", help="soft-delete demo products, delete demo Keycloak users, then re-seed"
    )
    args = parser.parse_args()

    from src.shared.config.logging import setup_logging  # noqa: PLC0415 - mirrors the workers' entrypoints

    settings = get_settings()
    if not settings.seed_demo_data:
        print(
            "Refusing to seed: SEED_DEMO_DATA=0 (kill switch).\n"
            "Demo seeding creates hardcoded users with known passwords and a fictional\n"
            "catalog - set SEED_DEMO_DATA=1 (or unset it) to allow it in dev/local.",
            file=sys.stderr,
        )
        raise SystemExit(2)
    setup_logging(settings.log_level)
    asyncio.run(run(reset=args.reset))


if __name__ == "__main__":
    main()
