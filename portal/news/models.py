from django.contrib.postgres.indexes import GinIndex
from django.contrib.postgres.search import SearchVectorField
from django.db import models
from django.utils import timezone
import uuid
from urllib.parse import urlparse


def _host_source_name(url: str, fallback: str = "Unknown Source") -> str:
    value = (url or "").strip()
    if not value:
        return fallback
    try:
        host = (urlparse(value).hostname or "").lower().strip(".")
    except Exception:
        host = ""
    if not host:
        return fallback
    if host.startswith("www."):
        host = host[4:]
    parts = [p for p in host.split(".") if p]
    if not parts:
        return fallback

    if len(parts) >= 3 and parts[-1] in {"uk", "pt", "br", "au", "jp", "in", "es", "fr", "de", "it", "nl"} and parts[-2] in {"co", "com", "org", "gov", "edu"}:
        base = parts[-3]
    elif len(parts) >= 2 and parts[-2] in {"co", "com", "org", "net", "gov", "edu"}:
        base = parts[-3] if len(parts) >= 3 else parts[-2]
    elif len(parts) >= 2:
        base = parts[-2]
    else:
        base = parts[0]

    special = {
        "bbc": "BBC",
        "cnbc": "CNBC",
        "wsj": "WSJ",
        "nytimes": "The New York Times",
        "ft": "Financial Times",
    }
    if base in special:
        return special[base]
    return base.replace("-", " ").replace("_", " ").strip().title() or fallback


class Newsletter(models.Model):
    gmail_uid = models.CharField(max_length=50, blank=True, default="", db_index=True)
    gmail_message_id = models.CharField(max_length=255, unique=True, db_index=True)
    sender_name = models.CharField(max_length=255)
    sender_email = models.EmailField()
    original_sender_name = models.CharField(max_length=255, blank=True, default="")
    original_sender_email = models.CharField(max_length=255, blank=True, default="")
    subject = models.CharField(max_length=500)
    original_sent_at_raw = models.CharField(max_length=255, blank=True, default="")
    original_sent_at = models.DateTimeField(null=True, blank=True, db_index=True)
    received_at = models.DateTimeField()
    processed_at = models.DateTimeField(null=True, blank=True)
    raw_html = models.TextField()
    status = models.CharField(
        max_length=32,
        choices=[
            ("pending", "Pending"),
            ("processing", "Processing"),
            ("review", "Review"),
            ("completed", "Completed"),
            ("eliminada_pos_publicada", "Eliminada_Pós_Publicada"),
            ("error", "Error"),
        ],
        default="pending",
        db_index=True,
    )
    error_message = models.TextField(blank=True, default="")
    news_count = models.IntegerField(default=0)

    class Meta:
        ordering = ["original_sent_at", "received_at"]
        indexes = [
            models.Index(
                fields=["status", "original_sent_at", "received_at"],
                name="news_news_status_orig_recv_idx",
            )
        ]

    def __str__(self):
        return f"{self.sender_name}: {self.subject} ({self.received_at:%Y-%m-%d})"


class Category(models.Model):
    name = models.CharField(max_length=100, unique=True, db_index=True)
    slug = models.SlugField(max_length=120, unique=True)
    description = models.TextField(blank=True, default="")
    news_count = models.IntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name_plural = "categories"
        ordering = ["name"]

    def __str__(self):
        return self.name


class Article(models.Model):
    title = models.CharField(max_length=500)
    summary = models.TextField()
    original_url = models.URLField(max_length=2000, blank=True, default="")
    image_url = models.URLField(max_length=2000, blank=True, default="")
    section = models.CharField(max_length=120, blank=True, default="")
    category = models.CharField(max_length=120, blank=True, default="")
    subcategory = models.CharField(max_length=120, blank=True, default="")
    enrichment_context = models.TextField(blank=True, default="")
    newsletter = models.ForeignKey(Newsletter, on_delete=models.CASCADE, related_name="articles")
    categories = models.ManyToManyField(Category, related_name="articles", blank=True)
    published_at = models.DateTimeField(default=timezone.now, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    is_favorite = models.BooleanField(default=False, db_index=True)
    rating = models.IntegerField(null=True, blank=True, choices=[(i, str(i)) for i in range(1, 6)])
    is_read = models.BooleanField(default=False, db_index=True)
    is_review_approved = models.BooleanField(default=False, db_index=True)
    review_approved_at = models.DateTimeField(null=True, blank=True)
    preview_token = models.UUIDField(default=uuid.uuid4, db_index=True)
    draft_article_index = models.PositiveIntegerField(null=True, blank=True)
    editorial_status = models.CharField(
        max_length=32,
        choices=[
            ("pending", "Pending"),
            ("approved", "Approved"),
            ("changes_requested", "Changes Requested"),
            ("on_hold", "On Hold"),
        ],
        default="pending",
        db_index=True,
    )
    editorial_reviewed_at = models.DateTimeField(null=True, blank=True)
    editorial_comment = models.TextField(blank=True, default="")

    # --- Link validation (migrated from local agent) ---
    link_validation_status = models.CharField(
        max_length=24,
        choices=[
            ("not_checked", "Not Checked"),
            ("valid", "Valid"),
            ("uncertain", "Uncertain"),
            ("invalid", "Invalid"),
        ],
        default="not_checked",
        db_index=True,
    )
    link_validation_confidence = models.FloatField(default=0.0)
    link_validation_reason = models.TextField(blank=True, default="")
    source_link_origin = models.CharField(
        max_length=24,
        choices=[
            ("email", "Email"),
            ("search", "Search"),
            ("user", "User"),
        ],
        default="email",
    )
    link_candidates_json = models.TextField(blank=True, default="")
    link_validated_at = models.DateTimeField(null=True, blank=True)

    # --- Telegram triage state (migrated from local agent) ---
    telegram_triage_status = models.CharField(
        max_length=32,
        choices=[
            ("not_sent", "Not Sent"),
            ("awaiting_triage", "Awaiting Triage"),
            ("awaiting_llm", "Awaiting LLM"),
            ("pending_approval", "Pending Approval"),
            ("waiting_user_input", "Waiting User Input"),
            ("waiting_edit", "Waiting Edit"),
            ("blocked_llm", "Blocked LLM"),
            ("approved", "Approved"),
            ("rejected", "Rejected"),
        ],
        default="not_sent",
        db_index=True,
    )
    telegram_message_id = models.BigIntegerField(null=True, blank=True)
    proposed_title = models.CharField(max_length=500, blank=True, default="")
    content_profile = models.CharField(
        max_length=24,
        choices=[("news", "News"), ("resource", "Resource")],
        default="news",
    )

    search_vector = SearchVectorField(null=True, blank=True)

    class Meta:
        ordering = ["-published_at"]
        indexes = [
            GinIndex(fields=["search_vector"]),
            models.Index(fields=["is_favorite", "-published_at"]),
            models.Index(fields=["rating", "-published_at"]),
            models.Index(fields=["is_read", "-published_at"]),
            models.Index(
                fields=["is_review_approved", "-published_at"],
                name="news_art_approved_pub_idx",
            ),
            models.Index(
                fields=["editorial_status", "-published_at"],
                name="news_art_editorial_pub_idx",
            ),
            models.Index(
                fields=["telegram_triage_status"],
                name="news_art_tg_triage_idx",
            ),
            models.Index(
                fields=["link_validation_status"],
                name="news_art_link_val_idx",
            ),
        ]

    def __str__(self):
        return self.title

    @property
    def source_name(self) -> str:
        return _host_source_name(self.original_url, fallback=self.newsletter.sender_name)


class Resource(models.Model):
    title = models.CharField(max_length=500)
    summary = models.TextField(blank=True, default="")
    article_body = models.TextField(blank=True, default="")
    description = models.TextField(blank=True, default="")
    resource_url = models.URLField(max_length=2000, unique=True)
    image_url = models.URLField(max_length=2000, blank=True, default="")
    section = models.CharField(max_length=120, blank=True, default="", db_index=True)
    category = models.CharField(max_length=120, blank=True, default="", db_index=True)
    subcategory = models.CharField(max_length=120, blank=True, default="", db_index=True)
    is_featured = models.BooleanField(default=False, db_index=True)
    is_active = models.BooleanField(default=True, db_index=True)
    preview_token = models.UUIDField(default=uuid.uuid4, db_index=True)
    review_status = models.CharField(
        max_length=24,
        choices=[
            ("pending", "Pending"),
            ("approved", "Approved"),
            ("rejected", "Rejected"),
        ],
        default="approved",
        db_index=True,
    )
    review_requested_at = models.DateTimeField(null=True, blank=True)
    review_notified_at = models.DateTimeField(null=True, blank=True)
    review_decided_at = models.DateTimeField(null=True, blank=True)
    review_comment = models.TextField(blank=True, default="")
    source_published_at = models.DateTimeField(null=True, blank=True, db_index=True)
    published_at = models.DateTimeField(default=timezone.now, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-is_featured", "-published_at", "title"]
        indexes = [
            models.Index(fields=["is_active", "-published_at"]),
            models.Index(fields=["section", "category", "subcategory"]),
            models.Index(fields=["is_featured", "-published_at"]),
            models.Index(fields=["review_status", "-review_notified_at"]),
        ]

    def __str__(self):
        return self.title

    @property
    def source_name(self) -> str:
        return _host_source_name(self.resource_url)


class ProcessingLog(models.Model):
    newsletter = models.ForeignKey(Newsletter, on_delete=models.CASCADE, related_name="logs", null=True, blank=True)
    action = models.CharField(max_length=50)
    status = models.CharField(
        max_length=20,
        choices=[
            ("started", "Started"),
            ("success", "Success"),
            ("error", "Error"),
        ],
    )
    message = models.TextField(blank=True, default="")
    duration_seconds = models.FloatField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.action} - {self.status} ({self.created_at:%Y-%m-%d %H:%M})"
