from django.contrib import admin
from django.utils import timezone
from django.db.models import Count

from .models import Article, Category, Newsletter, ProcessingLog, Resource


def _sync_newsletter_news_count(newsletter_ids: set[int]) -> None:
    if not newsletter_ids:
        return
    rows = (
        Newsletter.objects.filter(id__in=newsletter_ids)
        .annotate(total_articles=Count("articles"))
    )
    for row in rows:
        if row.news_count != row.total_articles:
            row.news_count = row.total_articles
            row.save(update_fields=["news_count"])


@admin.register(Newsletter)
class NewsletterAdmin(admin.ModelAdmin):
    list_display = ["sender_name", "subject", "received_at", "status", "news_count"]
    list_filter = ["status", "sender_name"]
    search_fields = ["sender_name", "subject", "gmail_message_id"]
    readonly_fields = ["gmail_message_id", "raw_html", "processed_at"]
    date_hierarchy = "received_at"


@admin.register(Category)
class CategoryAdmin(admin.ModelAdmin):
    list_display = ["name", "slug", "news_count"]
    prepopulated_fields = {"slug": ("name",)}
    search_fields = ["name"]


@admin.register(Article)
class ArticleAdmin(admin.ModelAdmin):
    list_display = [
        "title",
        "newsletter",
        "section",
        "category",
        "subcategory",
        "published_at",
        "is_review_approved",
        "review_approved_at",
        "editorial_status",
        "editorial_reviewed_at",
        "is_favorite",
        "is_read",
    ]
    list_filter = [
        "is_review_approved",
        "editorial_status",
        "is_favorite",
        "is_read",
        "section",
        "category",
        "subcategory",
        "categories",
        "newsletter__sender_name",
    ]
    search_fields = ["title", "summary", "enrichment_context", "original_url", "newsletter__subject", "newsletter__sender_name"]
    list_per_page = 50
    filter_horizontal = ["categories"]
    date_hierarchy = "published_at"
    readonly_fields = ["search_vector", "created_at", "updated_at"]
    actions = ["unpublish_selected_articles", "delete_selected_publications"]

    @admin.action(description="Retirar publicações selecionadas do portal (mover para on_hold)")
    def unpublish_selected_articles(self, request, queryset):
        published_qs = queryset.filter(is_review_approved=True, editorial_status="approved")
        total = published_qs.count()
        if total == 0:
            self.message_user(request, "Nenhuma publicação pública selecionada para retirar.")
            return
        newsletter_ids = set(published_qs.values_list("newsletter_id", flat=True))
        updated = published_qs.update(
            is_review_approved=False,
            editorial_status="on_hold",
            editorial_reviewed_at=timezone.now(),
            editorial_comment="Removed from public feed by admin action.",
        )
        _sync_newsletter_news_count(newsletter_ids)
        self.message_user(request, f"{updated} publicação(ões) retirada(s) do portal.")

    @admin.action(description="Eliminar publicações selecionadas do portal")
    def delete_selected_publications(self, request, queryset):
        selected_qs = queryset
        total = selected_qs.count()
        if total == 0:
            self.message_user(request, "Nenhuma publicação selecionada para eliminar.")
            return
        newsletter_ids = set(selected_qs.values_list("newsletter_id", flat=True))
        selected_qs.delete()
        _sync_newsletter_news_count(newsletter_ids)
        if newsletter_ids:
            Newsletter.objects.filter(id__in=newsletter_ids).update(
                status="eliminada_pos_publicada",
                error_message="Eliminada no portal apos publicacao final.",
                processed_at=timezone.now(),
            )
        self.message_user(request, f"{total} publicação(ões) eliminada(s).")


@admin.register(ProcessingLog)
class ProcessingLogAdmin(admin.ModelAdmin):
    list_display = ["action", "status", "newsletter", "duration_seconds", "created_at"]
    list_filter = ["action", "status"]
    date_hierarchy = "created_at"


@admin.register(Resource)
class ResourceAdmin(admin.ModelAdmin):
    list_display = [
        "title",
        "review_status",
        "source_published_at",
        "section",
        "category",
        "subcategory",
        "is_featured",
        "is_active",
        "published_at",
        "source_name",
    ]
    list_filter = ["review_status", "is_active", "is_featured", "section", "category", "subcategory"]
    search_fields = ["title", "summary", "article_body", "description", "resource_url", "category", "subcategory", "section"]
    list_per_page = 50
    date_hierarchy = "published_at"
    actions = ["unpublish_selected_resources", "delete_selected_resources"]

    @admin.action(description="Retirar recursos selecionados do portal (marcar rejeitado)")
    def unpublish_selected_resources(self, request, queryset):
        published_qs = queryset.filter(review_status="approved", is_active=True)
        total = published_qs.count()
        if total == 0:
            self.message_user(request, "Nenhum recurso público selecionado para retirar.")
            return
        updated = published_qs.update(
            review_status="rejected",
            is_active=False,
            review_decided_at=timezone.now(),
            review_comment="Removed from public feed by admin action.",
        )
        self.message_user(request, f"{updated} recurso(s) retirado(s) do portal.")

    @admin.action(description="Eliminar recursos selecionados do portal")
    def delete_selected_resources(self, request, queryset):
        selected_qs = queryset
        total = selected_qs.count()
        if total == 0:
            self.message_user(request, "Nenhum recurso selecionado para eliminar.")
            return
        selected_qs.delete()
        self.message_user(request, f"{total} recurso(s) eliminado(s).")
