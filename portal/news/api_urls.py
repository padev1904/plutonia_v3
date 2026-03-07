from django.urls import path

from . import api_views

urlpatterns = [
    path("newsletter/register/", api_views.register_newsletter, name="api-register-newsletter"),
    path("newsletter/ingest-cursor/", api_views.get_ingest_cursor, name="api-ingest-cursor"),
    path("newsletter/<int:newsletter_id>/raw/", api_views.get_newsletter_raw, name="api-newsletter-raw"),
    path("articles/publish/", api_views.publish_articles, name="api-publish-articles"),
    path("articles/editorial-data/", api_views.get_article_editorial_data, name="api-article-editorial-data"),
    path("articles/editorial-pending/", api_views.get_pending_article_editorial, name="api-article-editorial-pending"),
    path("articles/editorial-decision/", api_views.apply_article_editorial_decision, name="api-article-editorial-decision"),
    path("resources/publish/", api_views.publish_resource, name="api-publish-resource"),
    path("resources/review-pending/", api_views.get_pending_resource_reviews, name="api-resource-review-pending"),
    path("resources/review-notified/", api_views.mark_resource_review_notified, name="api-resource-review-notified"),
    path("resources/review-decision/", api_views.apply_resource_review_decision, name="api-resource-review-decision"),
    path("newsletter/status/", api_views.update_newsletter_status, name="api-update-status"),
    path("newsletter/pending/", api_views.get_pending_newsletters, name="api-pending"),
    path("log/", api_views.log_processing, name="api-log"),
    path("articles/link-validation/", api_views.update_article_link_validation, name="api-article-link-validation"),
]
