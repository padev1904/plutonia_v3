from datetime import timedelta

from django.contrib.admin.sites import AdminSite
from django.contrib.auth import get_user_model
from django.test import RequestFactory, TestCase, SimpleTestCase
from django.utils import timezone

from .admin import ArticleAdmin, ResourceAdmin
from .models import Article, Newsletter, Resource

from .api_views import _newsletter_workflow_summary, _resource_article_body_fallback, _resource_article_body_is_weak


class ResourceEditorialFallbackTests(SimpleTestCase):
    def test_article_body_fallback_is_multi_paragraph_and_non_empty(self):
        body = _resource_article_body_fallback(
            title="Build a Reasoning Model From Scratch",
            summary="Short teaser text.",
            section="Learning Resources",
            category="AI Development",
            subcategory="Model Construction",
        )
        paragraphs = [p.strip() for p in body.split("\n\n") if p.strip()]
        self.assertGreaterEqual(len(paragraphs), 5)
        self.assertGreater(len(body), 420)

    def test_weak_detector_rejects_summary_like_text(self):
        summary = "This is a concise teaser."
        body = "This is a concise teaser."
        self.assertTrue(_resource_article_body_is_weak(body, summary))

    def test_weak_detector_accepts_richer_multi_paragraph_body(self):
        summary = "A concise teaser about the resource."
        body = "\n\n".join(
            [
                "This resource introduces a full workflow for building and evaluating reasoning models in realistic settings.",
                "It explains the design choices behind dataset setup, iteration strategy, and practical experimentation loops.",
                "The material is aimed at practitioners who already know core ML concepts and want implementation depth.",
                "Readers can use it as a roadmap to prioritize architecture choices, training constraints, and validation criteria.",
                "It also surfaces trade-offs, including time-to-train, quality thresholds, and reproducibility considerations.",
                "The source should still be consulted for complete examples and updates from the author.",
            ]
        )
        self.assertFalse(_resource_article_body_is_weak(body, summary))


class TestAdminPublicationActions(TestCase):
    def setUp(self):
        self.site = AdminSite()
        self.factory = RequestFactory()
        User = get_user_model()
        self.user = User.objects.create_superuser(
            username="admin_tests",
            email="admin-tests@example.com",
            password="secret123",
        )

    def _request(self):
        request = self.factory.post("/admin/")
        request.user = self.user
        return request

    def test_article_admin_unpublish_and_delete_actions(self):
        newsletter = Newsletter.objects.create(
            gmail_uid="1001",
            gmail_message_id="msg-test-1001",
            sender_name="Sender",
            sender_email="sender@example.com",
            subject="Subject",
            received_at=timezone.now() - timedelta(days=1),
            raw_html="<html></html>",
            status="completed",
            news_count=2,
        )
        published = Article.objects.create(
            title="Published article",
            summary="summary",
            original_url="https://example.com/a",
            newsletter=newsletter,
            is_review_approved=True,
            editorial_status="approved",
        )
        pending = Article.objects.create(
            title="Pending article",
            summary="summary",
            original_url="https://example.com/b",
            newsletter=newsletter,
            is_review_approved=True,
            editorial_status="pending",
        )

        admin_obj = ArticleAdmin(Article, self.site)
        admin_obj.message_user = lambda *args, **kwargs: None

        admin_obj.unpublish_selected_articles(self._request(), Article.objects.filter(id__in=[published.id, pending.id]))

        published.refresh_from_db()
        pending.refresh_from_db()
        newsletter.refresh_from_db()

        self.assertFalse(published.is_review_approved)
        self.assertEqual(published.editorial_status, "on_hold")
        self.assertEqual(pending.editorial_status, "pending")
        self.assertEqual(newsletter.news_count, 2)

        admin_obj.delete_selected_publications(self._request(), Article.objects.filter(id__in=[published.id, pending.id]))
        newsletter.refresh_from_db()
        self.assertFalse(Article.objects.filter(id=published.id).exists())
        self.assertFalse(Article.objects.filter(id=pending.id).exists())
        self.assertEqual(newsletter.status, "eliminada_pos_publicada")
        self.assertEqual(newsletter.news_count, 0)

    def test_resource_admin_unpublish_and_delete_actions(self):
        published = Resource.objects.create(
            title="Published resource",
            resource_url="https://example.com/resource-a",
            review_status="approved",
            is_active=True,
        )
        pending = Resource.objects.create(
            title="Pending resource",
            resource_url="https://example.com/resource-b",
            review_status="pending",
            is_active=True,
        )

        admin_obj = ResourceAdmin(Resource, self.site)
        admin_obj.message_user = lambda *args, **kwargs: None

        admin_obj.unpublish_selected_resources(self._request(), Resource.objects.filter(id__in=[published.id, pending.id]))
        published.refresh_from_db()
        pending.refresh_from_db()

        self.assertEqual(published.review_status, "rejected")
        self.assertFalse(published.is_active)
        self.assertEqual(pending.review_status, "pending")

        admin_obj.delete_selected_resources(self._request(), Resource.objects.filter(id__in=[published.id, pending.id]))
        self.assertFalse(Resource.objects.filter(id=published.id).exists())
        self.assertFalse(Resource.objects.filter(id=pending.id).exists())


class TestPublicCardView(TestCase):
    def test_public_card_page_available_for_approved_article(self):
        newsletter = Newsletter.objects.create(
            gmail_uid="2001",
            gmail_message_id="msg-test-2001",
            sender_name="Sender",
            sender_email="sender@example.com",
            subject="Subject",
            received_at=timezone.now() - timedelta(days=1),
            raw_html="<html></html>",
            status="completed",
            news_count=1,
        )
        article = Article.objects.create(
            title="Public card story",
            summary="summary",
            original_url="https://example.com/public-card",
            newsletter=newsletter,
            is_review_approved=True,
            editorial_status="approved",
        )

        response = self.client.get(f"/article/{article.id}/card/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Public card story")
        self.assertContains(response, f"/article/{article.id}/")


class NewsletterWorkflowSummaryTests(TestCase):
    def _newsletter(self, uid: str) -> Newsletter:
        return Newsletter.objects.create(
            gmail_uid=uid,
            gmail_message_id=f"msg-{uid}",
            sender_name="Sender",
            sender_email="sender@example.com",
            subject=f"Subject {uid}",
            received_at=timezone.now() - timedelta(days=1),
            raw_html="<html></html>",
            status="review",
            news_count=0,
        )

    def test_summary_returns_pending_when_some_articles_decided_but_not_all(self):
        newsletter = self._newsletter("3001")
        Article.objects.create(
            title="Approved",
            summary="summary",
            original_url="https://example.com/a",
            newsletter=newsletter,
            is_review_approved=True,
            editorial_status="approved",
            telegram_triage_status="approved",
        )
        Article.objects.create(
            title="Still pending",
            summary="summary",
            original_url="https://example.com/b",
            newsletter=newsletter,
            is_review_approved=True,
            editorial_status="pending",
            telegram_triage_status="not_sent",
        )

        summary = _newsletter_workflow_summary(newsletter)

        self.assertEqual(summary["gmail_label"], "Pending")
        self.assertEqual(summary["approved_articles"], 1)
        self.assertEqual(summary["rejected_articles"], 0)
        self.assertEqual(summary["unresolved_articles"], 1)

    def test_summary_returns_published_when_all_articles_approved(self):
        newsletter = self._newsletter("3002")
        for idx in range(2):
            Article.objects.create(
                title=f"Approved {idx}",
                summary="summary",
                original_url=f"https://example.com/published-{idx}",
                newsletter=newsletter,
                is_review_approved=True,
                editorial_status="approved",
                telegram_triage_status="approved",
            )

        summary = _newsletter_workflow_summary(newsletter)
        self.assertEqual(summary["gmail_label"], "Published")

    def test_summary_returns_partial_when_mix_of_approved_and_rejected(self):
        newsletter = self._newsletter("3003")
        Article.objects.create(
            title="Approved",
            summary="summary",
            original_url="https://example.com/partial-a",
            newsletter=newsletter,
            is_review_approved=True,
            editorial_status="approved",
            telegram_triage_status="approved",
        )
        Article.objects.create(
            title="Rejected",
            summary="summary",
            original_url="https://example.com/partial-b",
            newsletter=newsletter,
            is_review_approved=True,
            editorial_status="changes_requested",
            telegram_triage_status="rejected",
        )

        summary = _newsletter_workflow_summary(newsletter)
        self.assertEqual(summary["gmail_label"], "Partial")

    def test_summary_returns_rejected_when_all_articles_rejected(self):
        newsletter = self._newsletter("3004")
        for idx in range(2):
            Article.objects.create(
                title=f"Rejected {idx}",
                summary="summary",
                original_url=f"https://example.com/rejected-{idx}",
                newsletter=newsletter,
                is_review_approved=True,
                editorial_status="changes_requested",
                telegram_triage_status="rejected",
            )

        summary = _newsletter_workflow_summary(newsletter)
        self.assertEqual(summary["gmail_label"], "Rejected")
