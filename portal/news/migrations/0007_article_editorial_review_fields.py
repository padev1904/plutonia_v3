import uuid
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("news", "0006_article_review_approval"),
    ]

    operations = [
        migrations.AddField(
            model_name="article",
            name="draft_article_index",
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="article",
            name="editorial_comment",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="article",
            name="editorial_reviewed_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="article",
            name="editorial_status",
            field=models.CharField(
                choices=[
                    ("pending", "Pending"),
                    ("approved", "Approved"),
                    ("changes_requested", "Changes Requested"),
                    ("on_hold", "On Hold"),
                ],
                db_index=True,
                default="pending",
                max_length=32,
            ),
        ),
        migrations.AddField(
            model_name="article",
            name="preview_token",
            field=models.UUIDField(db_index=True, default=uuid.uuid4),
        ),
        migrations.AddIndex(
            model_name="article",
            index=models.Index(fields=["editorial_status", "-published_at"], name="news_art_editorial_pub_idx"),
        ),
    ]
