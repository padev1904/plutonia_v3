"""Add link validation and Telegram triage fields to Article.

Migrated from local agent (Fase 1 + Fase 2 + Fase 4 do MERGE_PLAN).

IMPORTANT: The dependency below assumes the last migration is '0001_initial'.
If your project has additional migrations, change the dependency to match
the actual last migration name. Run `python manage.py showmigrations news`
to verify.
"""

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("news", "0015_alter_newsletter_status"),
    ]

    operations = [
        # --- Link validation fields (Fase 2) ---
        migrations.AddField(
            model_name="article",
            name="link_validation_status",
            field=models.CharField(
                choices=[
                    ("not_checked", "Not Checked"),
                    ("valid", "Valid"),
                    ("uncertain", "Uncertain"),
                    ("invalid", "Invalid"),
                ],
                db_index=True,
                default="not_checked",
                max_length=24,
            ),
        ),
        migrations.AddField(
            model_name="article",
            name="link_validation_confidence",
            field=models.FloatField(default=0.0),
        ),
        migrations.AddField(
            model_name="article",
            name="link_validation_reason",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="article",
            name="source_link_origin",
            field=models.CharField(
                choices=[
                    ("email", "Email"),
                    ("search", "Search"),
                    ("user", "User"),
                ],
                default="email",
                max_length=24,
            ),
        ),
        migrations.AddField(
            model_name="article",
            name="link_candidates_json",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="article",
            name="link_validated_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        # --- Telegram triage state (Fase 4) ---
        migrations.AddField(
            model_name="article",
            name="telegram_triage_status",
            field=models.CharField(
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
                db_index=True,
                default="not_sent",
                max_length=32,
            ),
        ),
        migrations.AddField(
            model_name="article",
            name="telegram_message_id",
            field=models.BigIntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="article",
            name="proposed_title",
            field=models.CharField(blank=True, default="", max_length=500),
        ),
        migrations.AddField(
            model_name="article",
            name="content_profile",
            field=models.CharField(
                choices=[("news", "News"), ("resource", "Resource")],
                default="news",
                max_length=24,
            ),
        ),
        # --- New indexes ---
        migrations.AddIndex(
            model_name="article",
            index=models.Index(
                fields=["telegram_triage_status"],
                name="news_art_tg_triage_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="article",
            index=models.Index(
                fields=["link_validation_status"],
                name="news_art_link_val_idx",
            ),
        ),
    ]


